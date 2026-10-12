"""The local profile: which gate this machine is logged in to, as whom, and its key (§7.4, §10.3).

Everything lives in one directory, ``$KONTRA_HOME`` (default ``~/.kontra``):

* ``profile`` -- JSON (:class:`Profile`), mode 0600: it holds the refresh token.
* ``key`` -- the party's age X25519 identity, mode 0600, in ``age-keygen`` format so the
  ``age`` CLI can decrypt a delivery with it during a dispute (decisions I7).
* ``.lock`` -- serialises refresh-token rotation between processes sharing the profile
  (``kontra serve`` in one terminal, ``kontra hire`` in another): the gate revokes a whole
  session when a rotated refresh token is presented twice.

Files are written to a temporary name and renamed into place, so a crash never leaves a
truncated profile, and created 0600 from the start rather than chmod-ed afterwards. A key
is never overwritten: it may be the only thing that can decrypt a delivery.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import stat
import sys
import tempfile
import time
from collections.abc import AsyncGenerator, Collection
from datetime import UTC, datetime
from pathlib import Path

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    PrivateAttr,
    ValidationError,
    field_validator,
)

from kontra.contract.names import is_handle

__all__ = [
    "KEY_FILE",
    "PROFILE_FILE",
    "NotLoggedIn",
    "Profile",
    "ProfileError",
    "kontra_home",
    "profile_lock",
]

PROFILE_FILE = "profile"
KEY_FILE = "key"
_LOCK_FILE = ".lock"
_LOCK_WAIT_S = 60.0
_LOCK_POLL_S = 0.05


class ProfileError(Exception):
    """The profile or key on disk is unusable (corrupt, wrong shape, locked)."""


class NotLoggedIn(ProfileError):
    """No profile or key here, or the gate no longer honours the session: run ``kontra login``."""


def kontra_home() -> Path:
    """``$KONTRA_HOME`` if set and non-empty, else ``~/.kontra``."""
    env = os.environ.get("KONTRA_HOME")
    return Path(env).expanduser() if env else Path.home() / ".kontra"


def _resolve(home: Path | str | None) -> Path:
    return Path(home).expanduser() if home is not None else kontra_home()


def _ensure_dir(home: Path) -> None:
    """Create ``home`` as 0700. An existing directory is left alone: ``KONTRA_HOME`` may
    point somewhere shared, and the files inside are 0600 whatever the directory is."""
    if home.is_dir():
        return
    home.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(home, 0o700)  # mkdir's mode is filtered by the umask; we want exactly 0700


def _read_private(path: Path) -> bytes:
    """Read a credential file, refusing one another user could have written.

    ``KONTRA_HOME`` may be a directory others can write to (we leave an existing one's
    mode alone). There another user can create ``key`` before we do: :meth:`Profile.ensure_identity`
    would adopt it, ``kontra login`` would register its public key, and every delivery to
    the party would be encrypted to them. So, like ssh with a private key, the file must be
    a regular file owned by us (or root, who can read it anyway) and not world-writable.
    Group/world-*readable* is tolerated: a container's mounted secret is often root-owned
    0644. Opened non-blocking so a planted FIFO cannot hang us. Raises FileNotFoundError
    when absent."""
    flags = os.O_RDONLY | getattr(os, "O_NONBLOCK", 0) | getattr(os, "O_CLOEXEC", 0)
    try:
        fd = os.open(path, flags)
    except PermissionError as e:  # e.g. written by an earlier `sudo kontra login`
        raise ProfileError(f"cannot read {path}: {e.strerror}; check its owner") from None
    with os.fdopen(fd, "rb") as f:
        st = os.fstat(fd)
        if not stat.S_ISREG(st.st_mode):
            raise ProfileError(f"{path} is not a regular file; refusing to read it")
        if hasattr(os, "geteuid"):
            if st.st_uid not in (os.geteuid(), 0):
                raise ProfileError(
                    f"{path} belongs to another user (uid {st.st_uid}), who could have "
                    "planted it; refusing to use it. Remove it and run `kontra login`, and keep "
                    "KONTRA_HOME in a directory only you can write to."
                )
            if st.st_mode & stat.S_IWOTH:
                raise ProfileError(
                    f"{path} is writable by every user; refusing to use it. Run "
                    f"`chmod 600 {path}` if its content is yours, else `kontra login`."
                )
        return f.read()


def _write_private(path: Path, data: bytes, *, replace: bool) -> bool:
    """Write ``data`` to ``path`` as a 0600 file, atomically. With ``replace=False`` an
    existing file wins and False is returned (``link`` refuses to clobber)."""
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}-")
    try:
        os.chmod(tmp, 0o600)
        with os.fdopen(fd, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        if replace:
            os.replace(tmp, path)
            return True
        try:
            os.link(tmp, path)
        except FileExistsError:
            return False
        return True
    finally:
        with contextlib.suppress(FileNotFoundError):
            os.unlink(tmp)


def _problems(err: ValidationError, fields: Collection[str]) -> str:
    """What is wrong with the profile, field by field, and never a value: pydantic's own
    rendering quotes the input, and this file holds a refresh token (or whatever was pasted
    into it -- an age secret key, say), which would then reach a terminal, a log or an agent.
    A key is named only when it is one of ``fields``, the profile's own: any other key is the
    file's content too (a token pasted where a name belongs)."""
    found: list[str] = []
    for e in err.errors(include_url=False, include_context=False, include_input=False):
        loc = e["loc"]
        if not loc:
            where = "the profile"
        elif isinstance(loc[0], str) and loc[0] in fields:
            where = loc[0]  # nothing in a profile is nested: the field says where
        else:
            where = "an unknown field"
        found.append(f"{where}: {e['msg']}")
    return "; ".join(dict.fromkeys(found)) or "it does not parse"


class Profile(BaseModel):
    """What ``kontra login`` writes. ``refresh_token`` is rotated by the gate on use, and
    :class:`kontra.auth.Session` writes the new one back here."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    gate_url: str
    temporal_address: str
    party: str
    namespace: str
    #: A bearer credential: kept out of ``repr`` so a log line, a traceback with locals or
    #: an error reporter never carries it.
    refresh_token: str = Field(repr=False)
    #: When the human session behind ``refresh_token`` logged in (publishing needs a fresh one).
    auth_time: AwareDatetime
    #: Our age recipient (``age1...``), as registered with the gate.
    public_key: str | None = None
    #: TLS to the Temporal frontend. Always on outside ``kontra dev`` (§10.2).
    tls: bool = True
    dev: bool = False

    _home: Path | None = PrivateAttr(default=None)

    @field_validator("party", "namespace")
    @classmethod
    def _handle(cls, value: str) -> str:
        if not is_handle(value):  # the value stays out of the message (see _problems)
            raise ValueError("not a party handle")
        return value

    @field_validator("gate_url")
    @classmethod
    def _url(cls, value: str) -> str:
        if not value.startswith(("https://", "http://")):
            raise ValueError("gate_url must be an http(s) URL")
        return value

    @property
    def home(self) -> Path | None:
        """The directory this profile was loaded from or last saved to; None for a profile
        that only exists in memory."""
        return self._home

    @classmethod
    def load(cls, home: Path | str | None = None) -> Profile:
        h = _resolve(home)
        path = h / PROFILE_FILE
        try:
            raw = _read_private(path)
        except FileNotFoundError:
            raise NotLoggedIn(
                f"not logged in: there is no profile at {path}. Run `kontra login` "
                "(or point KONTRA_HOME at the directory that has one)."
            ) from None
        try:
            profile = cls.model_validate_json(raw)
        except ValidationError as e:
            problems = _problems(e, cls.model_fields)
        else:
            profile._home = h
            return profile
        # Raised here, outside the handler: the ValidationError (which quotes the input) is
        # neither this error's cause nor its context, so no traceback prints it.
        raise ProfileError(
            f"the profile at {path} is not valid: {problems}. "
            "Run `kontra login` to write a new one."
        )

    def save(self, home: Path | str | None = None) -> Path:
        """Write the profile (0600) and return its path. Without ``home``, it goes back
        where it came from, else to :func:`kontra_home`."""
        h = _resolve(home) if home is not None else (self._home or kontra_home())
        _ensure_dir(h)
        path = h / PROFILE_FILE
        _write_private(path, self.model_dump_json(indent=2).encode() + b"\n", replace=True)
        self._home = h
        return path

    @staticmethod
    def identity(home: Path | str | None = None) -> str:
        """The party's secret age identity (``AGE-SECRET-KEY-1...``) from ``key``."""
        from kontra.codec.age import is_identity

        path = _resolve(home) / KEY_FILE
        try:
            text = _read_private(path).decode("utf-8")
        except FileNotFoundError:
            raise NotLoggedIn(
                f"no encryption key at {path}. Run `kontra login`, or copy the key file from "
                "the machine that registered this party's public key."
            ) from None
        except UnicodeDecodeError as e:
            raise ProfileError(f"the key file {path} is not text") from e
        found = [ln for ln in text.splitlines() if ln and not ln.startswith("#")]
        if len(found) != 1 or not is_identity(found[0]):
            raise ProfileError(
                f"the key file {path} must hold exactly one AGE-SECRET-KEY-1... line "
                "(comment lines start with #)"
            )
        return found[0]

    @staticmethod
    def ensure_identity(home: Path | str | None = None) -> tuple[str, str]:
        """``(identity, public key)``, creating the key on first use. An existing key is
        always kept, also when two processes race to create one."""
        from kontra.codec.age import generate_identity, public_key_of

        h = _resolve(home)
        path = h / KEY_FILE
        if not path.exists():
            _ensure_dir(h)
            secret, public = generate_identity()
            created = datetime.now(UTC).replace(microsecond=0).isoformat()
            text = f"# created: {created}\n# public key: {public}\n{secret}\n"
            if _write_private(path, text.encode(), replace=False):
                return secret, public
        secret = Profile.identity(h)
        return secret, public_key_of(secret)


@contextlib.asynccontextmanager
async def profile_lock(home: Path | None) -> AsyncGenerator[None]:
    """Hold an exclusive advisory lock on ``home`` (``flock`` on ``home/.lock``).

    Polls with ``LOCK_NB`` instead of blocking in a thread, so a cancelled waiter leaves
    nothing behind. No-op when ``home`` is None or does not exist, and on Windows.
    """
    if home is None or sys.platform == "win32":
        yield
        return
    import fcntl

    # O_NOFOLLOW: in a shared directory a planted ``.lock`` symlink would otherwise make us
    # create (O_CREAT) an empty file wherever it points, with our privileges.
    flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)
    try:
        fd = os.open(home / _LOCK_FILE, flags, 0o600)
    except FileNotFoundError:  # no directory yet: nothing on disk to race over
        yield
        return
    except OSError as e:  # ELOOP: a symlink
        raise ProfileError(f"cannot use the lock file {home / _LOCK_FILE}: {e}") from e
    try:
        deadline = time.monotonic() + _LOCK_WAIT_S
        while True:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    raise ProfileError(
                        f"the profile in {home} has been locked by another process for "
                        f"{_LOCK_WAIT_S:.0f}s"
                    ) from None
                await asyncio.sleep(_LOCK_POLL_S)
        try:
            yield
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
    finally:
        os.close(fd)
