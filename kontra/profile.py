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
import re
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
    "DEVICE_KEY_FILE",
    "JOBS_DIR",
    "KEY_FILE",
    "KEY_FILE_ENV",
    "PROFILE_FILE",
    "NotLoggedIn",
    "Profile",
    "ProfileError",
    "kontra_home",
    "profile_lock",
]

PROFILE_FILE = "profile"
KEY_FILE = "key"
#: This machine's own device key (decisions I25), made by ``kontra devices add --new-key``
#: once its key file was removed as a device (a removed key never comes back). Without it the
#: key file is the device key. Never a recipient: only copies of job keys are encrypted to it.
DEVICE_KEY_FILE = "device.key"
#: Where a worker's age identity comes from when it is not ``$KONTRA_HOME/key``: a fleet
#: mounts ONE key into every pod from a secret, and pod churn never changes the party's
#: recipient set, so an in-flight input stays decryptable by whichever worker takes the
#: task (decisions I7). Same ops shape as a TLS certificate.
KEY_FILE_ENV = "KONTRA_KEY_FILE"
#: Per-job buyer keys: ``$KONTRA_HOME/jobs/<job_id>.key``, made at hire, read by
#: ``kontra job <id> --result``, shredded by ``kontra job <id> --accept``.
JOBS_DIR = "jobs"
#: A job key written BEFORE the hire, when no job id exists yet. If the gate's answer is
#: lost the hold may still have locked, and a key that was never written would make that
#: job's delivery unreadable by anyone -- so it is written first under the quote and
#: renamed to the job id once the gate names it. :meth:`Profile.job_identities` globs
#: ``*.key``, so a pending file that never got renamed still decrypts.
PENDING_PREFIX = "pending-"
#: What may become part of a key file's name. Anchored, ASCII, no dot and no separator, so
#: a quote id from the gate can never walk out of the jobs directory.
_SAFE_TOKEN = re.compile(r"[A-Za-z0-9_-]{1,128}", re.ASCII)
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


def profile_is_dev(home: Path | str | None = None) -> bool:
    """Whether the profile in ``home`` is a dev profile (False when there is none, or it cannot
    be read). For dev-aware hints: a dev profile is told ``kontra dev up``, never ``kontra login``
    (decisions I34, OD14)."""
    try:
        return Profile.load(home).dev
    except ProfileError:
        return False


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


def _read_job_key(path: Path) -> tuple[str, str]:
    """The keypair in a job key file. ``FileNotFoundError`` when there is none."""
    from kontra.codec.age import is_identity, public_key_of

    try:
        text = _read_private(path).decode("utf-8")
    except UnicodeDecodeError as e:
        raise ProfileError(f"the job key file {path} is not text") from e
    found = [ln for ln in text.splitlines() if ln and not ln.startswith("#")]
    if len(found) != 1 or not is_identity(found[0]):
        raise ProfileError(
            f"the job key file {path} must hold exactly one AGE-SECRET-KEY-1... line"
        )
    return found[0], public_key_of(found[0])


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
    #: an error reporter never carries it. None only in the in-memory profile of an API
    #: key's session (:meth:`kontra.auth.Session.from_api_key`), which is never written.
    refresh_token: str | None = Field(repr=False)
    #: When the human session behind ``refresh_token`` logged in (publishing needs a fresh
    #: one); for an API key, the key's creation (such a session is never fresh).
    auth_time: AwareDatetime
    #: This machine's age recipient (``age1...``), as `kontra login` registered it. One of
    #: the party's set, never "the party's key": another machine has its own, and a fleet
    #: mounts one of its own (decisions I7). None when this machine has registered none --
    #: a buyer that only hires, or a worker holding a key from ``$KONTRA_KEY_FILE``.
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
                "(or point KONTRA_HOME at the directory that has one), or `kontra dev up` "
                "for a local dev stack."
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
    def key_path(home: Path | str | None = None) -> Path:
        """Where this machine's age identity lives: ``$KONTRA_KEY_FILE`` if it is set
        (a fleet's mounted secret), else ``<home>/key``."""
        mounted = os.environ.get(KEY_FILE_ENV)
        if mounted:
            return Path(mounted)
        return _resolve(home) / KEY_FILE

    @staticmethod
    def identity(home: Path | str | None = None) -> str:
        """This machine's secret age identity (``AGE-SECRET-KEY-1...``)."""
        from kontra.codec.age import is_identity

        path = Profile.key_path(home)
        try:
            text = _read_private(path).decode("utf-8")
        except FileNotFoundError:
            raise NotLoggedIn(
                f"no encryption key at {path}. Run `kontra recipients add` on this machine "
                f"to make one and register it, point {KEY_FILE_ENV} at the key a fleet "
                "mounts, or copy a key file this party already has a recipient for."
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
    def device_key_path(home: Path | str | None = None) -> Path:
        """Where this machine's own device key lives, when it has one (decisions I25)."""
        return _resolve(home) / DEVICE_KEY_FILE

    @staticmethod
    def device_identity(home: Path | str | None = None, *, fallback: str | None = None) -> str:
        """This machine's device key (decisions I25): its own ``device.key`` when it has
        one, else its key file (``fallback``, when the caller read it already)."""
        from kontra.codec.age import is_identity

        path = Profile.device_key_path(home)
        try:
            text = _read_private(path).decode("utf-8")
        except FileNotFoundError:
            return fallback if fallback is not None else Profile.identity(home)
        except UnicodeDecodeError as e:
            raise ProfileError(f"the device key file {path} is not text") from e
        found = [ln for ln in text.splitlines() if ln and not ln.startswith("#")]
        if len(found) != 1 or not is_identity(found[0]):
            raise ProfileError(
                f"the device key file {path} must hold exactly one AGE-SECRET-KEY-1... line"
            )
        return found[0]

    @staticmethod
    def save_device_identity(secret: str, home: Path | str | None = None) -> Path:
        """Write ``secret`` as this machine's own device key (0600, atomically), in place of
        one there before -- which was removed as a device too: a live one is never replaced
        (``kontra devices add --new-key`` checks)."""
        from kontra.codec.age import public_key_of

        path = Profile.device_key_path(home)
        _ensure_dir(path.parent)
        created = datetime.now(UTC).replace(microsecond=0).isoformat()
        text = (
            f"# this machine's device key (kontra devices), created: {created}\n"
            f"# public key: {public_key_of(secret)}\n{secret}\n"
        )
        _write_private(path, text.encode(), replace=True)
        return path

    @staticmethod
    def ensure_identity(home: Path | str | None = None) -> tuple[str, str]:
        """``(identity, public key)``, creating the key on first use. An existing key is
        always kept, also when two processes race to create one."""
        from kontra.codec.age import generate_identity, public_key_of

        h = _resolve(home)
        path = Profile.key_path(h)
        if not path.exists():
            _ensure_dir(path.parent)
            secret, public = generate_identity()
            created = datetime.now(UTC).replace(microsecond=0).isoformat()
            text = f"# created: {created}\n# public key: {public}\n{secret}\n"
            if _write_private(path, text.encode(), replace=False):
                return secret, public
        secret = Profile.identity(h)
        return secret, public_key_of(secret)

    # ------------------------------------------------------------------ per-job keys

    @staticmethod
    def job_key_path(job_id: str, home: Path | str | None = None) -> Path:
        """``<home>/jobs/<job_id>.key``. ``job_id`` is checked, never joined blindly."""
        from kontra.contract.names import is_job_id

        if not is_job_id(job_id):
            raise ValueError(f"{job_id!r} is not a job id")
        return _resolve(home) / JOBS_DIR / f"{job_id}.key"

    @staticmethod
    def pending_job_key_path(quote_id: str, home: Path | str | None = None) -> Path:
        """``<home>/jobs/pending-<quote_id>.key``, before the gate names the job."""
        if not _SAFE_TOKEN.fullmatch(quote_id):
            raise ValueError(f"{quote_id!r} is not a quote id")
        return _resolve(home) / JOBS_DIR / f"{PENDING_PREFIX}{quote_id}.key"

    @staticmethod
    def new_pending_job_identity(
        quote_id: str, home: Path | str | None = None
    ) -> tuple[str, str, Path]:
        """Make the job's keypair before the hire and write it 0600 under the quote. A key
        already pending for the quote is returned instead: a hire whose answer was lost may
        have locked a job with it, and hiring the quote again must not overwrite it."""
        from kontra.codec.age import generate_identity

        path = Profile.pending_job_key_path(quote_id, home)
        _ensure_dir(path.parent)
        secret, public = generate_identity()
        created = datetime.now(UTC).replace(microsecond=0).isoformat()
        text = f"# quote: {quote_id}\n# created: {created}\n# public key: {public}\n{secret}\n"
        if not _write_private(path, text.encode(), replace=False):
            secret, public = _read_job_key(path)
        return secret, public, path

    @staticmethod
    def claim_job_identity(pending: Path, job_id: str, home: Path | str | None = None) -> Path:
        """Rename a pending key to the job the gate named. Best effort: the key already
        decrypts wherever it is (:meth:`job_identities`), so a failed rename is not fatal."""
        target = Profile.job_key_path(job_id, home)
        try:
            _ensure_dir(target.parent)
            pending.replace(target)
        except OSError:
            return pending
        return target

    @staticmethod
    def new_job_identity(job_id: str, home: Path | str | None = None) -> tuple[str, str]:
        """Make this job's keypair and write it 0600. The buyer sends the public half to
        the gate at hire, which signs THAT into the hold token; nothing registers it as a
        party recipient, so no setup is needed and a compromise reaches open jobs only."""
        from kontra.codec.age import generate_identity

        path = Profile.job_key_path(job_id, home)
        _ensure_dir(path.parent)
        secret, public = generate_identity()
        created = datetime.now(UTC).replace(microsecond=0).isoformat()
        text = f"# job: {job_id}\n# created: {created}\n# public key: {public}\n{secret}\n"
        if not _write_private(path, text.encode(), replace=False):
            return Profile.job_identity(job_id, home)
        return secret, public

    @staticmethod
    def save_job_identity(job_id: str, secret: str, home: Path | str | None = None) -> Path:
        """Write the key of a job hired elsewhere, which reached this machine as a copy for
        one of the account's devices (decisions I25): 0600, never over a key already
        there, in the same format as a key made here."""
        from kontra.codec.age import public_key_of

        path = Profile.job_key_path(job_id, home)
        _ensure_dir(path.parent)
        public = public_key_of(secret)
        created = datetime.now(UTC).replace(microsecond=0).isoformat()
        text = f"# job: {job_id}\n# created: {created}\n# public key: {public}\n{secret}\n"
        _write_private(path, text.encode(), replace=False)
        return path

    @staticmethod
    def job_identity(job_id: str, home: Path | str | None = None) -> tuple[str, str]:
        """This job's keypair from disk."""
        path = Profile.job_key_path(job_id, home)
        try:
            return _read_job_key(path)
        except FileNotFoundError:
            raise ProfileError(
                f"no key for job {job_id} at {path}: it is written by `kontra hire` on the "
                "machine that hired, or by `kontra job <id> --result` from this machine's copy "
                "when another of your devices shared it, and `kontra job <id> --accept` shreds "
                "it here."
            ) from None

    @staticmethod
    def job_identities(home: Path | str | None = None) -> list[str]:
        """Every job key on this machine. age tries each stanza against each identity, so
        handing the codec the whole list lets one client read any job it hired; accepting
        a job shreds its key and shortens the list."""
        from kontra.codec.age import is_identity

        found: list[str] = []
        directory = _resolve(home) / JOBS_DIR
        if not directory.is_dir():
            return found
        for path in sorted(directory.glob("*.key")):
            try:
                text = _read_private(path).decode("utf-8")
            except (OSError, UnicodeDecodeError):
                continue
            lines = [ln for ln in text.splitlines() if ln and not ln.startswith("#")]
            if len(lines) == 1 and is_identity(lines[0]):
                found.append(lines[0])
        return found

    @staticmethod
    def shred_job_identity(job_id: str, home: Path | str | None = None) -> bool:
        """Delete this job's key. True if a key was there. Forward secrecy is the point:
        once the buyer has accepted, the stored ciphertext stops being readable at all."""
        path = Profile.job_key_path(job_id, home)
        try:
            path.unlink()
        except FileNotFoundError:
            return False
        return True


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
