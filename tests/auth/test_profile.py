"""The local profile and key: location, permissions, atomic writes, clear errors."""

from __future__ import annotations

import asyncio
import os
import stat
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest

import kontra.codec.age as age
from kontra.profile import (
    KEY_FILE,
    PROFILE_FILE,
    NotLoggedIn,
    Profile,
    ProfileError,
    kontra_home,
    profile_lock,
)

from ._fakes import make_profile


def mode(path: Path) -> int:
    return stat.S_IMODE(path.stat().st_mode)


@pytest.fixture
def loose_umask() -> Iterator[None]:
    """Permissions must not depend on the caller's umask."""
    old = os.umask(0)
    try:
        yield
    finally:
        os.umask(old)


def test_kontra_home(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("KONTRA_HOME", str(tmp_path / "k"))
    assert kontra_home() == tmp_path / "k"
    monkeypatch.setenv("KONTRA_HOME", "")
    assert kontra_home() == Path.home() / ".kontra"
    monkeypatch.delenv("KONTRA_HOME")
    assert kontra_home() == Path.home() / ".kontra"


def test_save_creates_private_files_and_directory(tmp_path: Path, loose_umask: None) -> None:
    home = tmp_path / "a" / "home"
    path = make_profile().save(home)
    assert path == home / PROFILE_FILE
    assert mode(home) == 0o700
    assert mode(path) == 0o600
    make_profile(refresh_token="r9").save(home)  # replacing keeps it private
    assert mode(path) == 0o600
    assert sorted(p.name for p in home.iterdir()) == [PROFILE_FILE]  # no temp files left


def test_existing_directory_is_left_alone(tmp_path: Path, loose_umask: None) -> None:
    tmp_path.chmod(0o755)
    make_profile().save(tmp_path)
    assert mode(tmp_path) == 0o755
    assert mode(tmp_path / PROFILE_FILE) == 0o600


def test_round_trip_and_remembered_home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KONTRA_HOME", str(tmp_path / "env"))
    p = make_profile(public_key="age1xyz", tls=True, dev=False)
    assert p.home is None
    p.save()
    assert p.home == tmp_path / "env"
    loaded = Profile.load()
    assert loaded == p
    assert loaded.home == tmp_path / "env"
    other = tmp_path / "other"
    loaded.refresh_token = "r2"
    loaded.save(other)
    assert Profile.load(other).refresh_token == "r2"
    assert Profile.load(str(tmp_path / "env")).refresh_token == "r0"
    loaded.refresh_token = "r3"
    loaded.save()  # back where it was last saved
    assert Profile.load(other).refresh_token == "r3"


def test_missing_profile_says_how_to_log_in(tmp_path: Path) -> None:
    with pytest.raises(NotLoggedIn, match="kontra login") as ei:
        Profile.load(tmp_path)
    assert str(tmp_path / PROFILE_FILE) in str(ei.value)


@pytest.mark.parametrize(
    "content",
    [
        b"{not json",
        b'{"gate_url": "http://g"}',
        make_profile().model_dump_json().replace('"tls"', '"tsl"').encode(),
        make_profile().model_dump_json().replace('"alice"', '"Alice"').encode(),
        make_profile().model_dump_json().replace("http://gate.test", "file:///etc").encode(),
        make_profile().model_dump_json().replace("+00:00", "").replace("Z", "").encode(),
    ],
    ids=["json", "missing", "unknown-field", "bad-handle", "bad-url", "naive-time"],
)
def test_invalid_profile_is_a_clear_error(tmp_path: Path, content: bytes) -> None:
    (tmp_path / PROFILE_FILE).write_bytes(content)
    with pytest.raises(ProfileError, match="kontra login") as ei:
        Profile.load(tmp_path)
    assert not isinstance(ei.value, NotLoggedIn)


# --------------------------------------------------------------------------- key


def test_ensure_identity_creates_a_private_age_keygen_file(
    tmp_path: Path, loose_umask: None
) -> None:
    home = tmp_path / "h"
    secret, public = Profile.ensure_identity(home)
    key = home / KEY_FILE
    assert mode(home) == 0o700
    assert mode(key) == 0o600
    lines = key.read_text().splitlines()
    assert lines[0].startswith("# created: ")
    assert lines[1] == f"# public key: {public}"
    assert lines[2] == secret
    assert age.is_identity(secret)
    assert age.public_key_of(secret) == public
    assert Profile.identity(home) == secret
    assert sorted(p.name for p in home.iterdir()) == [KEY_FILE]


def test_ensure_identity_never_regenerates(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[int] = []
    real = age.generate_identity

    def counting() -> tuple[str, str]:
        calls.append(1)
        return real()

    monkeypatch.setattr(age, "generate_identity", counting)
    first = Profile.ensure_identity(tmp_path)
    assert Profile.ensure_identity(tmp_path) == first
    assert calls == [1]


def test_ensure_identity_loses_a_race_gracefully(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Another process writes its key between our existence check and our link: theirs wins."""
    theirs, their_public = age.generate_identity()
    real = age.generate_identity

    def racing() -> tuple[str, str]:
        (tmp_path / KEY_FILE).write_text(theirs + "\n")
        return real()

    monkeypatch.setattr(age, "generate_identity", racing)
    assert Profile.ensure_identity(tmp_path) == (theirs, their_public)
    assert Profile.identity(tmp_path) == theirs
    assert sorted(p.name for p in tmp_path.iterdir()) == [KEY_FILE]


def test_missing_key_says_how_to_fix_it(tmp_path: Path) -> None:
    with pytest.raises(NotLoggedIn, match="kontra login"):
        Profile.identity(tmp_path)


@pytest.mark.parametrize(
    "content",
    [
        "",
        "# only a comment\n",
        "AGE-SECRET-KEY-1NOTAKEY\n",
        "{secret} \n",  # trailing space: not stripped, so not valid
        "{secret}\n{secret}\n",
        "age1notasecret\n",
    ],
)
def test_malformed_key_file(tmp_path: Path, content: str) -> None:
    secret, _ = age.generate_identity()
    (tmp_path / KEY_FILE).write_text(content.format(secret=secret))
    with pytest.raises(ProfileError, match="AGE-SECRET-KEY-1"):
        Profile.identity(tmp_path)


def test_key_file_with_crlf_and_comments_is_accepted(tmp_path: Path) -> None:
    secret, _ = age.generate_identity()
    (tmp_path / KEY_FILE).write_bytes(f"# c\r\n\r\n{secret}\r\n".encode())
    assert Profile.identity(tmp_path) == secret


# --------------------------------------------------------------------------- lock


async def test_profile_lock_is_exclusive(tmp_path: Path) -> None:
    order: list[str] = []

    async def holder() -> None:
        async with profile_lock(tmp_path):
            order.append("a-in")
            await asyncio.sleep(0.2)
            order.append("a-out")

    async def waiter() -> None:
        await asyncio.sleep(0.05)
        async with profile_lock(tmp_path):
            order.append("b-in")

    await asyncio.gather(holder(), waiter())
    assert order == ["a-in", "a-out", "b-in"]
    assert mode(tmp_path / ".lock") == 0o600


async def test_profile_lock_without_a_directory_is_a_no_op(tmp_path: Path) -> None:
    async with profile_lock(None):
        pass
    async with profile_lock(tmp_path / "missing"):
        pass
    assert not (tmp_path / "missing").exists()


# --------------------------------------------------------------------------- planted files


@pytest.fixture
def foreign(monkeypatch: pytest.MonkeyPatch) -> Callable[[Path], None]:
    """Make ``path`` look like another (non-root) user's file: really, when we are root;
    otherwise by making us someone else."""

    def make(path: Path) -> None:
        if os.geteuid() == 0:
            os.chown(path, 4242, 4242)
        else:
            owner = path.stat().st_uid
            monkeypatch.setattr(os, "geteuid", lambda: owner + 1)

    return make


def test_a_key_planted_by_another_user_is_refused(
    tmp_path: Path, foreign: Callable[[Path], None]
) -> None:
    """In a shared KONTRA_HOME another user can create ``key`` first. ensure_identity would
    adopt it and login() would register its public key: every delivery to the party would
    then be encrypted to the attacker."""
    theirs, _ = age.generate_identity()
    key = tmp_path / KEY_FILE
    key.write_text(theirs + "\n")
    key.chmod(0o600)
    foreign(key)
    with pytest.raises(ProfileError, match="another user"):
        Profile.identity(tmp_path)
    with pytest.raises(ProfileError, match="another user"):
        Profile.ensure_identity(tmp_path)


def test_a_profile_planted_by_another_user_is_refused(
    tmp_path: Path, foreign: Callable[[Path], None]
) -> None:
    path = make_profile(gate_url="https://evil.example").save(tmp_path)
    foreign(path)
    with pytest.raises(ProfileError, match="another user") as ei:
        Profile.load(tmp_path)
    assert not isinstance(ei.value, NotLoggedIn)


@pytest.mark.parametrize("name", [KEY_FILE, PROFILE_FILE])
def test_world_writable_credentials_are_refused(tmp_path: Path, name: str) -> None:
    Profile.ensure_identity(tmp_path)
    make_profile().save(tmp_path)
    (tmp_path / name).chmod(0o666)
    with pytest.raises(ProfileError, match="chmod 600"):
        Profile.identity(tmp_path) if name == KEY_FILE else Profile.load(tmp_path)


def test_readable_root_owned_or_group_readable_credentials_are_accepted(tmp_path: Path) -> None:
    """A container's mounted secret is typically 0644 (and root-owned): readable is fine."""
    secret, _ = Profile.ensure_identity(tmp_path)
    (tmp_path / KEY_FILE).chmod(0o644)
    assert Profile.identity(tmp_path) == secret


def test_a_fifo_in_place_of_the_key_does_not_hang(tmp_path: Path) -> None:
    os.mkfifo(tmp_path / KEY_FILE)
    with pytest.raises(ProfileError, match="regular file"):
        Profile.identity(tmp_path)


async def test_the_lock_does_not_follow_a_planted_symlink(tmp_path: Path) -> None:
    target = tmp_path / "elsewhere"
    (tmp_path / ".lock").symlink_to(target)
    with pytest.raises(ProfileError, match="lock"):
        async with profile_lock(tmp_path):
            pass
    assert not target.exists()


def test_an_unreadable_key_is_a_clear_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    Profile.ensure_identity(tmp_path)
    real_open = os.open

    def denied(path: Any, flags: int, *a: Any) -> int:
        if Path(path).name == KEY_FILE:
            raise PermissionError(13, "Permission denied")
        return real_open(path, flags, *a)

    monkeypatch.setattr(os, "open", denied)
    with pytest.raises(ProfileError, match="cannot read"):
        Profile.identity(tmp_path)
