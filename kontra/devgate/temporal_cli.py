"""The one pinned Temporal CLI, downloaded once and verified by checksum (decisions I34, OD10).

``kontra dev up`` and the dev-server tests both need the Temporal CLI 1.9.1 dev server (server
1.32, standalone Nexus operations). This module is the single place that pins its version and the
sha256 of the release archive **and** of the extracted binary, downloads it over HTTPS into the
Makefile's cache path, verifies it, and hands back the path. ``$KONTRA_TEMPORAL_CLI`` overrides
it with a binary already on disk (version-checked). temporalio's own downloader verifies no
pinned digest, so :mod:`kontra.testing.devserver` goes through :func:`ensure` too.

The Makefile's ``$(TEMPORAL_CLI)`` rule is ``python -m kontra.devgate.temporal_cli fetch``.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import re
import stat
import sys
import tarfile
import tempfile
from collections.abc import Callable
from pathlib import Path

__all__ = [
    "ARCHIVE_SHA256",
    "BINARY_SHA256",
    "CLI_ENV",
    "MIN_SERVER",
    "VERSION",
    "cache_path",
    "ensure",
    "platform_key",
    "server_version",
]

#: Overrides the download with a binary already on disk.
CLI_ENV = "KONTRA_TEMPORAL_CLI"
VERSION = "1.9.1"
#: The server this CLI's dev server must be at least (standalone Nexus operations; decisions I4).
MIN_SERVER = (1, 32)

#: sha256 of ``temporal_cli_<VERSION>_<os>_<arch>.tar.gz`` (equal to the Makefile's pins; a test
#: holds them equal).
ARCHIVE_SHA256 = {
    ("linux", "amd64"): "09a0326a51db84d02735e53542b9ebd8c4758daf47482a9ab0abce15844e60d5",
    ("linux", "arm64"): "6c57c352d52fc3df34412376fd9ba6f74b7e3ace8e426e6cba8600156d36a145",
    ("darwin", "amd64"): "48cdd6c84c56e27ae8e4d9a47052b89289a9c552dee029b49dd312c6fb72873f",
    ("darwin", "arm64"): "41e0425378fcb4fb5766340b97435e20fe47bbff2d7bf644ec2d51f7662b7c56",
}
#: sha256 of the extracted ``temporal`` binary. Pinned where a binary has been hashed; where it
#: is not, the archive digest is the pin on that platform and the binary check is skipped.
BINARY_SHA256 = {
    ("linux", "amd64"): "712217694cac9f786a72d4b6c876cd15f3dcf1bf31434d9fed6af25242a327a0",
}

_RELEASE = "https://github.com/temporalio/cli/releases/download"
_VERSION_LINE = re.compile(r"Server\s+(\d+)\.(\d+)\.(\d+)", re.ASCII)


class TemporalCLIError(Exception):
    """A pinned CLI could not be produced (download, checksum, extraction, or a bad override)."""


def platform_key() -> tuple[str, str]:
    """``(os, arch)`` the way the release assets are named (``linux``/``darwin``,
    ``amd64``/``arm64``)."""
    system = "darwin" if sys.platform == "darwin" else "linux"
    machine = platform.machine().lower()
    arch = {"x86_64": "amd64", "amd64": "amd64", "aarch64": "arm64", "arm64": "arm64"}.get(machine)
    if arch is None:
        raise TemporalCLIError(f"no pinned Temporal CLI for machine {machine!r}; set {CLI_ENV}")
    return system, arch


def cache_path(version: str = VERSION) -> Path:
    """``${XDG_CACHE_HOME:-~/.cache}/kontra-sdk/temporal-cli-<version>/temporal`` -- the
    Makefile's path, so a checkout's ``make test`` and ``kontra dev up`` share one download."""
    base = os.environ.get("XDG_CACHE_HOME") or str(Path.home() / ".cache")
    return Path(base) / "kontra-sdk" / f"temporal-cli-{version}" / "temporal"


def server_version(cli: Path) -> tuple[int, int, int] | None:
    """The server version this CLI reports (``temporal --version`` -> ``Server X.Y.Z``), or
    None when it cannot be told."""
    import subprocess

    try:
        out = subprocess.run(  # noqa: S603 -- fixed argv, no shell
            [str(cli), "--version"], capture_output=True, text=True, timeout=10, check=False
        ).stdout
    except (OSError, subprocess.SubprocessError):
        return None
    match = _VERSION_LINE.search(out)
    return (int(match[1]), int(match[2]), int(match[3])) if match else None


def ensure(*, version: str = VERSION, say: Callable[[str], None] | None = None) -> Path:
    """A verified Temporal CLI, downloading it once if need be. ``$KONTRA_TEMPORAL_CLI`` (a
    binary on disk) wins and is version-checked. Raises :class:`TemporalCLIError` on any
    mismatch, leaving nothing half-written."""
    say = say or (lambda _msg: None)
    override = os.environ.get(CLI_ENV)
    if override:
        cli = Path(override)
        if not (cli.is_file() and os.access(cli, os.X_OK)):
            raise TemporalCLIError(f"{CLI_ENV}={override} is not an executable file")
        reported = server_version(cli)
        if reported is not None and reported[:2] < MIN_SERVER:
            raise TemporalCLIError(
                f"{CLI_ENV}={override} reports server {reported[0]}.{reported[1]}, but the dev "
                f"stack needs {MIN_SERVER[0]}.{MIN_SERVER[1]}+ (standalone Nexus operations)"
            )
        return cli

    target = cache_path(version)
    _own_dir(target.parent)
    if _owned_file(target) and _cache_is_good(target):
        return target
    return _download(version, target, say)


# --------------------------------------------------------------------------- the cache


def _sidecar(target: Path) -> Path:
    return target.with_name("temporal.sha256")


def _own_dir(path: Path) -> None:
    """Make ``path`` (the version's cache directory) this user's own: what sits in it is run.
    ``XDG_CACHE_HOME`` may name a shared place; there, whoever plants a binary and a sidecar
    first would choose what runs as us. Created 0700; an existing one of ours that others may
    write is tightened to 0700; one that is not ours (or not a real directory) is refused."""
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    if not hasattr(os, "getuid"):
        return
    st = os.lstat(path)
    if not stat.S_ISDIR(st.st_mode) or st.st_uid != os.getuid():
        raise TemporalCLIError(
            f"{path} is not a directory of yours; the Temporal CLI it holds would run as you. "
            f"Remove it, or set {CLI_ENV} to a binary you trust"
        )
    if st.st_mode & 0o022:
        path.chmod(0o700)


def _owned_file(path: Path) -> bool:
    """A regular file (not a link) of this user's that no one else may write: the only kind
    of cached binary or sidecar the fast path may trust."""
    try:
        st = os.lstat(path)
    except FileNotFoundError:
        return False
    if not stat.S_ISREG(st.st_mode) or st.st_mode & 0o022:
        return False
    return not hasattr(os, "getuid") or st.st_uid == os.getuid()


def _cache_is_good(target: Path) -> bool:
    """A cached binary to trust: the sidecar records its size and mtime (the fast path), else
    the binary is re-hashed against the pinned :data:`BINARY_SHA256` and, if it matches, a
    sidecar is written so the next ``up`` takes the fast path. Without a pin for this platform
    an unverifiable cache is rejected (the archive-verified download is the fallback)."""
    try:
        if not _owned_file(_sidecar(target)):
            raise ValueError("an untrusted sidecar")
        doc = json.loads(_sidecar(target).read_text(encoding="utf-8"))
        want_size, want_mtime = int(doc["size"]), float(doc["mtime"])
    except (OSError, ValueError, KeyError, TypeError):
        pass
    else:
        st = target.stat()
        if st.st_size == want_size and st.st_mtime == want_mtime:
            return True
    pinned = BINARY_SHA256.get(platform_key())
    if pinned is not None and _sha256(target) == pinned:
        _write_sidecar(target, pinned)
        return True
    return False


def _write_sidecar(target: Path, sha: str) -> None:
    st = target.stat()
    doc = json.dumps({"sha256": sha, "size": st.st_size, "mtime": st.st_mtime}).encode()
    path = _sidecar(target)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
    with os.fdopen(fd, "wb") as f:
        os.fchmod(
            f.fileno(), 0o644
        )  # whatever the umask: the fast path trusts no group-writable file
        f.write(doc)


# --------------------------------------------------------------------------- the download


def _download(version: str, target: Path, say: Callable[[str], None]) -> Path:
    import httpx

    key = platform_key()
    archive_sha = ARCHIVE_SHA256.get(key)
    if archive_sha is None:
        raise TemporalCLIError(
            f"no pinned Temporal CLI archive for {key[0]}/{key[1]}; set {CLI_ENV}"
        )
    name = f"temporal_cli_{version}_{key[0]}_{key[1]}.tar.gz"
    url = f"{_RELEASE}/v{version}/{name}"
    say(f"downloading {url}")
    target.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="kontra-temporal-cli-") as tmp:
        archive = Path(tmp) / name
        # trust_env: honour a corporate HTTPS proxy. HTTPS only.
        with (
            httpx.Client(trust_env=True, follow_redirects=True, timeout=120) as http,
            http.stream("GET", url) as resp,
        ):
            resp.raise_for_status()
            with archive.open("wb") as f:
                for chunk in resp.iter_bytes():
                    f.write(chunk)
        got = _sha256(archive)
        if got != archive_sha:
            raise TemporalCLIError(
                f"Temporal CLI archive checksum mismatch: got {got}, want {archive_sha}"
            )
        extracted = _extract_binary(archive, Path(tmp))
        binary_sha = _sha256(extracted)
        pinned = BINARY_SHA256.get(key)
        if pinned is not None and binary_sha != pinned:
            raise TemporalCLIError(
                f"Temporal CLI binary checksum mismatch: got {binary_sha}, want {pinned}"
            )
        extracted.chmod(0o755)
        os.replace(extracted, target)
    _write_sidecar(target, binary_sha)
    say(f"Temporal CLI {version} at {target}")
    return target


def _extract_binary(archive: Path, into: Path) -> Path:
    """Extract only the ``temporal`` member, refusing a traversal (``..`` or absolute) or a
    link member -- a malicious archive must not write outside ``into`` or plant a symlink."""
    out = into / "temporal"
    with tarfile.open(archive, "r:gz") as tar:
        member = tar.getmember("temporal")
        if member.name != "temporal" or not member.isfile() or member.issym() or member.islnk():
            raise TemporalCLIError(
                "the Temporal CLI archive's `temporal` member is not a plain file"
            )
        src = tar.extractfile(member)
        if src is None:
            raise TemporalCLIError("the Temporal CLI archive has no readable `temporal` member")
        with src, out.open("wb") as f:
            while chunk := src.read(1 << 20):
                f.write(chunk)
    return out


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while chunk := f.read(1 << 20):
            h.update(chunk)
    return h.hexdigest()


# --------------------------------------------------------------------------- `python -m ... fetch`


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m kontra.devgate.temporal_cli")
    sub = parser.add_subparsers(dest="cmd", required=True)
    fetch = sub.add_parser("fetch", help="download and verify the pinned Temporal CLI")
    fetch.add_argument("--version", default=VERSION)
    args = parser.parse_args(argv)
    if args.cmd == "fetch":
        try:
            path = ensure(version=args.version, say=lambda m: print(m, file=sys.stderr))
        except TemporalCLIError as e:
            print(f"error: {e}", file=sys.stderr)
            return 1
        print(path)
        # chmod is set, but be loud if the OS dropped the bit (a noexec cache mount).
        if not os.access(path, os.X_OK):
            print(f"warning: {path} is not executable", file=sys.stderr)
        return 0
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
