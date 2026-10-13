"""Finding and running nexgen (``temporalio/nexgen``) for ``kontra pull`` (PRD §11.1).

nexgen is a Rust binary and pre-1.0, so it is never a dependency of the package (§11.1:
"not on the seller's hot path"). Where it comes from, in order:

1. ``$KONTRA_NEXGEN``: a binary you point at;
2. ``nexgen`` on ``PATH``;
3. the pinned release (:data:`VERSION`) for this platform, downloaded once into
   ``$KONTRA_HOME/bin`` with the archive's sha256 checked against :data:`SHA256`.

The Linux release links glibc 2.39. On an older glibc the binary cannot start, so it runs
inside :data:`RUNNER_IMAGE` (Ubuntu 24.04, pinned by digest) with Docker: no network, a
read-only root, only the output directory mounted, as the calling user.
"""

from __future__ import annotations

import hashlib
import os
import platform
import shutil
import stat
import subprocess
import sys
import tarfile
import tempfile
import zipfile
from dataclasses import dataclass
from pathlib import Path

from kontra.cli._common import fail

__all__ = [
    "NEXGEN_ENV",
    "RUNNER_IMAGE",
    "SHA256",
    "VERSION",
    "Nexgen",
    "find_nexgen",
    "release_target",
]

VERSION = "v0.2.5"
NEXGEN_ENV = "KONTRA_NEXGEN"
_URL = "https://github.com/temporalio/nexgen/releases/download/{v}/nexgen-{v}-{target}.{ext}"
#: sha256 of each release archive of :data:`VERSION`, as GitHub's release API reports them
#: (and checked against downloads of the Linux and macOS archives when pinned).
SHA256 = {
    "x86_64-unknown-linux-gnu": "4c441c5b31fe23d950bcf76be6447003bcf6cc75f073d892adb4a7e961a1e632",
    "aarch64-unknown-linux-gnu": "b81bdae3adfcfdcc73eed83cc98374237840592a286be9a68cd2dcc350641baa",
    "x86_64-apple-darwin": "e060fc826a4ec27315a81ec9d0987efbfdfb62c22a8c0a7bfa7f9b18b74f65f7",
    "aarch64-apple-darwin": "eefa0275ea852c1e6c2b5cb60752c421cabc479f6da24a153e0971cf7a86b98d",
    "x86_64-pc-windows-msvc": "cb2792d87ab38aeefbc920b7a652c303b062e995d925aacefac2931ab4e80cae",
    "aarch64-pc-windows-msvc": "aac5ff6bd820cca53841210cbd4d586486e35172c65352b999059d45015469bb",
}
#: Where a Linux nexgen runs when this machine's glibc is too old for it.
RUNNER_IMAGE = (
    "ubuntu:24.04@sha256:534baea6a22c03a63003dbc8dbe78fe34bc0d7e595d9a9dc9834884ff530eb55"
)
_MAX_ARCHIVE = 100 * 1024 * 1024
_TIMEOUT = 600


def release_target(system: str | None = None, machine: str | None = None) -> str | None:
    """The release target for this platform, or None when nexgen ships none."""
    system = (system or platform.system()).lower()
    machine = (machine or platform.machine()).lower()
    arch = {"x86_64": "x86_64", "amd64": "x86_64", "aarch64": "aarch64", "arm64": "aarch64"}.get(
        machine
    )
    if arch is None:
        return None
    vendor = {
        "linux": "unknown-linux-gnu",
        "darwin": "apple-darwin",
        "windows": "pc-windows-msvc",
    }.get(system)
    return f"{arch}-{vendor}" if vendor else None


def _check_private(path: Path) -> None:
    """Run only a regular file we (or root) own that nobody else can write: a binary in a
    shared directory could otherwise be swapped for anything."""
    st = path.stat()
    if not stat.S_ISREG(st.st_mode):
        fail(f"{path} is not a regular file")
    if hasattr(os, "geteuid") and (
        st.st_uid not in (os.geteuid(), 0) or st.st_mode & (stat.S_IWGRP | stat.S_IWOTH)
    ):
        fail(
            f"{path} may be written by another user; refusing to run it (chmod go-w, or delete it)"
        )


def _download(target: str, dest: Path) -> None:
    """Fetch the release archive, check its sha256, and put the binary at ``dest``."""
    import httpx

    windows = "windows" in target
    url = _URL.format(v=VERSION, target=target, ext="zip" if windows else "tar.gz")
    member = "nexgen.exe" if windows else "nexgen"
    dest.parent.mkdir(parents=True, exist_ok=True)
    print(f"downloading nexgen {VERSION} ({target})...", file=sys.stderr, flush=True)
    with tempfile.TemporaryDirectory(dir=dest.parent) as tmp:
        archive = Path(tmp) / "archive"
        digest = hashlib.sha256()
        size = 0
        try:
            with httpx.stream("GET", url, follow_redirects=True, timeout=60) as r:
                r.raise_for_status()
                with archive.open("wb") as f:
                    for chunk in r.iter_bytes():
                        size += len(chunk)
                        if size > _MAX_ARCHIVE:
                            fail(f"{url} is larger than expected; not using it")
                        digest.update(chunk)
                        f.write(chunk)
        except httpx.HTTPError as e:
            fail(f"cannot download nexgen from {url}: {e}")
        if digest.hexdigest() != SHA256[target]:
            fail(
                f"{url} does not have the pinned sha256 {SHA256[target]} (got "
                f"{digest.hexdigest()}); not using it"
            )
        binary = Path(tmp) / member
        try:
            if windows:
                with zipfile.ZipFile(archive) as z:
                    binary.write_bytes(z.read(member))
            else:
                with tarfile.open(archive, "r:gz") as t:
                    info = t.getmember(member)
                    src = t.extractfile(info) if info.isfile() else None
                    if src is None:
                        fail(f"{url} has no regular file {member}")
                    binary.write_bytes(src.read())
        except KeyError:
            fail(f"{url} has no {member}")
        binary.chmod(0o755)
        os.replace(binary, dest)


@dataclass(frozen=True)
class Nexgen:
    """A nexgen binary and whether it must run in :data:`RUNNER_IMAGE`."""

    binary: Path
    container: bool = False

    def generate(
        self, lang: str, document: Path, output: Path, *, package: str | None = None
    ) -> None:
        """``nexgen <lang> <document> --output <output>``. The document must be inside
        ``output`` (only that directory is mounted into the container)."""
        if self.container:
            if "," in str(self.binary) or "," in str(output):
                fail(f"cannot mount a path with a ',' into Docker: {output} / {self.binary}")
            # The directory keeps its name in the container: Go and Java derive the
            # package from it.
            inside = f"/w/{output.name}"
            cmd = [
                shutil.which("docker") or "docker",
                "run",
                "--rm",
                "--network",
                "none",
                "--read-only",
            ]
            cmd += ["--tmpfs", "/tmp"]  # noqa: S108 -- the container's own, private /tmp
            cmd += ["--security-opt", "no-new-privileges"]
            if hasattr(os, "getuid"):
                cmd += ["--user", f"{os.getuid()}:{os.getgid()}"]
            # --mount, not -v: a ':' in a path would split a -v spec.
            cmd += ["--mount", f"type=bind,src={self.binary},dst=/usr/local/bin/nexgen,readonly"]
            cmd += ["--mount", f"type=bind,src={output},dst={inside}"]
            cmd += ["-w", inside, RUNNER_IMAGE, "nexgen"]
            cmd += [lang, f"{inside}/{document.name}", "--output", inside]
            if package is not None:
                cmd += ["--package-name", package]
            proc = _run(cmd, cwd=None)
        else:
            # Run from the parent so the output path's last segment is the directory name.
            cmd = [str(self.binary), lang, str(document), "--output", str(output)]
            if package is not None:
                cmd += ["--package-name", package]
            proc = _run(cmd, cwd=output.parent)
        if proc.returncode != 0:
            detail = (proc.stderr or proc.stdout).strip()[-2000:]
            fail(f"nexgen {lang} failed (exit {proc.returncode}): {detail}")


def _run(cmd: list[str], cwd: Path | None) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(  # noqa: S603 -- argv, no shell
            cmd, cwd=cwd, capture_output=True, text=True, timeout=_TIMEOUT, check=False
        )
    except FileNotFoundError:
        fail(f"cannot run {cmd[0]}: not found")
    except subprocess.TimeoutExpired:
        fail(f"nexgen did not finish within {_TIMEOUT}s")


def _too_old_glibc(binary: Path) -> bool:
    """Whether ``binary`` cannot start here because it needs a newer glibc (Linux)."""
    if not sys.platform.startswith("linux"):
        return False
    try:
        probe = subprocess.run(  # noqa: S603 -- argv, no shell
            [str(binary), "--version"], capture_output=True, timeout=30, check=False
        )
    except OSError as e:
        fail(f"cannot run {binary}: {e}")
    except subprocess.TimeoutExpired:
        fail(f"{binary} --version did not answer")
    return probe.returncode != 0 and b"GLIBC_" in probe.stderr


def _docker_ok() -> bool:
    docker = shutil.which("docker")
    if docker is None:
        return False
    probe = subprocess.run(  # noqa: S603 -- argv, no shell
        [docker, "version", "--format", "{{.Server.Version}}"],
        capture_output=True,
        timeout=60,
        check=False,
    )
    return probe.returncode == 0


def find_nexgen(home: Path) -> Nexgen:
    """The nexgen to use (see the module docstring); downloads it on first use."""
    given = os.environ.get(NEXGEN_ENV)
    if given:
        binary = Path(given).expanduser().resolve()
        if not binary.is_file():
            fail(f"{NEXGEN_ENV}={given} is not a file")
    elif (on_path := shutil.which("nexgen")) is not None:
        binary = Path(on_path).resolve()
    else:
        target = release_target()
        if target is None:
            fail(
                f"nexgen ships no binary for {platform.system()} {platform.machine()}; "
                f"build it (github.com/temporalio/nexgen) and set {NEXGEN_ENV}"
            )
        binary = (
            home / "bin" / f"nexgen-{VERSION}" / ("nexgen.exe" if "windows" in target else "nexgen")
        )
        if not binary.is_file():
            if not home.exists():  # as `kontra login` would make it: private
                home.mkdir(mode=0o700, parents=True)
            _download(target, binary)
        _check_private(binary)
    if not _too_old_glibc(binary):
        return Nexgen(binary)
    if not _docker_ok():
        libc = " ".join(platform.libc_ver()) or "an older glibc"
        fail(
            f"{binary} needs glibc 2.39 and this machine has {libc}; install Docker (nexgen "
            f"then runs in {RUNNER_IMAGE.split('@', 1)[0]}) or point {NEXGEN_ENV} at a nexgen "
            "built here"
        )
    return Nexgen(binary, container=True)
