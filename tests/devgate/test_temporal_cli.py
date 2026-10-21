"""``kontra.devgate.temporal_cli``: the pins match the Makefile, extraction is safe, the cache
and every directory above it are the user's, and a download never crosses filesystems."""

from __future__ import annotations

import errno
import hashlib
import io
import json
import os
import re
import stat
import tarfile
import tempfile
from pathlib import Path
from typing import Any

import httpx
import pytest

from kontra.devgate import temporal_cli as tc

ROOT = Path(__file__).resolve().parents[2]
_HTTPX_CLIENT = httpx.Client


def test_the_archive_pins_equal_the_makefile() -> None:
    """One pin (OD10): until the Makefile's ``temporal-cli`` target calls this module, its
    checksums and these must agree, or ``make test`` and ``kontra dev up`` fetch different CLIs."""
    makefile = (ROOT / "Makefile").read_text(encoding="utf-8")
    version = re.search(r"TEMPORAL_CLI_VERSION\s*:=\s*(\S+)", makefile)
    assert version is not None
    assert version[1] == tc.VERSION
    for (os_name, arch), sha in tc.ARCHIVE_SHA256.items():
        line = re.search(rf"TEMPORAL_CLI_SHA256_{os_name}_{arch}\s*:=\s*(\S+)", makefile)
        assert line is not None, f"{os_name}_{arch} not in the Makefile"
        assert line[1] == sha, f"{os_name}/{arch} pin differs from the Makefile"


def test_platform_key_and_cache_path(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    os_name, arch = tc.platform_key()
    assert os_name in ("linux", "darwin")
    assert arch in ("amd64", "arm64")
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path))
    assert tc.cache_path() == tmp_path / "kontra-sdk" / f"temporal-cli-{tc.VERSION}" / "temporal"


def _cli(tmp_path: Path, printed: str) -> Path:
    """An executable that answers ``--version`` with ``printed``."""
    cli = tmp_path / "bin" / "temporal"
    cli.parent.mkdir(parents=True, exist_ok=True)
    cli.write_text(f"#!/bin/sh\necho '{printed}'\n")
    cli.chmod(0o755)
    return cli


@pytest.mark.parametrize(
    ("printed", "match"),
    [
        ("temporal version 1.4.0", "cannot tell the server version"),  # no Server X.Y.Z
        ("", "cannot tell the server version"),  # prints nothing
        ("temporal version 1.3.0 (server 1.27.1, UI 2.30.0)", "reports server 1.27"),
        ("temporal version 1.3.0 (Server 1.27.1, UI 2.30.0)", "reports server 1.27"),
    ],
)
def test_an_override_of_an_unknown_or_old_server_is_refused(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, printed: str, match: str
) -> None:
    """The version check fails closed: an override whose ``--version`` names no server (or
    names it in another case) is not waved through as unchecked."""
    monkeypatch.setenv(tc.CLI_ENV, str(_cli(tmp_path, printed)))
    with pytest.raises(tc.TemporalCLIError, match=match):
        tc.ensure()


def test_an_override_of_a_new_enough_server_is_used(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    cli = _cli(tmp_path, "temporal version 1.9.1 (Server 1.32.0, UI 2.54.1)")
    monkeypatch.setenv(tc.CLI_ENV, str(cli))
    assert tc.ensure() == cli


def test_an_override_that_is_not_executable_is_refused(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    missing = tmp_path / "nope"
    monkeypatch.setenv(tc.CLI_ENV, str(missing))
    with pytest.raises(tc.TemporalCLIError, match="not an executable file"):
        tc.ensure()


def test_a_link_member_in_the_archive_is_refused(tmp_path: Path) -> None:
    """A malicious archive must not plant a symlink in place of the binary."""
    archive = tmp_path / "evil.tar.gz"
    with tarfile.open(archive, "w:gz") as tar:
        info = tarfile.TarInfo("temporal")
        info.type = tarfile.SYMTYPE
        info.linkname = "/etc/passwd"
        tar.addfile(info)
    with pytest.raises(tc.TemporalCLIError, match="not a plain file"):
        tc._extract_binary(archive, tmp_path)


def test_the_cache_fast_path(monkeypatch: pytest.MonkeyPatch) -> None:
    """A verified cache is accepted without a download; the override path returns it too."""
    cache = tc.cache_path()
    if not cache.is_file():
        pytest.skip("no cached Temporal CLI on this machine")
    monkeypatch.delenv(tc.CLI_ENV, raising=False)
    first = tc.ensure()
    assert first == cache
    assert tc._sidecar(cache).exists()  # written so the next `up` is fast
    assert tc.ensure() == cache
    # The server it reports satisfies the minimum (standalone Nexus operations).
    reported = tc.server_version(cache)
    assert reported is not None
    assert reported[:2] >= tc.MIN_SERVER


# --------------------------------------------------------------------------- the cache is the user's


def _plant(cache: Path, *, sidecar_mode: int = 0o644, binary_mode: int = 0o755) -> Path:
    """A binary that is not the pinned one, with a sidecar that matches its size and mtime."""
    target = cache / "kontra-sdk" / f"temporal-cli-{tc.VERSION}" / "temporal"
    target.parent.mkdir(parents=True)
    target.write_bytes(b"#!/bin/sh\necho planted\n")
    target.chmod(binary_mode)
    st = target.stat()
    sidecar = tc._sidecar(target)
    sidecar.write_text(json.dumps({"sha256": "0" * 64, "size": st.st_size, "mtime": st.st_mtime}))
    sidecar.chmod(sidecar_mode)
    return target


@pytest.fixture
def no_download(monkeypatch: pytest.MonkeyPatch) -> list[Path]:
    """No test downloads: :func:`tc._download` records where it would have written."""
    calls: list[Path] = []

    def fake(version: str, target: Path, say: object) -> Path:
        calls.append(target)
        raise tc.TemporalCLIError("download refused in tests")

    monkeypatch.delenv(tc.CLI_ENV, raising=False)
    monkeypatch.setattr(tc, "_download", fake)
    return calls


def test_a_sidecar_others_may_write_is_not_trusted(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, no_download: list[Path]
) -> None:
    """The fast path trusts size and mtime only in a sidecar no one else could have written:
    in a shared ``XDG_CACHE_HOME`` a planted binary with a matching, group-writable sidecar is
    re-verified (here: not the pinned binary, so it is downloaded over)."""
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path))
    target = _plant(tmp_path, sidecar_mode=0o664)
    with pytest.raises(tc.TemporalCLIError, match="refused in tests"):
        tc.ensure()
    assert no_download == [target]


def test_a_binary_others_may_write_is_not_trusted(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, no_download: list[Path]
) -> None:
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path))
    target = _plant(tmp_path, binary_mode=0o775)
    with pytest.raises(tc.TemporalCLIError, match="refused in tests"):
        tc.ensure()
    assert no_download == [target]


def test_the_cache_directory_is_made_private(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, no_download: list[Path]
) -> None:
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path))
    target = _plant(tmp_path)
    target.parent.chmod(0o777)
    assert tc.ensure() == target  # its own sidecar matches: the fast path (no hash, no download)
    assert stat.S_IMODE(target.parent.stat().st_mode) == 0o700
    assert no_download == []


@pytest.mark.skipif(not hasattr(os, "geteuid") or os.geteuid() != 0, reason="needs root to chown")
def test_a_cache_directory_of_another_user_is_refused(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, no_download: list[Path]
) -> None:
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path))
    target = _plant(tmp_path)
    os.chown(target.parent, 65534, 65534)
    with pytest.raises(tc.TemporalCLIError, match="not a directory of yours"):
        tc.ensure()
    assert no_download == []


# --------------------------------------------------------------------------- the dev server uses it


async def test_the_dev_server_runs_the_verified_cli(monkeypatch: pytest.MonkeyPatch) -> None:
    """``kontra.testing.devserver`` goes through :func:`tc.ensure` (``$KONTRA_TEMPORAL_CLI``,
    else the pinned, checksum-verified download), never temporalio's own downloader; a
    version that cannot be verified is refused before anything runs."""
    from kontra.testing import devserver

    class Sentinel(Exception):
        pass

    def ensure(**_: object) -> Path:
        raise Sentinel

    monkeypatch.setattr(tc, "ensure", ensure)
    with pytest.raises(Sentinel):
        await devserver.start_dev_server()
    with pytest.raises(ValueError, match="only the pinned Temporal CLI"):
        await devserver.start_dev_server(version="v1.8.0")


# --------------------------------------------------------------------------- the download


def _release(monkeypatch: pytest.MonkeyPatch, status: int = 200) -> list[str]:
    """A release archive whose pins this platform's are (for the test), served by
    ``httpx.MockTransport``: the URLs fetched."""
    binary = b"#!/bin/sh\necho 'temporal version 1.9.1 (Server 1.32.0, UI 2.54.1)'\n"
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        info = tarfile.TarInfo("temporal")
        info.size, info.mode = len(binary), 0o755
        tar.addfile(info, io.BytesIO(binary))
    archive = buf.getvalue()
    key = tc.platform_key()
    monkeypatch.setitem(tc.ARCHIVE_SHA256, key, hashlib.sha256(archive).hexdigest())
    monkeypatch.setitem(tc.BINARY_SHA256, key, hashlib.sha256(binary).hexdigest())
    fetched: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        fetched.append(str(request.url))
        return httpx.Response(status, content=archive)

    def client(**kwargs: Any) -> httpx.Client:
        return _HTTPX_CLIENT(transport=httpx.MockTransport(handler), **kwargs)

    monkeypatch.setattr(httpx, "Client", client)
    return fetched


def test_fetch_under_the_makefiles_export_downloads_an_empty_cache(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """The Makefile exports ``KONTRA_TEMPORAL_CLI=<the cache path>`` into every recipe, its
    own fetch rule's too: on a fresh runner that names a file not downloaded yet, and
    ``fetch`` (``make temporal-cli``, so ``make test``/``seams``/``e2e``) must download it,
    not refuse it as a missing override."""
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "cache"))
    monkeypatch.setenv(tc.CLI_ENV, str(tc.cache_path()))
    fetched = _release(monkeypatch)
    assert tc.main(["fetch"]) == 0
    assert capsys.readouterr().out.strip() == str(tc.cache_path())
    assert tc.cache_path().is_file()
    assert len(fetched) == 1
    assert tc.main(["fetch"]) == 0  # the verified cache: no second download
    assert len(fetched) == 1


def test_the_download_is_renamed_within_one_filesystem(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """``rename(2)`` fails with EXDEV across filesystems (a tmpfs ``/tmp`` and the cache on
    disk): the work directory is beside the target, so the final rename never crosses."""
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "cache"))
    monkeypatch.delenv(tc.CLI_ENV, raising=False)
    _release(monkeypatch)
    real = os.replace

    def replace(src: Any, dst: Any) -> None:
        if Path(src).parent.parent != Path(dst).parent:  # another directory: another mount
            raise OSError(errno.EXDEV, "Invalid cross-device link", str(src), None, str(dst))
        real(src, dst)

    monkeypatch.setattr(tc.os, "replace", replace)
    monkeypatch.setattr(tempfile, "tempdir", str(tmp_path / "elsewhere"))
    (tmp_path / "elsewhere").mkdir()
    assert tc.ensure() == tc.cache_path()
    assert tc.server_version(tc.cache_path()) == (1, 32, 0)


@pytest.mark.skipif(not Path("/dev/shm").is_dir(), reason="no tmpfs at /dev/shm")
def test_the_download_works_with_tmpdir_on_tmpfs(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """The review's reproduction, for real: ``TMPDIR`` on tmpfs, the cache elsewhere."""
    if os.stat("/dev/shm").st_dev == os.stat(tmp_path).st_dev:
        pytest.skip("/dev/shm is on the cache's filesystem here")
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "cache"))
    monkeypatch.delenv(tc.CLI_ENV, raising=False)
    monkeypatch.setattr(tempfile, "tempdir", "/dev/shm")
    _release(monkeypatch)
    assert tc.ensure() == tc.cache_path()


def test_io_and_http_failures_are_temporal_cli_errors(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Callers (``kontra dev up``, ``fetch``, the dev-server tests) catch one error type, and
    nothing half-written is left in the cache."""
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "cache"))
    monkeypatch.delenv(tc.CLI_ENV, raising=False)
    _release(monkeypatch)

    def replace(src: Any, dst: Any) -> None:
        raise OSError(errno.EXDEV, "Invalid cross-device link")

    with monkeypatch.context() as m:
        m.setattr(tc.os, "replace", replace)
        with pytest.raises(tc.TemporalCLIError, match="Invalid cross-device link"):
            tc.ensure()
    assert list(tc.cache_path().parent.iterdir()) == []
    _release(monkeypatch, status=404)
    with pytest.raises(tc.TemporalCLIError, match="cannot download"):
        tc.ensure()
    assert list(tc.cache_path().parent.iterdir()) == []


# --------------------------------------------------------------------------- the way to the cache


def test_a_cache_below_a_directory_others_may_write_is_refused(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, no_download: list[Path]
) -> None:
    """Whoever may write a directory above the cache could rename the verified one after
    ``ensure`` and plant the binary the host then runs: refused, as ssh's StrictModes."""
    shared = tmp_path / "shared"
    shared.mkdir()
    _plant(shared / "cache")
    shared.chmod(0o777)
    monkeypatch.setenv("XDG_CACHE_HOME", str(shared / "cache"))
    with pytest.raises(tc.TemporalCLIError, match="writable by every user"):
        tc.ensure()
    assert no_download == []


def test_a_sticky_shared_directory_above_the_cache_is_fine(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, no_download: list[Path]
) -> None:
    """In a sticky directory (``/tmp``) only an entry's owner may rename it."""
    shared = tmp_path / "shared"
    shared.mkdir()
    target = _plant(shared / "cache")
    shared.chmod(0o1777)
    monkeypatch.setenv("XDG_CACHE_HOME", str(shared / "cache"))
    assert tc.ensure() == target
    assert no_download == []


def test_a_link_on_the_way_is_followed_and_checked(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, no_download: list[Path]
) -> None:
    """A link of ours to a cache below a directory others may write is that directory."""
    shared = tmp_path / "shared"
    shared.mkdir()
    _plant(shared / "cache")
    shared.chmod(0o777)
    link = tmp_path / "cache-link"
    link.symlink_to(shared / "cache")
    monkeypatch.setenv("XDG_CACHE_HOME", str(link))
    with pytest.raises(tc.TemporalCLIError, match="writable by every user"):
        tc.ensure()
    shared.chmod(0o755)
    assert tc.ensure() == link / "kontra-sdk" / f"temporal-cli-{tc.VERSION}" / "temporal"


@pytest.mark.skipif(not hasattr(os, "geteuid") or os.geteuid() != 0, reason="needs root to chown")
def test_a_cache_below_another_users_directory_is_refused(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, no_download: list[Path]
) -> None:
    """The review's reproduction: ``kontra-sdk/`` is another user's, the version directory
    below it ours. That user could rename ours away and plant theirs: refused."""
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path))
    target = _plant(tmp_path)
    os.chown(target.parent.parent, 65534, 65534)
    with pytest.raises(tc.TemporalCLIError, match="owned by uid 65534"):
        tc.ensure()
    assert no_download == []
