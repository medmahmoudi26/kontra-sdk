"""``kontra.devgate.temporal_cli``: the pins match the Makefile, and extraction is safe."""

from __future__ import annotations

import json
import os
import re
import stat
import tarfile
from pathlib import Path

import pytest

from kontra.devgate import temporal_cli as tc

ROOT = Path(__file__).resolve().parents[2]


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
