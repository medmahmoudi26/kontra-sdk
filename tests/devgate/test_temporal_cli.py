"""``kontra.devgate.temporal_cli``: the pins match the Makefile, and extraction is safe."""

from __future__ import annotations

import re
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
