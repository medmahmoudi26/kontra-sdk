"""The temporalio range kontra-sdk is tested on (decisions I29, kontra._compat): one constant
pair, equal to ``pyproject.toml``'s specifiers; asserted when a worker starts
(``KontraPlugin.prepare``), warned on by ``kontra serve`` and ``kontra publish`` before they
import ``worker.py``."""

from __future__ import annotations

import logging
import tomllib
from pathlib import Path

import pytest
import temporalio
from click.testing import CliRunner

from kontra import _compat
from kontra.cli import cli as kontra_cli
from kontra.plugin import KontraPlugin
from kontra.serve import ServeError

from . import seller
from .support import write_offering

PYPROJECT = Path(__file__).resolve().parents[2] / "pyproject.toml"


def test_the_range_is_pyprojects() -> None:
    project = tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))["project"]
    spec = _compat.temporalio_range()
    assert spec == ">=1.34,<1.38"
    assert f"temporalio{spec}" in project["dependencies"]
    assert f"temporalio[opentelemetry]{spec}" in project["optional-dependencies"]["opentelemetry"]
    assert "agents" not in project["optional-dependencies"]  # the seller's dependency now


def test_the_installed_temporalio_is_in_range() -> None:
    assert _compat.temporalio_warning() is None
    _compat.check_temporalio()


@pytest.mark.parametrize(
    ("version", "inside"),
    [
        ("1.34.0", True),
        ("1.37.12", True),
        ("1.35.0rc1", True),
        ("1.33.9", False),
        ("1.38.0", False),
        ("2.0.0", False),
        ("not a version", False),
        ("1.34.0\n", False),
    ],
)
def test_outside_the_range_warns_and_refuses(
    monkeypatch: pytest.MonkeyPatch, version: str, inside: bool
) -> None:
    monkeypatch.setattr(temporalio, "__version__", version)
    line = _compat.temporalio_warning()
    if inside:
        assert line is None
        _compat.check_temporalio()
        return
    assert line is not None
    assert "outside the range kontra-sdk is tested on (temporalio>=1.34,<1.38)" in line
    with pytest.raises(ServeError, match="refusing to start: temporalio"):
        _compat.check_temporalio()


async def test_a_worker_refuses_to_start_outside_it(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """At startup, before anything else (no profile is even read here), not when worker.py
    is imported: ``kontra publish`` imports it and only warns."""
    monkeypatch.setattr(temporalio, "__version__", "1.40.0")
    plugin = KontraPlugin(seller.bind(write_offering(tmp_path)), home=tmp_path / "nobody")
    with pytest.raises(ServeError, match=r"temporalio 1\.40\.0 is outside the range"):
        await plugin.prepare()
    assert not plugin.prepared


def test_serve_and_publish_warn_before_loading_worker_py(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Both say it first: a worker.py that fails to import on another temporalio is
    explained by the line before its traceback."""
    monkeypatch.setattr(temporalio, "__version__", "1.33.0")
    env = {"KONTRA_HOME": str(tmp_path / "home"), "KONTRA_API_KEY": None, "KONTRA_GATE": None}
    empty = tmp_path / "empty"
    empty.mkdir()
    published = CliRunner().invoke(kontra_cli, ["publish", "--dir", str(empty)], env=env)
    assert published.exit_code == 1
    first, second = published.stderr.splitlines()[:2]
    assert first.startswith("warning: temporalio 1.33.0 is outside the range")
    assert "no worker.py" in second
    caplog.set_level(logging.WARNING, "kontra.serve")
    served = CliRunner().invoke(kontra_cli, ["serve", "--dir", str(empty)], env=env)
    assert served.exit_code == 1
    assert "no worker.py" in served.stderr
    [warned] = [r for r in caplog.records if "outside the range" in r.getMessage()]
    assert warned.levelno == logging.WARNING
    assert warned.getMessage().startswith("temporalio 1.33.0 is outside the range")
