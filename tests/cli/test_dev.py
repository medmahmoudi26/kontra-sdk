"""``kontra dev`` command shapes, with no subprocess Temporal (decisions I34).

The real no-container lifecycle (acceptance 1) is ``tests/cli/test_dev_stack.py`` (marker
``temporal``); here each command's shape is checked with a stubbed host, plus the two hard
rules that need no process at all: ``up`` never punts to ``kontra login``, and ``kontra dev``
never reads ``KONTRA_DEV`` as a mode switch.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

import kontra.cli._dev as dev_module
from kontra.cli._dev import OBSERVABILITY_PROJECT, dev
from kontra.devgate import state
from kontra.profile import PROFILE_FILE

ROOT = Path(__file__).resolve().parents[2]


# --------------------------------------------------------------------------- the hard rules, static


def test_kontra_dev_never_says_kontra_login_or_reads_the_dev_mode() -> None:
    """``up`` never punts (decisions I34, §11): the dev CLI and the host never tell a user to
    ``kontra login`` or read ``KONTRA_DEV`` as a mode switch (``KONTRA_DEV_*_PORT`` port names
    are fine). ``gate.py`` is the gate's own implementation (the shared ``FakeGate``), whose API
    messages are the platform's behaviour, not the dev CLI's, so it is not in scope."""
    host = [p for p in (ROOT / "kontra" / "devgate").glob("*.py") if p.name != "gate.py"]
    files = [ROOT / "kontra" / "cli" / "_dev.py", *host]
    mode_switch = re.compile(r"KONTRA_DEV\b")  # not KONTRA_DEV_ (a port name)
    for path in files:
        text = path.read_text(encoding="utf-8")
        assert "kontra login" not in text, f"{path} names `kontra login`"
        assert not mode_switch.search(text), f"{path} reads KONTRA_DEV as a mode switch"


# --------------------------------------------------------------------------- up: refuse a real login


def _write_profile(home: Path, *, dev_profile: bool) -> None:
    home.mkdir(mode=0o700, parents=True, exist_ok=True)
    profile = {
        "gate_url": "https://api.kontra.run" if not dev_profile else "http://127.0.0.1:8080",
        "temporal_address": "api.kontra.run:443" if not dev_profile else "127.0.0.1:7233",
        "party": "alice",
        "namespace": "alice",
        "refresh_token": "r" * 40,
        "auth_time": "2026-10-20T00:00:00Z",
        "tls": not dev_profile,
        "dev": dev_profile,
    }
    import json

    (home / PROFILE_FILE).write_text(json.dumps(profile))
    (home / PROFILE_FILE).chmod(0o600)


def test_up_refuses_a_real_login_without_replace_profile(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    home = tmp_path / "home"
    _write_profile(home, dev_profile=False)
    monkeypatch.setenv("KONTRA_HOME", str(home))
    result = CliRunner().invoke(dev, ["up"])
    assert result.exit_code == 1
    assert "would replace" in result.output
    assert "--replace-profile" in result.output
    # It failed before anything was started: no stack recorded.
    assert state.read_stack_record(home) is None


def test_check_profile_passes_a_dev_profile_and_a_replace(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    home = tmp_path / "home"
    _write_profile(home, dev_profile=True)
    monkeypatch.setenv("KONTRA_HOME", str(home))
    assert dev_module._check_profile(False) is not None  # a dev profile: fine
    _write_profile(home, dev_profile=False)
    assert dev_module._check_profile(True) is not None  # --replace-profile: any profile


# --------------------------------------------------------------------------- observability


def test_observability_needs_docker(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KONTRA_HOME", str(tmp_path / "home"))
    monkeypatch.setattr(dev_module.shutil, "which", lambda _name: None)
    result = CliRunner().invoke(dev, ["up", "--observability"])
    assert result.exit_code == 1
    assert "needs Docker" in result.output


def test_the_observability_compose_is_run_as_its_own_project_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``--observability`` runs exactly ``docker compose -p kontra-dev-observability -f <packaged
    file> up -d --remove-orphans`` and nothing a user chooses: no ``--env-file``, no extra
    ``-f`` (§7)."""
    monkeypatch.setattr(dev_module.shutil, "which", lambda _name: "/usr/bin/docker")
    # The shape is what matters here, not the shipped file (packaged in a later PR).
    monkeypatch.setattr(
        dev_module, "observability_compose_file", lambda: Path("/x/observability/compose.yaml")
    )
    cmd = dev_module._observability_cmd("up", "-d", "--remove-orphans")
    assert cmd[:6] == [
        "/usr/bin/docker",
        "compose",
        "-p",
        OBSERVABILITY_PROJECT,
        "-f",
        "/x/observability/compose.yaml",
    ]
    assert cmd[6:] == ["up", "-d", "--remove-orphans"]
    assert "--env-file" not in cmd
    env = dev_module._observability_env(state.Ports())
    # Only the port variables beyond our own environment (no gate, no image).
    assert env[state.PORT_ENV["prometheus"]] == "9090"
    assert env[state.PORT_ENV["otlp"]] == "4317"


def test_down_touches_docker_only_after_an_observability_up(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A stack that never ran ``--observability`` never calls docker (acceptance 1's docker
    shim fails the test if it is called, OD11); once one did, ``down`` takes the observability
    project down with ``-p``/``-f`` only, and forgets it."""
    home = tmp_path / "home"
    monkeypatch.setenv("KONTRA_HOME", str(home))
    monkeypatch.setattr(dev_module.shutil, "which", lambda _name: "/usr/bin/docker")
    monkeypatch.setattr(
        dev_module, "_observability_file", lambda: Path("/x/observability/compose.yaml")
    )
    calls: list[list[str]] = []

    def fake_run(cmd: list[str], **_: Any) -> Any:
        calls.append(cmd)
        return type("P", (), {"returncode": 0})()

    monkeypatch.setattr(dev_module.subprocess, "run", fake_run)
    # Never used --observability: nothing calls docker.
    result = CliRunner().invoke(dev, ["down"])
    assert result.exit_code == 0, result.output
    assert calls == []
    # A previous `up --observability` is remembered in the settings: down takes it down.
    settings = state.read_settings(home)
    settings.observability = True
    state.write_settings(settings, home)
    result = CliRunner().invoke(dev, ["down"])
    assert result.exit_code == 0, result.output
    assert any(
        c[:4] == ["/usr/bin/docker", "compose", "-p", OBSERVABILITY_PROJECT] and "down" in c
        for c in calls
    )
    assert all("--env-file" not in c for c in calls)
    assert state.read_settings(home).observability is False  # forgotten once it is down
