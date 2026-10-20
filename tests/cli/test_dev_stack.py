"""Acceptance 1 with no containers (decisions I34, OD11): ``kontra init`` through a hired job,
on the in-process dev gate, with a ``docker`` on ``PATH`` that fails the test if it is called.

This is the primary proof that ``kontra dev up`` needs no Docker; the e2e suite keeps the
prod-gate half. Marked ``temporal``: it runs the real Temporal CLI dev server as a subprocess.
"""

from __future__ import annotations

import json
import os
import shutil
import socket
import stat
import subprocess
import sys
import time
from collections.abc import Mapping
from pathlib import Path

import pytest

pytestmark = pytest.mark.temporal

ROOT = Path(__file__).resolve().parents[2]
KONTRA = Path(sys.executable).parent / "kontra"


def _free() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def _port_closed(port: int) -> bool:
    with socket.socket() as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            s.bind(("127.0.0.1", port))
            return True
        except OSError:
            return False


@pytest.fixture
def fake_docker(tmp_path: Path) -> Path:
    """A ``docker`` first on ``PATH`` that records any call and exits 1."""
    bin_dir = tmp_path / "fakebin"
    bin_dir.mkdir()
    marker = tmp_path / "docker-was-called"
    script = bin_dir / "docker"
    script.write_text(f'#!/bin/sh\necho "$@" >> "{marker}"\nexit 1\n')
    script.chmod(0o755)
    return marker


def _env(home: Path) -> dict[str, str]:
    cli = os.environ.get("KONTRA_TEMPORAL_CLI") or str(
        Path.home() / ".cache" / "kontra-sdk" / "temporal-cli-1.9.1" / "temporal"
    )
    return {**os.environ, "KONTRA_HOME": str(home), "KONTRA_TEMPORAL_CLI": cli}


def _run(
    args: list[str], env: Mapping[str, str], *, timeout: float = 180
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [str(KONTRA), *args],
        env=dict(env),
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )


def test_ac01_init_dev_up_serve_hire_with_no_containers(
    tmp_path: Path, fake_docker: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    if not KONTRA.exists():
        pytest.skip("the kontra console script is not installed in this venv")
    cli = os.environ.get("KONTRA_TEMPORAL_CLI") or str(
        Path.home() / ".cache" / "kontra-sdk" / "temporal-cli-1.9.1" / "temporal"
    )
    if not Path(cli).is_file():
        pytest.skip("no Temporal CLI on this machine")

    home = tmp_path / "home"
    project = tmp_path / "hello"
    shutil.copytree(
        ROOT / "examples" / "hello", project, ignore=shutil.ignore_patterns("__pycache__")
    )
    monkeypatch.setenv("PATH", f"{fake_docker.parent}{os.pathsep}{os.environ['PATH']}")
    env = _env(home)
    ports = {k: _free() for k in ("gate", "temporal", "ui", "codec")}
    port_args = [f"--{k}-port" for k in ports]
    flat: list[str] = []
    for flag, (_k, v) in zip(port_args, ports.items(), strict=True):
        flat += [flag, str(v)]

    try:
        up = _run(["dev", "up", "--handle", "acme", *flat], env)
        assert up.returncode == 0, up.stdout + up.stderr
        assert "logged in as acme" in up.stdout
        assert "no containers" in up.stdout
        profile = json.loads((home / "profile").read_text())
        assert profile["dev"] is True
        assert stat.S_IMODE((home / "dev" / "stack.json").stat().st_mode) == 0o600

        status = _run(["dev", "status", "--json"], env)
        assert status.returncode == 0
        assert json.loads(status.stdout)["running"] is True

        serve = subprocess.Popen(
            [str(KONTRA), "serve", "--dir", str(project)],
            env=dict(env),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        try:
            published = False
            for _ in range(30):
                if (
                    _run(
                        ["quote", "acme/hello", "-f", str(project / "input.json")], env, timeout=30
                    ).returncode
                    == 0
                ):
                    published = True
                    break
                time.sleep(1)
            assert published, "kontra serve did not publish acme/hello"
            hire = _run(
                [
                    "hire",
                    "acme/hello",
                    "-f",
                    str(project / "input.json"),
                    "--max-price",
                    "1",
                    "--accept",
                    "--json",
                ],
                env,
            )
            assert hire.returncode == 0, hire.stdout + hire.stderr
            assert (
                '"state": "completed"' in hire.stdout.replace("\n", " ")
                or '"state":"completed"' in hire.stdout
            )
        finally:
            serve.terminate()
            serve.wait(timeout=30)

        logs = _run(["dev", "logs", "gate", "--tail", "5"], env)
        assert logs.returncode == 0
    finally:
        down = _run(["dev", "down"], env)
    assert down.returncode == 0, down.stdout + down.stderr
    assert not (home / "dev" / "stack.json").exists()
    assert not (home / "dev" / "data").exists()
    assert _port_closed(ports["gate"])
    assert _port_closed(ports["temporal"])
    # Acceptance 1 is proved with no containers: the fake docker was never called.
    assert not fake_docker.exists(), fake_docker.read_text() if fake_docker.exists() else ""
