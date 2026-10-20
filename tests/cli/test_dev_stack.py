"""Acceptance 1 with no containers (decisions I34, OD11): ``kontra init`` through a hired job,
on the in-process dev gate, with a ``docker`` on ``PATH`` that fails the test if it is called.

This is the primary proof that ``kontra dev up`` needs no Docker; the e2e suite keeps the
prod-gate half. Marked ``temporal``: it runs the real Temporal CLI dev server as a subprocess.
"""

from __future__ import annotations

import json
import os
import shutil
import signal
import socket
import stat
import subprocess
import sys
import time
from collections.abc import Mapping
from pathlib import Path
from typing import Any

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
    # The per-user registry of running stacks beside the homes, never the real one.
    state_home = str(home.parent / "xdg-state")
    return {
        **os.environ,
        "KONTRA_HOME": str(home),
        "KONTRA_TEMPORAL_CLI": cli,
        "XDG_STATE_HOME": state_home,
    }


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


# --------------------------------------------------------------------------- T4, T6, T5


def _needs_a_cli() -> None:
    if not KONTRA.exists():
        pytest.skip("the kontra console script is not installed in this venv")
    cli = _env(Path("/nonexistent/home"))["KONTRA_TEMPORAL_CLI"]
    if not Path(cli).is_file():
        pytest.skip("no Temporal CLI on this machine")


def _port_flags(ports: Mapping[str, int]) -> list[str]:
    flat: list[str] = []
    for name, port in ports.items():
        flat += [f"--{name}-port", str(port)]
    return flat


def _status(env: Mapping[str, str]) -> dict[str, Any]:
    done = _run(["dev", "status", "--json"], env)
    assert done.returncode == 0, done.stdout + done.stderr
    doc: dict[str, Any] = json.loads(done.stdout)
    return doc


def _alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


def _gone(pid: int, timeout: float = 20) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not _alive(pid):
            return True
        time.sleep(0.1)
    return False


def test_a_running_stack_is_reconciled_joined_and_persisted(tmp_path: Path) -> None:
    """T4: a second ``up`` keeps the host; ``--ui`` on and off restarts only the Temporal
    child; other ports ask for ``down``. T6 (OD7): a second home of the same user joins the
    running stack, logs in to it, and is told to run ``down`` in the owner's home. And
    ``--persist`` keeps the gate across ``down`` and ``up``."""
    _needs_a_cli()
    owner, guest = tmp_path / "owner", tmp_path / "guest"
    env, guest_env = _env(owner), _env(guest)
    ports = {k: _free() for k in ("gate", "temporal", "ui", "codec")}
    try:
        up = _run(["dev", "up", "--handle", "alice", *_port_flags(ports)], env)
        assert up.returncode == 0, up.stdout + up.stderr
        first = _status(env)
        host, temporal = first["pid"], first["temporal_pid"]
        assert first["running"] is True
        assert first["ui"] is False

        again = _run(["dev", "up"], env)  # the remembered ports: the same stack
        assert again.returncode == 0, again.stdout + again.stderr
        same = _status(env)
        assert (same["pid"], same["temporal_pid"]) == (host, temporal)

        with_ui = _run(["dev", "up", "--ui"], env)
        assert with_ui.returncode == 0, with_ui.stdout + with_ui.stderr
        on = _status(env)
        assert on["pid"] == host  # the gate (and its in-memory state) kept
        assert on["temporal_pid"] != temporal  # only Temporal restarted, with its UI
        assert on["ui"] is True
        assert not _port_closed(ports["ui"])
        off = _run(["dev", "up"], env)
        assert off.returncode == 0, off.stdout + off.stderr
        now = _status(env)
        assert (now["pid"], now["ui"]) == (host, False)
        assert now["temporal_pid"] not in (temporal, on["temporal_pid"])

        moved = _run(["dev", "up", "--gate-port", str(_free())], env)
        assert moved.returncode == 1
        assert "`kontra dev down` first" in moved.stdout + moved.stderr

        # A second home of this user joins the stack on that gate port, as its own party.
        joined = _run(
            ["dev", "up", "--handle", "bob", "--gate-port", str(ports["gate"])], guest_env
        )
        assert joined.returncode == 0, joined.stdout + joined.stderr
        assert "joining the dev stack" in joined.stderr
        profile = json.loads((guest / "profile").read_text())
        assert (profile["party"], profile["dev"]) == ("bob", True)
        assert profile["gate_url"] == f"http://127.0.0.1:{ports['gate']}"
        balance = _run(["balance"], guest_env)  # its profile works against the stack
        assert balance.returncode == 0, balance.stdout + balance.stderr
        assert "100.00" in balance.stdout
        rejoined = _run(["dev", "up"], guest_env)  # the port is remembered: it joins again
        assert rejoined.returncode == 0, rejoined.stdout + rejoined.stderr
        seen = _status(guest_env)
        assert (seen["joined"], seen["running"], seen["pid"]) == (True, True, host)
        refused = _run(["dev", "down"], guest_env)
        assert refused.returncode == 1
        assert f"KONTRA_HOME={owner}" in refused.stdout + refused.stderr
        assert _status(env)["pid"] == host  # the guest's down stopped nothing

        # --persist on a running stack snapshots at once; down keeps it; up restores it.
        kept = _run(["dev", "up", "--persist"], env)
        assert kept.returncode == 0, kept.stdout + kept.stderr
        snapshot = owner / "dev" / "data" / "gate.json"
        assert stat.S_IMODE(snapshot.stat().st_mode) == 0o600
        counts = _status(env)["counts"]
        assert counts["parties"] == 2  # alice, and bob who joined
        assert _run(["dev", "down"], env).returncode == 0
        assert _gone(host)
        assert snapshot.exists()
        back = _run(["dev", "up"], env)  # --persist is remembered
        assert back.returncode == 0, back.stdout + back.stderr
        restored = _status(env)
        assert restored["counts"]["parties"] == 2
        assert restored["persist"] is True
        assert _run(["balance"], guest_env).returncode == 0  # bob's tokens still good
    finally:
        _run(["dev", "down", "--reset"], env)
        _run(["dev", "down"], guest_env)
    assert not (owner / "dev" / "data").exists()
    assert _port_closed(ports["gate"])
    assert _port_closed(ports["temporal"])
    assert not (guest / "dev" / "joined.json").exists()  # the guest forgot the stack


def test_a_killed_host_is_cleaned_up(tmp_path: Path) -> None:
    """T5: SIGKILL the host; its Temporal child lives on in the group the host led. ``status``
    says stale; the next ``up`` stops that orphan (its command line names this home's db)
    and starts a fresh stack."""
    _needs_a_cli()
    home = tmp_path / "home"
    env = _env(home)
    ports = {k: _free() for k in ("gate", "temporal")}
    try:
        up = _run(["dev", "up", "--handle", "carol", *_port_flags(ports)], env)
        assert up.returncode == 0, up.stdout + up.stderr
        first = _status(env)
        host, temporal = first["pid"], first["temporal_pid"]
        os.kill(host, signal.SIGKILL)
        assert _gone(host)
        assert _alive(temporal)  # orphaned, still holding the Temporal port
        stale = _status(env)
        assert (stale["running"], stale["stale"]) == (False, True)
        again = _run(["dev", "up"], env)
        assert again.returncode == 0, again.stdout + again.stderr
        assert _gone(temporal, 10)
        fresh = _status(env)
        assert fresh["running"] is True
        assert fresh["pid"] not in (host, None)
    finally:
        down = _run(["dev", "down"], env)
    assert down.returncode == 0, down.stdout + down.stderr
    assert _port_closed(ports["gate"])
    assert _port_closed(ports["temporal"])
