"""``kontra dev`` command shapes, with no subprocess Temporal (decisions I34).

The real no-container lifecycle (acceptance 1) is ``tests/cli/test_dev_stack.py`` (marker
``temporal``); here each command's shape is checked with a stubbed host or none, plus the
rules that need no stack at all: ``up`` never punts to ``kontra login`` and never reads
``KONTRA_DEV`` as a mode switch, needs the ``[dev]`` extra (and ``down``/``status``/``logs`` do
not), names a taken port, and never signals a process group that is not provably its own.
The compose file's structural tests that lived in the old ``tests/cli/test_dev.py`` are
``tests/stack/test_compose.py``.
"""

from __future__ import annotations

import dataclasses
import http.server
import json
import os
import re
import shutil
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any, ClassVar

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


def _no_docker(_name: str) -> str | None:
    return None


def _docker_here(_name: str) -> str | None:
    return "/usr/bin/docker"


def test_observability_needs_docker(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KONTRA_HOME", str(tmp_path / "home"))
    monkeypatch.setattr(dev_module.shutil, "which", _no_docker)
    result = CliRunner().invoke(dev, ["up", "--observability"])
    assert result.exit_code == 1
    assert "needs Docker" in result.output


def test_the_observability_compose_is_run_as_its_own_project_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``--observability`` runs exactly ``docker compose -p kontra-dev-observability -f <packaged
    file> up -d --remove-orphans`` and nothing a user chooses: no ``--env-file``, no extra
    ``-f`` (§7)."""
    monkeypatch.setattr(dev_module.shutil, "which", _docker_here)
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
    monkeypatch.setattr(dev_module.shutil, "which", _docker_here)
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


# --------------------------------------------------------------------------- up never punts (T2)


@pytest.fixture
def dev_home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """A fresh ``KONTRA_HOME`` and a private per-user registry (``XDG_STATE_HOME``)."""
    home = tmp_path / "home"
    monkeypatch.setenv("KONTRA_HOME", str(home))
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "state"))
    for name in (*state.PORT_ENV.values(), "KONTRA_DEV"):
        monkeypatch.delenv(name, raising=False)
    return home


class _StubHost(http.server.BaseHTTPRequestHandler):
    """What answers on a recorded stack's port: the control routes with its token, and a
    ``/v1/info`` that says ``dev: false`` -- as a production-mode gate would."""

    token = ""
    seen: ClassVar[list[str]] = []

    def _answer(self, status: int, doc: dict[str, Any]) -> None:
        body = json.dumps(doc).encode()
        self.send_response(status)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _authorized(self) -> bool:
        return self.headers.get("authorization") == f"Bearer {self.token}"

    def do_GET(self) -> None:
        self.seen.append(f"GET {self.path}")
        if self.path == "/_devgate/ready" and self._authorized():
            self._answer(200, {"pid": 1, "ui": False, "metrics": False, "persist": False})
        elif self.path == "/v1/info":
            self._answer(200, {"dev": False, "name": "a production-mode gate"})
        else:
            self._answer(403, {})

    def do_POST(self) -> None:
        self.seen.append(f"POST {self.path}")
        length = int(self.headers.get("content-length") or 0)
        self.rfile.read(length)
        self._answer(200 if self._authorized() else 403, {"restarted": False})

    def log_message(self, format: str, *args: Any) -> None:
        pass


def _record(home: Path, gate: int, *, pid: int, pgid: int, token: str, cli: str = "") -> None:
    ports = dataclasses.asdict(state.Ports(gate=gate))
    state.write_stack_record(
        state.StackRecord(
            pid=pid, pgid=pgid, token=token, started_at="2026-10-20T00:00:00Z",
            sdk_version="0.3.0", cli={"path": cli or "/x/temporal", "version": "1.9.1"},
            ports=ports, temporal_http=0,
        ),
        home,
    )  # fmt: skip


def test_up_never_punts_when_the_gate_is_not_a_dev_gate(dev_home: Path) -> None:
    """T2: whatever answers on the stack's port, if ``/v1/info`` says ``dev: false``, ``up``
    exits 1, writes no profile, and names neither ``kontra login`` nor ``KONTRA_DEV`` (the
    punt of kontra-sdk <= 0.2 exited 0 with that advice, decisions I34 §11)."""
    token = "stub-token-" + "y" * 20
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _StubHost)
    _StubHost.token = token
    _StubHost.seen = []
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        _record(dev_home, server.server_address[1], pid=2**22 - 5, pgid=2**22 - 5, token=token)
        result = CliRunner().invoke(dev, ["up"])
    finally:
        server.shutdown()
        server.server_close()
    assert result.exit_code == 1, result.output
    assert "is not this home's dev gate" in result.output
    assert "kontra login" not in result.output
    assert not re.search(r"KONTRA_DEV\b", result.output)
    assert not (dev_home / PROFILE_FILE).exists()
    assert "GET /v1/info" in _StubHost.seen  # it asked, and believed the answer


# --------------------------------------------------------------------------- the [dev] extra (T8)

#: Runs the CLI with FastAPI, Starlette and uvicorn unimportable (no ``[dev]`` extra).
_WITHOUT_THE_EXTRA = """
import sys
class _NoDevExtra:
    def find_spec(self, name, path=None, target=None):
        if name.split(".")[0] in ("fastapi", "starlette", "uvicorn"):
            raise ImportError(f"No module named {name!r} (no [dev] extra)")
        return None
sys.meta_path.insert(0, _NoDevExtra())
sys.argv = ["kontra", *sys.argv[1:]]
from kontra.cli import main
main()
"""


def _without_the_extra(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-c", _WITHOUT_THE_EXTRA, *args],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )


def test_up_without_the_dev_extra_stops_at_once_with_the_install_line(dev_home: Path) -> None:
    """OD3: without FastAPI and uvicorn ``up`` exits 1 before anything else -- even before it
    would refuse a real login -- with the line that installs them; ``status``, ``down`` and
    ``logs`` never import them and work."""
    _write_profile(dev_home, dev_profile=False)  # `up` would refuse this: the extra comes first
    up = _without_the_extra("dev", "up")
    assert up.returncode == 1, up.stdout + up.stderr
    assert 'pip install "kontra-sdk[dev]"' in up.stderr
    assert "would replace" not in up.stderr
    assert "Traceback" not in up.stderr
    assert state.read_stack_record(dev_home) is None
    for args in (("dev", "status"), ("dev", "status", "--json"), ("dev", "logs"), ("dev", "down")):
        done = _without_the_extra(*args)
        assert done.returncode == 0, (args, done.stdout + done.stderr)
        assert "Traceback" not in done.stderr


# --------------------------------------------------------------------------- stale records (T5)


def _fake_temporal(tmp_path: Path, cli: str, db: Path) -> subprocess.Popen[bytes]:
    """A process whose command line is exactly a Temporal child's (``<cli> server start-dev
    ... --db-filename <db>``), leading a group of its own as the host's group would: a Python
    that runs the package ``./server`` and sleeps."""
    pkg = tmp_path / "fake" / "server"
    pkg.mkdir(parents=True, exist_ok=True)
    (pkg / "__main__.py").write_text("import time\ntime.sleep(120)\n")
    proc = subprocess.Popen(
        [cli, "server", "start-dev", "--port", "1", "--db-filename", str(db)],
        executable=sys.executable,
        cwd=pkg.parent,
        start_new_session=True,
    )
    threading.Thread(target=proc.wait, daemon=True).start()  # reaped as soon as it exits
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:  # until its command line is the one asked for
        try:
            if Path(f"/proc/{proc.pid}/cmdline").read_bytes().startswith(cli.encode()):
                break
        except OSError:
            pass
        time.sleep(0.05)
    return proc


def _unreachable_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


@pytest.mark.skipif(not Path("/proc/self/cmdline").exists(), reason="reads /proc")
def test_a_stale_record_never_signals_a_process_that_is_not_its_own(
    dev_home: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A recorded pid outlives its process: after a reboot it may be anyone's, a group leader
    included. ``down`` (and a stale ``up``) signal the group only when a member's command line
    names this home; a stranger, another home's Temporal, and this command's own group are
    never signalled (T5)."""
    signalled: list[tuple[int, int]] = []

    def killpg(pgid: int, sig: int) -> None:
        signalled.append((pgid, sig))

    monkeypatch.setattr(dev_module.os, "killpg", killpg)
    cli = str(tmp_path / "bin" / "temporal")
    stranger = subprocess.Popen(["sleep", "120"], start_new_session=True)
    threading.Thread(target=stranger.wait, daemon=True).start()
    other_home = _fake_temporal(tmp_path, cli, tmp_path / "other" / "dev" / "data" / "temporal.db")
    try:
        for pid, pgid in (
            (stranger.pid, stranger.pid),  # a recycled pid: a group leader of someone else's
            (other_home.pid, other_home.pid),  # a Temporal CLI, but on another home's db
            (os.getpgrp(), os.getpgrp()),  # this very command's group
            (stranger.pid, os.getpgrp()),  # a forged pgid
        ):
            _record(dev_home, _unreachable_port(), pid=pid, pgid=pgid, token="t" * 20, cli=cli)
            result = CliRunner().invoke(dev, ["down"])
            assert result.exit_code == 0, result.output
            assert state.read_stack_record(dev_home) is None  # forgotten either way
        assert signalled == []
        assert stranger.poll() is None
        assert other_home.poll() is None
    finally:
        for proc in (stranger, other_home):
            proc.kill()


@pytest.mark.skipif(not Path("/proc/self/cmdline").exists(), reason="reads /proc")
def test_a_stale_record_of_this_home_has_its_orphans_stopped(
    dev_home: Path, tmp_path: Path
) -> None:
    """The positive half: the host was killed, its Temporal child lives on in the group it
    led; that child's command line names this home's db, so ``down`` stops the group (T5)."""
    cli = str(tmp_path / "bin" / "temporal")
    orphan = _fake_temporal(tmp_path, cli, dev_home / "dev" / "data" / "temporal.db")
    try:
        _record(dev_home, _unreachable_port(), pid=orphan.pid, pgid=orphan.pid, token="t" * 20, cli=cli)  # fmt: skip
        result = CliRunner().invoke(dev, ["down"])
        assert result.exit_code == 0, result.output
        deadline = time.monotonic() + 10
        while orphan.poll() is None and time.monotonic() < deadline:
            time.sleep(0.05)
        assert orphan.poll() is not None
    finally:
        orphan.kill()


def test_serves_home_matches_only_this_homes_host_and_temporal(tmp_path: Path) -> None:
    home = tmp_path / "home"
    db = str(home / "dev" / "data" / "temporal.db")
    cli = "/c/temporal"
    ours = [
        ["/usr/bin/python3", "-m", "kontra.devgate", "--home", str(home)],
        [cli, "server", "start-dev", "--ip", "127.0.0.1", "--db-filename", db, "--headless"],
    ]
    theirs = [
        ["/usr/bin/python3", "-m", "kontra.devgate", "--home", str(tmp_path / "other")],
        ["/usr/bin/python3", "-m", "kontra.devgate"],  # no label: not provably ours
        [cli, "server", "start-dev", "--db-filename", db + ".other"],
        ["/other/temporal", "server", "start-dev", "--db-filename", db],
        [cli, "--db-filename", db],
        ["sleep", "120", str(home), db],
        [],
    ]
    assert all(dev_module._serves_home(argv, home, cli) for argv in ours)
    assert not any(dev_module._serves_home(argv, home, cli) for argv in theirs)
    assert not dev_module._serves_home(ours[1], home, "")  # no recorded CLI: only the host


# --------------------------------------------------------------------------- ports taken (T6)


def test_a_taken_port_is_named_with_its_holder_and_flag(dev_home: Path) -> None:
    with socket.socket() as held:
        held.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        held.bind(("127.0.0.1", 0))
        held.listen()
        port = held.getsockname()[1]
        result = CliRunner().invoke(
            dev, ["up", "--gate-port", str(port), "--temporal-port", str(_unreachable_port())]
        )
    assert result.exit_code == 1, result.output
    assert f"127.0.0.1:{port} is taken" in result.output
    assert "--gate-port" in result.output
    if shutil.which("ss") is not None:
        assert f"pid {os.getpid()}" in result.output  # who holds it, when it can be told
    assert state.read_stack_record(dev_home) is None
    assert "kontra login" not in result.output


def test_up_binds_ui_ports_only_when_asked(dev_home: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A plain ``up`` needs only the gate and Temporal ports: a taken UI or codec port (the
    defaults 8233 and 8081 are popular) matters only with ``--ui``."""
    checked: list[tuple[str, ...]] = []

    def check(_ports: state.Ports, fields: tuple[str, ...]) -> None:
        checked.append(fields)
        raise dev_module.CliError("stop here")

    monkeypatch.setattr(dev_module, "_check_free", check)
    CliRunner().invoke(dev, ["up"])
    CliRunner().invoke(dev, ["up", "--ui"])
    assert checked == [("gate", "temporal"), ("gate", "temporal", "ui", "codec")]
