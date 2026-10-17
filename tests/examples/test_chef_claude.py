"""``examples/chef-claude``: an agent that is a process, driven by one heartbeating activity,
with a stand-in ``claude`` first on PATH.

The acceptance of kontra-sdk#27, end to end: its own ``worker.py`` serving as ``acme``, the
worker killed mid-run (SIGKILL: no graceful shutdown, the activity mid-flight) and another
started; the binary dies with the first worker, the retry passes ``--resume`` with the same
session id -- ``uuid5`` of the job's workflow id -- and the job completes.

And ``cook`` by itself (temporalio's ``ActivityEnvironment``, in a process of its own), for
what one attempt does wherever it lands: a retry on a machine that never saw the session
starts it there, an attempt 1 that finds it (a reset workflow) resumes it, the buyer's words
reach the binary on stdin only, the binary runs in a directory no one else can write, and a
binary that falls silent is stopped.

The stand-in answers as ``claude`` 2.1 does where the example depends on it: it reads the
prompt from stdin, prints ``system``/``init`` when it takes the session, refuses
``--session-id`` of a session it holds (``already in use``, nothing on stdout) and
``--resume`` of one it does not (a ``result`` event, ``error_during_execution`` with no
turn). What a real ``claude`` cooks is not tested here."""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
import uuid
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
from temporalio.client import WorkflowHandle

from kontra.contract.names import job_workflow_id
from kontra.gate_client import models as m

from .support import EXAMPLES, Market, Seller, seller_env, until

OFFERING = "acme/chef-claude"
CRAVING = "something quick with eggs"
CHEF = EXAMPLES / "chef-claude"

#: ``claude -p --output-format stream-json ...``, as far as the example uses it.
STAND_IN = """#!{python}
import json, os, sys, time
from pathlib import Path

args = sys.argv[1:]
said = sys.stdin.read()
with open(os.environ["STAND_IN_ARGV"], "a") as log:
    run = {{"argv": args, "stdin": said, "pid": os.getpid(), "cwd": os.getcwd()}}
    log.write(json.dumps(run) + "\\n")


def opt(name):
    return args[args.index(name) + 1] if name in args else None


def say(**event):
    print(json.dumps(event), flush=True)


sessions = Path(os.environ["STAND_IN_SESSIONS"])
cook = os.environ.get("STAND_IN_COOK", "until-killed")
if opt("--session-id"):
    session = sessions / opt("--session-id")
    if session.exists():
        print(f"Error: Session ID {{session.name}} is already in use.", file=sys.stderr)
        sys.exit(1)
    session.write_text(said)
    say(type="system", subtype="init", session_id=session.name)
    if cook == "until-killed":  # cooking: a line now and then
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            say(type="assistant", session_id=session.name, message={{"content": []}})
            time.sleep(0.5)
        sys.exit(3)
    if cook == "silent":  # stuck
        time.sleep(120)
        sys.exit(3)
    craving = said.rsplit("Craving: ", 1)[-1]
    say(type="result", subtype="success", is_error=False, num_turns=1, session_id=session.name,
        result=f"Cooked: an omelette, for {{craving}}")
    sys.exit(0)
session = sessions / opt("--resume")
if not session.exists():
    print(f"No conversation found with session ID: {{session.name}}", file=sys.stderr)
    say(type="result", subtype="error_during_execution", is_error=True, num_turns=0,
        errors=[f"No conversation found with session ID: {{session.name}}"])
    sys.exit(1)
say(type="system", subtype="init", session_id=session.name)
craving = session.read_text().rsplit("Craving: ", 1)[-1]
say(type="result", subtype="success", is_error=False, num_turns=1, session_id=session.name,
    result=f"Resumed: an omelette, for {{craving}}")
"""


class Machine:
    """A worker's machine: a home, the stand-in ``claude`` first on PATH, and the sessions it
    keeps there. ``runs`` is every time it was started."""

    def __init__(self, root: Path, base: dict[str, str]) -> None:
        self.home = root / "home"
        self.sessions = root / "sessions"
        self.argv = root / "argv.jsonl"
        bin_dir = root / "bin"
        for d in (self.home, self.sessions, bin_dir):
            d.mkdir(parents=True)
        claude = bin_dir / "claude"
        claude.write_text(STAND_IN.format(python=sys.executable))
        claude.chmod(0o755)
        self.env = {
            **base,
            "HOME": str(self.home),
            "PATH": f"{bin_dir}{os.pathsep}{base.get('PATH', '')}",
            "STAND_IN_ARGV": str(self.argv),
            "STAND_IN_SESSIONS": str(self.sessions),
        }

    @property
    def kitchen(self) -> Path:
        return self.home / ".kontra-chef-claude"

    def holds(self, workflow_id: str, craving: str = CRAVING) -> str:
        """The session of ``workflow_id`` on this machine already, as an earlier run left it."""
        session = str(uuid.uuid5(uuid.NAMESPACE_URL, workflow_id))
        (self.sessions / session).write_text(f"...\n\nCraving: {craving}")
        return session

    def runs(self) -> list[dict[str, Any]]:
        if not self.argv.exists():
            return []
        return [json.loads(line) for line in self.argv.read_text().splitlines()]


def _flag(run: dict[str, Any], name: str) -> str | None:
    argv: list[str] = run["argv"]
    return argv[argv.index(name) + 1] if name in argv else None


def _alive(pid: int) -> bool:
    """Running, as opposed to gone or a zombie nobody reaped yet."""
    try:
        stat = Path(f"/proc/{pid}/stat").read_text()
    except OSError:
        return False
    return stat.rsplit(")", 1)[1].split()[0] != "Z"


# --------------------------------------------------------------------------- one attempt

#: Runs ``activities.cook`` once in temporalio's ``ActivityEnvironment``: argv[1] says which
#: attempt of which workflow, and what to set on the module first.
DRIVER = """
import asyncio, dataclasses, json, sys

from temporalio.testing import ActivityEnvironment

import activities
from models import CookInput

spec = json.loads(sys.argv[1])
for name, value in spec.get("set", {}).items():
    setattr(activities, name, value)


async def main():
    env = ActivityEnvironment()
    env.info = dataclasses.replace(
        env.info,
        attempt=spec["attempt"],
        workflow_id=spec["workflow_id"],
        heartbeat_details=spec.get("details", []),
    )
    beats = []
    env.on_heartbeat = lambda *details: beats.append(details)
    try:
        dish = await env.run(activities.cook, CookInput(craving=spec["craving"]))
        out = {"recipe": dish.recipe}
    except Exception as e:
        out = {"error": type(e).__name__, "message": str(e)}
    print(json.dumps({**out, "heartbeats": len(beats)}))


asyncio.run(main())
"""


@pytest.fixture
def machine(tmp_path: Path) -> Machine:
    return Machine(tmp_path, {k: v for k, v in os.environ.items() if not k.startswith("KONTRA_")})


def _driver(machine: Machine, workflow_id: str, attempt: int, **spec: Any) -> list[str]:
    run = {"workflow_id": workflow_id, "attempt": attempt, "craving": CRAVING, **spec}
    return [sys.executable, "-c", DRIVER, json.dumps(run)]


def _cook(
    machine: Machine, workflow_id: str, attempt: int, *, cook: str = "done", **spec: Any
) -> dict[str, Any]:
    result = subprocess.run(
        _driver(machine, workflow_id, attempt, **spec),
        cwd=CHEF,
        env={**machine.env, "STAND_IN_COOK": cook},
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout.strip().splitlines()[-1])


def test_a_retry_on_a_machine_without_the_session_starts_it_there(machine: Machine) -> None:
    """Attempt 3 lands on a worker whose ``claude`` never held the session (another machine of
    a fleet ran attempts 1 and 2, or this one lost its home), whatever the heartbeat said: the
    binary refuses ``--resume``, and the attempt starts the session here instead of failing
    every attempt left."""
    out = _cook(machine, "job-fleet", 3, details=[{"events": 7, "last": "assistant"}])
    assert out.get("recipe") == f"Cooked: an omelette, for {CRAVING}", out
    session = str(uuid.uuid5(uuid.NAMESPACE_URL, "job-fleet"))
    refused, started = machine.runs()
    assert _flag(refused, "--resume") == session
    assert _flag(started, "--session-id") == session
    assert "--resume" not in started["argv"]
    assert CRAVING in started["stdin"]


def test_an_attempt_that_finds_the_session_resumes_it(machine: Machine) -> None:
    """The session is on this machine already when nothing on the server says so -- the
    workflow was reset (attempt 1 again, same workflow id), or the attempt that started it died
    before its heartbeat reached Temporal -- so the binary refuses ``--session-id``, and the
    attempt resumes it."""
    session = machine.holds("job-reset")
    out = _cook(machine, "job-reset", 1)
    assert out.get("recipe") == f"Resumed: an omelette, for {CRAVING}", out
    refused, resumed = machine.runs()
    assert _flag(refused, "--session-id") == session
    assert _flag(resumed, "--resume") == session

    # A retry resumes whatever its last heartbeat said: a quiet attempt's keepalive says
    # it heard nothing yet, and the session it started is here all the same.
    again = Machine(machine.home.parent / "again", machine.env)
    session = again.holds("job-quiet")
    out = _cook(again, "job-quiet", 2, details=[{"events": 0, "last": None}])
    assert out.get("recipe") == f"Resumed: an omelette, for {CRAVING}", out
    [resumed] = again.runs()
    assert _flag(resumed, "--resume") == session


def test_the_buyers_words_reach_only_the_binarys_stdin(machine: Machine) -> None:
    """The craving is the buyer's: on the command line any local user could read it
    (``/proc/<pid>/cmdline``), so it goes on stdin. The binary runs in a directory only this
    user can write -- ``claude`` runs the hooks of a ``.claude/settings.json`` it finds where
    it runs, even in print mode -- with none of the seller's own Claude Code setup
    (``--safe-mode``), one tool and no MCP server."""
    out = _cook(machine, "job-words", 1)
    assert out.get("recipe") == f"Cooked: an omelette, for {CRAVING}", out
    [run] = machine.runs()
    assert not any(CRAVING in arg for arg in run["argv"]), run["argv"]
    assert run["stdin"].endswith(f"Craving: {CRAVING}")
    assert Path(run["cwd"]) == machine.kitchen
    assert machine.kitchen.stat().st_mode & 0o777 == 0o700
    assert "--safe-mode" in run["argv"]
    assert "--strict-mcp-config" in run["argv"]
    assert _flag(run, "--tools") == _flag(run, "--allowedTools") == "WebSearch"
    assert out["heartbeats"] >= 2  # one per line it printed


def test_a_kitchen_someone_else_can_write_is_refused(machine: Machine) -> None:
    machine.kitchen.mkdir()
    machine.kitchen.chmod(0o777)
    out = _cook(machine, "job-kitchen", 1)
    assert "recipe" not in out, out
    assert str(machine.kitchen) in out["message"]
    assert machine.runs() == []


def test_a_binary_that_falls_silent_is_stopped(machine: Machine) -> None:
    """The keepalive heartbeats while the binary thinks, so the heartbeat timeout would only
    ever catch a dead worker: a binary silent for longer than ``QUIET`` is killed, and the
    attempt fails (retryable: the retry resumes the session)."""
    out = _cook(machine, "job-stuck", 1, cook="silent", set={"QUIET": 1.0, "KEEPALIVE": 0.2})
    assert out.get("error") == "ApplicationError", out
    assert "silent" in out["message"]
    [run] = machine.runs()
    assert not _alive(run["pid"])


@pytest.mark.skipif(sys.platform != "linux", reason="PR_SET_PDEATHSIG is Linux's")
def test_the_binary_dies_with_its_worker(machine: Machine) -> None:
    """A worker killed by SIGKILL takes its ``claude`` with it: an orphan would go on spending
    tokens, and writing the session that the retry is resuming. This one is in a long model
    turn, printing nothing, so no broken pipe would stop it either."""
    worker = subprocess.Popen(
        _driver(machine, "job-orphan", 1),
        cwd=CHEF,
        env={**machine.env, "STAND_IN_COOK": "silent"},
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        deadline = time.monotonic() + 30
        while not machine.runs():
            assert time.monotonic() < deadline, "the binary never started"
            time.sleep(0.1)
        [run] = machine.runs()
        time.sleep(0.5)
        assert _alive(run["pid"])
    finally:
        worker.send_signal(signal.SIGKILL)
        worker.wait(10)
    deadline = time.monotonic() + 10
    while _alive(run["pid"]):
        assert time.monotonic() < deadline, "the binary outlived its worker"
        time.sleep(0.1)


# --------------------------------------------------------------------------- end to end


async def _heartbeated(handle: WorkflowHandle[Any, Any]) -> int:
    """How many pending activities of the workflow have a heartbeat on the server."""
    described = await handle.describe()
    return sum(
        1 for a in described.raw_description.pending_activities if a.heartbeat_details.payloads
    )


@pytest.mark.temporal
@pytest.mark.asyncio(loop_scope="package")
async def test_a_crashed_worker_resumes_the_binarys_session(market: Market, tmp_path: Path) -> None:
    machine = Machine(tmp_path, seller_env(market.seller.home, market.sites))
    first = Seller("chef-claude", machine.env, tmp_path / "first.log")
    second = Seller("chef-claude", machine.env, tmp_path / "second.log")
    market.sellers.update({"chef-claude": first, "chef-claude (restarted)": second})
    first.start()
    try:
        await first.wait_ready(market.seller_client())
        buyer = await market.buyer()
        q = await buyer.k.quote(OFFERING, craving=CRAVING)
        assert (q.ok, q.price) == (True, Decimal("0.25"))
        job = await q.hire(max_price="0.25")
        workflow_id = job_workflow_id(job.job_id)
        handle = market.seller_client().get_workflow_handle(workflow_id)

        # Mid-run: the binary started its session and the server holds a heartbeat of it.
        await until(lambda: _heartbeated(handle), lambda n: n == 1, within=60)
        [cooking] = machine.runs()
        first.kill()
        if sys.platform == "linux":  # it went with its worker
            await until(lambda: _gone(cooking["pid"]), bool, within=10)
        second.start()
        result = await job.result(timeout=120)
        view = await job.accept()
    finally:
        await first.stop()
        await second.stop()

    session = str(uuid.uuid5(uuid.NAMESPACE_URL, workflow_id))
    runs = machine.runs()
    assert len(runs) == 2, runs
    started, resumed = runs
    assert _flag(started, "--session-id") == session
    assert "--resume" not in started["argv"]
    assert started["stdin"].endswith(f"Craving: {CRAVING}")
    assert _flag(resumed, "--resume") == session
    assert "--session-id" not in resumed["argv"]
    for run in runs:  # the same binary, the same leash
        assert not any(CRAVING in arg for arg in run["argv"])
        assert _flag(run, "--output-format") == "stream-json"
        assert "--verbose" in run["argv"]
        assert "--safe-mode" in run["argv"]
        assert _flag(run, "--allowedTools") == _flag(run, "--tools")
        assert int(_flag(run, "--max-turns") or 0) > 0
        assert Path(run["cwd"]) == machine.kitchen
    assert result.recipe == f"Resumed: an omelette, for {CRAVING}"
    assert (view.state, view.escrow) == (m.JobState.COMPLETED, m.Escrow.RELEASED)

    # One retry: the activity that completed is the second attempt.
    attempts = [
        e.activity_task_started_event_attributes.attempt
        async for e in handle.fetch_history_events()
        if e.HasField("activity_task_started_event_attributes")
    ]
    assert attempts == [2]


async def _gone(pid: int) -> bool:
    return not _alive(pid)
