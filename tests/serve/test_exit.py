"""``python worker.py`` exits 0 after a graceful stop, every time (slow: many real runs).

temporalio 1.34's Rust core hands each result to the event loop from a thread of its own,
inside ``loop.call_soon_threadsafe``, which lets go of the GIL to write to the loop's
socket. The loop can then finish the worker's shutdown and the interpreter start
finalizing before that thread takes the GIL back; Python 3.12 ends such a thread where it
waits, and the unwinding frees Python objects in the middle of the interpreter's last
garbage collection: SIGSEGV, exit status 139, after a clean stop. ``Worker.run_async``
returns only once no core thread is inside Python code (``_settle_core_threads``).

The race in its smallest form -- a Temporal call is the last thing a process awaits -- ends
in SIGSEGV in 95 runs of 100 pinned to one CPU without the wait, and in none with it; that
test is quick. A whole ``python worker.py`` crashed in 1 run of 80 pinned (1 in 15 on a busy
machine) without the fix and in none of 150 with it: that test is slow. Both pin the
process to one CPU when ``taskset`` exists, so the race does not depend on how busy the
machine is. ``KONTRA_EXIT_RUNS`` sets how many whole runs (default 40)."""

from __future__ import annotations

import asyncio
import os
import shutil
import signal
import subprocess
import sys
import time
import uuid
from pathlib import Path

import pytest

from kontra.testing import Marketplace
from kontra.testing.devserver import DevServer

pytestmark = [pytest.mark.temporal]

EXAMPLE = Path(__file__).resolve().parents[2] / "examples" / "legacy-actor"
RUNS = int(os.environ.get("KONTRA_EXIT_RUNS", "40"))
PIN = ["taskset", "-c", "0"] if shutil.which("taskset") else []
_LAST_CALL = """
import asyncio, sys
from temporalio.api.workflowservice.v1 import GetSystemInfoRequest
from temporalio.client import Client
from kontra.serve.worker import _settle_core_threads

async def main():
    client = await Client.connect(sys.argv[1])
    await client.workflow_service.get_system_info(GetSystemInfoRequest())
    await _settle_core_threads()

asyncio.run(main())
"""


async def test_a_temporal_call_last_before_exit_no_longer_crashes(dev_server: DevServer) -> None:
    for _ in range(20):
        done = await asyncio.to_thread(
            subprocess.run,
            [*PIN, sys.executable, "-c", _LAST_CALL, dev_server.grpc_address],
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )
        assert done.returncode == 0, (done.returncode, done.stderr[-2000:])


def _run_once(cmd: list[str], project: Path, env: dict[str, str], log: Path) -> int:
    """Start the worker, wait until it serves, SIGTERM it (a graceful stop): its status."""
    with log.open("wb") as out:
        proc = subprocess.Popen(cmd, cwd=project, env=env, stdout=out, stderr=subprocess.STDOUT)
    try:
        end = time.monotonic() + 60
        while b"serving " not in log.read_bytes():
            if proc.poll() is not None or time.monotonic() > end:
                raise AssertionError(f"the worker never served:\n{log.read_text()}")
            time.sleep(0.1)
        time.sleep(0.5)
        proc.send_signal(signal.SIGTERM)
        return proc.wait(60)
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait()


@pytest.mark.slow
async def test_python_worker_py_exits_0_after_a_graceful_stop(
    dev_server: DevServer, tmp_path: Path
) -> None:
    project = tmp_path / "probe"
    shutil.copytree(EXAMPLE, project, ignore=shutil.ignore_patterns("__pycache__"))
    async with Marketplace(dev_server=dev_server) as mp:
        party = await mp.party(f"exit-{uuid.uuid4().hex[:8]}")
        offering = project / "offering.yaml"
        offering.write_text(
            offering.read_text().replace("name: acme/probe", f"name: {party.handle}/probe")
        )
        env = {k: v for k, v in os.environ.items() if not k.startswith("KONTRA_")}
        env.update(KONTRA_HOME=str(party.home), PYTHONUNBUFFERED="1", PYTHONFAULTHANDLER="1")
        failed: list[tuple[int, int, str]] = []
        for i in range(RUNS):
            log = tmp_path / f"run{i}.log"
            cmd = [*PIN, sys.executable, "worker.py"]
            code = await asyncio.to_thread(_run_once, cmd, project, env, log)
            if code != 0:
                failed.append((i, code, log.read_text()[-2000:]))
        assert not failed, f"{len(failed)} of {RUNS} runs did not exit 0: {failed[0]}"
