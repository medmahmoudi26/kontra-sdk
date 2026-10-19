"""Each seller's activity gives a buyer's URL (target) an overall deadline. A client timeout
applies to each network operation, so a site that sends a header line every second keeps a
request open for as long as it likes; without an overall deadline one such URL holds its
batch -- and, with enough of them, the worker and every other buyer's job on it.

Each activity runs as plain Temporal code (``ActivityEnvironment``) in its example's own
directory, as ``python worker.py`` would import it, with the deadline shortened to 2 s,
against the ``stall`` site (it would answer after two minutes)."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from typing import Any

import pytest

from .sites import Sites
from .support import EXAMPLES

DEADLINE = 2
_SCRIPT = """
import asyncio, json, sys, time
from temporalio.testing import ActivityEnvironment
import activities, models

URL = sys.argv[1]
activities.{deadline} = {seconds}
env = ActivityEnvironment()
beats = []
env.on_heartbeat = lambda *details: beats.append(details)

async def main():
    return await env.run({activity}, {batch})

start = time.monotonic()
out = asyncio.run(main())
out = out if isinstance(out, list) else [out]
print(json.dumps({{
    "seconds": time.monotonic() - start,
    "out": [o.model_dump(mode="json") for o in out],
    "beats": len(beats),
}}))
"""
#: Each example's activity over the one stalled URL (or its site): the example's directory,
#: its deadline setting, the call, and the heartbeats it sends meanwhile (a batch reports
#: the URL it gave up on; one URL's own activity says nothing).
CASES = {
    "enrich": dict(
        directory="enrich",
        deadline="URL_DEADLINE_SECONDS",
        activity="activities.fetch_batch",
        batch="models.Batch(urls=[URL], done=0, total=1)",
        beats=1,
    ),
    "legacy-actor": dict(
        directory="legacy-actor",
        deadline="TARGET_DEADLINE_SECONDS",
        activity="activities.head_batch",
        batch="models.Batch(targets=[models.Target(url=URL)], done=0, total=1)",
        beats=1,
    ),
    "sweep": dict(
        directory="sweep",
        deadline="URL_DEADLINE_SECONDS",
        activity="activities.check_headers",
        batch="URL",
        beats=0,
    ),
    "sweep-sites": dict(
        directory="sweep",
        deadline="URL_DEADLINE_SECONDS",
        activity="activities.fetch_robots",
        batch="[URL.rstrip('/')]",
        beats=1,
    ),
}


def _run(name: str, url: str, sites: Sites) -> dict[str, Any]:
    script = _SCRIPT.format(seconds=DEADLINE, **CASES[name])
    result = subprocess.run(
        [sys.executable, "-c", script, url],
        cwd=EXAMPLES / str(CASES[name]["directory"]),
        env={**os.environ, "SSL_CERT_FILE": str(sites.ca_file)},
        capture_output=True,
        text=True,
        timeout=90,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


@pytest.mark.parametrize("name", sorted(CASES))
def test_a_url_that_stalls_ends_at_its_deadline(name: str, sites: Sites) -> None:
    sites.rearm()
    before = sites.stalled
    try:
        ran = _run(name, sites.stall("/"), sites)
    finally:
        sites.release()
    assert sites.stalled == before + 1  # it was there, held by the site
    [record] = ran["out"]
    assert (record["status"], record["error"]) == (None, "TimeoutError")
    assert DEADLINE <= ran["seconds"] < DEADLINE + 8
    assert ran["beats"] == CASES[name]["beats"]  # a batch still reports it, and goes on
