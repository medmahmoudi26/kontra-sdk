"""A worker that shuts down on a session that lives on leaves no token refresher behind
(FIXUPS #23): ``kontra.serve.Worker`` releases the client it connected through
:meth:`kontra.auth.Session.connect_temporal`, checked with asyncio's own task registry."""

from __future__ import annotations

import asyncio
import json
import os
import uuid
from collections.abc import Iterator
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from kontra.serve import Quote, Worker, offering
from kontra.testing import Marketplace
from kontra.testing.devserver import (
    CLI_ENV,
    DevServer,
    acquire_shared_dev_server,
    release_shared_dev_server,
)

from .worker_flows import Ping, PingFlow

pytestmark = pytest.mark.temporal


@pytest.fixture(scope="module")
def dev_server() -> Iterator[DevServer]:
    try:
        server = acquire_shared_dev_server()
    except Exception as e:
        if os.environ.get(CLI_ENV):
            raise  # an explicit binary must work
        pytest.skip(f"no Temporal CLI 1.9.1 dev server: {e}")
    try:
        yield server
    finally:
        release_shared_dev_server()


async def quote(ping: Ping) -> Quote:
    return Quote(price=Decimal("0.01"))


def _offering(directory: Path) -> Path:
    spec = {
        "name": "acme/ping",
        "version": f"1.{os.getpid() % 100_000}.{uuid.uuid4().int % 10_000}",
        "description": "Answers a ping",
        "max_duration": "10m",
        "review_window": "1h",
        "max_output_bytes": "1MB",
        "artifact_retention": "1d",
    }
    path = directory / "offering.yaml"
    path.write_text("".join(f"{k}: {json.dumps(v)}\n" for k, v in spec.items()))
    return path


def _refreshers() -> list[asyncio.Task[Any]]:
    return [
        t
        for t in asyncio.all_tasks()
        if t.get_name().startswith("kontra-temporal-token-") and not t.done()
    ]


async def test_the_refresher_stops_with_the_worker_not_the_session(
    dev_server: DevServer, tmp_path: Path
) -> None:
    async with Marketplace(dev_server=dev_server) as mp:
        acme = await mp.party("acme")
        worker = Worker(offerings=[offering(_offering(tmp_path), workflow=PingFlow, quote=quote)])
        assert _refreshers() == []
        async with mp.serve(worker, acme):  # the party's session: it outlives the worker
            assert len(_refreshers()) == 1
        assert _refreshers() == []
        assert not acme.session._tasks  # pyright: ignore[reportPrivateUsage]
        await acme.gate.me()  # the session itself is still open and usable
        # A second run on the same session starts (and stops) its own refresher.
        async with mp.serve(worker, acme):
            assert len(_refreshers()) == 1
        assert _refreshers() == []
