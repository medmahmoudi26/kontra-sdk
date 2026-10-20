"""``kontra status --workers`` against a served fake gate with no Temporal behind it (every
seller offline, unless a test hands the gate Temporal's answer): the versions it reads,
what it prints, its JSON, and its refusals."""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import pytest
import pytest_asyncio

from kontra.engine.temporal import TemporalEngine
from kontra.gate_client import models as m
from kontra.testing import Marketplace
from kontra.workers import STALE_NOTE

from .support import Cli, explain, handle

pytestmark = pytest.mark.asyncio(loop_scope="module")


@dataclass
class Store:
    mp: Marketplace
    root: Path
    seller: Cli
    project: Path
    offering: str


@pytest_asyncio.fixture(loop_scope="module", scope="module")
async def store(tmp_path_factory: pytest.TempPathFactory) -> AsyncIterator[Store]:
    root = tmp_path_factory.mktemp("status")
    async with Marketplace(temporal=False) as mp:
        seller = Cli(root / "seller-home", cwd=root)
        await seller.ok("login", "--dev", "--handle", handle("seller"), "--gate", mp.base_url)
        await seller.ok("init", "shop")
        [entry] = await seller.json("publish", "--dir", str(root / "shop"))
        yield Store(mp, root, seller, root / "shop", entry["offering"])


async def test_status_reads_worker_py_and_shows_every_seller_offline(store: Store) -> None:
    out = (await store.seller.ok("status", "--workers", "--dir", str(store.project))).stdout
    lines = out.splitlines()
    assert lines[0] == f"{store.offering}  no pollers -- start a worker"
    assert lines[1] == f"  task queue  {store.offering}"
    assert lines[2:5] == [
        f"  {kind:<10}  backlog 0, add 0.00/s, dispatch 0.00/s, change 0.00/s, 0 pollers"
        for kind in ("workflow", "activity", "nexus")
    ]
    assert lines[5] == f"  note        {STALE_NOTE}"  # Temporal reported nothing at all
    # `--workers` is the one section so far: `kontra status` alone shows the same.
    assert (await store.seller.ok("status", store.offering)).stdout == out


async def test_status_json_is_the_gates_view(store: Store) -> None:
    [view] = await store.seller.json("status", "--workers", store.offering)
    parsed = m.WorkersView.model_validate(view)
    assert (parsed.offering, parsed.stale) == (store.offering, True)
    assert set(view["types_info"]) == {"workflow", "activity", "nexus"}


class _Temporal:
    """The control plane's answer to every DescribeTaskQueue."""

    def __init__(self, described: dict[m.TaskQueueType, m.TaskQueueTypeInfo]) -> None:
        self.described = described

    async def describe_task_queue(
        self, namespace: str, task_queue: str
    ) -> dict[m.TaskQueueType, m.TaskQueueTypeInfo]:
        return self.described


async def test_status_shows_the_backlog_and_each_worker_once(store: Store) -> None:
    def seen(identity: str, minute: int, **said: str) -> m.PollerInfo:
        return m.PollerInfo(
            identity=identity,
            last_access_time=datetime(2026, 10, 15, 12, minute, tzinfo=UTC),
            rate_per_second=100_000.0,
            build_id=f"build-{identity}",
            **said,
        )

    sdk = {"sdk_name": "temporal-python", "sdk_version": "1.34.0"}

    growing = m.TaskQueueStats(
        approximate_backlog_count=120,
        approximate_backlog_age=300.0,
        tasks_add_rate=1.2,
        tasks_dispatch_rate=0.4,
        backlog_increase_rate=0.8,
    )
    described = {
        m.TaskQueueType.WORKFLOW: m.TaskQueueTypeInfo(pollers=[seen("a@1", 2), seen("b\x1b@2", 1)]),
        m.TaskQueueType.ACTIVITY: m.TaskQueueTypeInfo(
            pollers=[seen("a@1", 3, deployment_name="shop", **sdk)], stats=growing
        ),
        m.TaskQueueType.NEXUS: m.TaskQueueTypeInfo(pollers=[seen("a@1", 0)]),
    }
    gate = store.mp.gate
    # The Workers view is the Temporal adapter's (seam ⑥): give the gate one for the call.
    gate.engines["temporal"] = TemporalEngine(
        _Temporal(described),  # type: ignore[arg-type]
        signer=gate.signer,
        address=gate.temporal_address,
    )
    try:
        out = (await store.seller.ok("status", store.offering)).stdout
    finally:
        del gate.engines["temporal"]
    assert out.splitlines() == [
        f"{store.offering}  activity backlog growing for 5 min, 1 poller -- scale up",
        f"  task queue  {store.offering}",
        "  workflow    backlog 0, add 0.00/s, dispatch 0.00/s, change 0.00/s, 2 pollers",
        "  activity    backlog 120 (oldest 5 min), add 1.20/s, dispatch 0.40/s, "
        "change +0.80/s, 1 poller",
        "  nexus       backlog 0, add 0.00/s, dispatch 0.00/s, change 0.00/s, 1 poller",
        # by identity, most recent first: its latest poll and what it said of itself then
        "  pollers     a@1 (workflow, activity, nexus), last poll 2026-10-15 12:03:00Z, "
        "build build-a@1, deployment shop, sdk temporal-python 1.34.0",
        "              b@2 (workflow), last poll 2026-10-15 12:01:00Z, build build-b@2",
    ]


async def test_one_refused_version_does_not_hide_the_others(store: Store) -> None:
    """A version the gate refuses (here one not published yet, as worker.py may bind) is one
    line on stderr; the others are still shown, and the exit status says one was refused."""
    unpublished = store.offering.rsplit("@", 1)[0] + "@9.0.0"
    result = await store.seller.run("status", unpublished, store.offering)
    assert result.exit_code == 1, explain(result)
    assert result.stdout.splitlines()[0] == f"{store.offering}  no pollers -- start a worker"
    assert result.stderr.splitlines() == [
        f"error: {unpublished}: the gate answered 404 offering_not_found: no such offering"
    ]
    as_json = await store.seller.run("status", "--json", unpublished, store.offering)
    assert as_json.exit_code == 1, explain(as_json)
    assert [v["offering"] for v in json.loads(as_json.stdout)] == [store.offering]


async def test_status_refusals(store: Store) -> None:
    nowhere = await store.seller.run("status", "--workers")  # no worker.py where it runs
    assert nowhere.exit_code == 1, explain(nowhere)
    assert "name an offering (acme/enrich@1.2.0)" in nowhere.stderr
    malformed = await store.seller.run("status", "Acme/enrich")
    assert malformed.exit_code == 1, explain(malformed)
    assert "is not an offering reference" in malformed.stderr
    # Another party's version is the gate's 404, as if it did not exist.
    other = Cli(store.root / "other-home", cwd=store.root)
    await other.ok("login", "--dev", "--handle", handle("other"), "--gate", store.mp.base_url)
    theirs = await other.run("status", store.offering)
    assert theirs.exit_code == 1, explain(theirs)
    assert "404 offering_not_found" in theirs.stderr
