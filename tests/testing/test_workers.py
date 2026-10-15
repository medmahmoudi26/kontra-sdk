"""The Workers view on the fake gate (kontra-cloud#6): a seller reads Temporal's task-queue
stats of its own versions only, through the control plane, with the verdict of
:mod:`kontra.workers`."""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest

from kontra._control import TemporalError
from kontra.gate_client import GateError
from kontra.gate_client import models as m
from kontra.testing.fake_gate import FakeGate
from kontra.workers import offline

from .conftest import World
from .support import ScriptedControl

QUEUE = "acme/enrich@1.0.0"


def _busy() -> dict[m.TaskQueueType, m.TaskQueueTypeInfo]:
    poller = m.PollerInfo(
        identity="4242@host-a",
        last_access_time=datetime(2026, 10, 15, 12, 0, tzinfo=UTC),
        rate_per_second=100_000.0,
        build_id="514f3b79f58b7521c60b95c388ff0cc1",
    )
    growing = m.TaskQueueStats(
        approximate_backlog_count=120,
        approximate_backlog_age=300.0,
        tasks_add_rate=1.2,
        tasks_dispatch_rate=0.4,
        backlog_increase_rate=0.8,
    )
    return {
        kind: m.TaskQueueTypeInfo(
            pollers=[poller, poller.model_copy(update={"identity": "4243@host-b"})],
            stats=growing if kind is m.TaskQueueType.ACTIVITY else m.TaskQueueStats(),
        )
        for kind in m.TaskQueueType
    }


async def test_a_seller_reads_its_own_versions_only(world: World) -> None:
    acme = await world.party("acme")
    bob = await world.party("bob")
    await world.publish(acme)
    view = await acme.gate.workers("acme/enrich@1.0.0")
    assert (view.offering, view.task_queue) == ("acme/enrich@1.0.0", QUEUE)
    assert world.control.described == [("acme", QUEUE)]  # in the seller's own namespace
    assert view.types_info == offline()  # nothing polls it yet
    assert (view.verdict, view.stale) == ("no pollers -- start a worker", True)
    # Another party's version is not found, exactly like one that does not exist.
    for who, ref in ((bob, "acme/enrich@1.0.0"), (acme, "acme/enrich@2"), (acme, "acme/other")):
        with pytest.raises(GateError) as e:
            await who.gate.workers(ref)
        assert (e.value.status, e.value.code) == (404, "offering_not_found")
    assert world.control.described == [("acme", QUEUE)]  # nothing asked of Temporal for them


async def test_the_view_is_temporals_numbers_and_their_verdict(world: World) -> None:
    acme = await world.party("acme")
    await world.publish(acme)
    world.control.task_queues[("acme", QUEUE)] = _busy()
    view = await acme.gate.workers("acme/enrich")
    assert view.types_info == _busy()
    assert view.verdict == "activity backlog growing for 5 min, 2 pollers -- scale up"
    assert view.stale is False


async def test_versions_resolve_among_the_sellers_own_retired_ones_too(world: World) -> None:
    """A retired version's workers may still be finishing its jobs, so its view stays
    readable though the catalog no longer lists it."""
    acme = await world.party("acme")
    for version in ("1.0.0", "1.2.0", "2.0.0"):
        await world.publish(acme, version=version)
    world.gate.offerings["acme/enrich@2.0.0"].retired = True
    assert (await acme.gate.workers("acme/enrich")).offering == "acme/enrich@2.0.0"
    assert (await acme.gate.workers("acme/enrich@1")).offering == "acme/enrich@1.2.0"
    assert (await acme.gate.workers("acme/enrich@1.0.0")).task_queue == QUEUE


@pytest.mark.parametrize(
    "path",
    [
        "/v1/offerings/acme/enrich/versions/01/workers",
        "/v1/offerings/acme/enrich/versions/1.0.0.0/workers",
        "/v1/offerings/ACME/enrich/versions/latest/workers",
        "/v1/offerings/acme/enrich@1.0.0/versions/latest/workers",
    ],
)
async def test_workers_paths_are_validated_segment_by_segment(world: World, path: str) -> None:
    acme = await world.party("acme")
    await world.publish(acme)
    token = await acme.access_token()
    async with httpx.AsyncClient(transport=world.transport, base_url=world.gate.base_url) as http:
        r = await http.get(path, headers={"Authorization": f"Bearer {token}"})
    assert (r.status_code, r.json()["code"]) == (404, "offering_not_found"), path


async def test_temporal_not_answering_is_503(world: World) -> None:
    acme = await world.party("acme")
    await world.publish(acme)
    world.control.fail_describe = TemporalError("frontend down")
    with pytest.raises(GateError) as e:
        await acme.gate.workers("acme/enrich@1.0.0")
    assert (e.value.status, e.value.code) == (503, "temporal_unavailable")


@pytest.fixture
async def offline_world(tmp_path: Path) -> AsyncIterator[World]:
    gate = FakeGate(None, sweep_interval=0)
    async with gate:
        w = World(gate, ScriptedControl(), httpx.ASGITransport(app=gate.app), tmp_path)
        try:
            yield w
        finally:
            await w.close()


async def test_a_gate_without_temporal_reports_every_seller_offline(offline_world: World) -> None:
    acme = await offline_world.party("acme")
    await offline_world.publish(acme)
    view = await acme.gate.workers("acme/enrich@1.0.0")
    assert view.types_info == offline()
    assert (view.verdict, view.stale) == ("no pollers -- start a worker", True)
