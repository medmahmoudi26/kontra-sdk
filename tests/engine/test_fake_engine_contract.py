"""Seam ⑥: the engine contract against :class:`~kontra.testing.FakeEngine` -- every check
runs, none is skipped -- and what the fake adds for a gate's tests."""

from __future__ import annotations

import pytest
from engine_harness import FakeHarness, party, seal_text

from kontra.contract.names import OfferingId, new_endpoint_name, new_job_id
from kontra.engine import EngineError, InvalidHandle, JobPlan, NotSealed, Stop
from kontra.escrow import ObservedStatus, Provenance
from kontra.testing import FakeEngine, fake_engine
from kontra.testing.engine_contract import CHECKS, run_engine_contract


async def test_the_fake_engine_passes_every_check() -> None:
    report = await run_engine_contract(lambda: FakeHarness().setup(), settle=0.05)
    assert [line.split()[0] for line in report] == list(CHECKS)
    assert all(" ok: " in line for line in report), report


async def test_only_runs_the_checks_named_and_e2_with_e18() -> None:
    report = await run_engine_contract(lambda: FakeHarness().setup(), only={"E1", "E18"})
    assert [line.split()[0] for line in report] == ["E1", "E2", "E18"]
    with pytest.raises(ValueError, match="no such checks"):
        await run_engine_contract(lambda: FakeHarness().setup(), only={"E99"})


async def test_fake_engine_is_the_testing_export() -> None:
    engine = fake_engine(name="temporal")
    assert isinstance(engine, FakeEngine)
    assert engine.name == "temporal"


async def test_a_run_nothing_serves_stays_running_until_ended() -> None:
    h = await FakeHarness().setup()
    h.fake.offline(h.offering)
    job_id = new_job_id()
    handle = h.fake.bind(JobPlan(job_id, h.offering, h.route, h.buyer, h.seller))
    holds = frozenset()
    await h.fake.start(handle, b"{}", hold="testhold", deadline=_far())
    seen = await h.fake.observe(handle, pin=None, holds=holds)
    assert seen.observation.status is ObservedStatus.RUNNING
    assert seen.observation.provenance is Provenance.NOT_FROM_BUYER  # holds not the gate's
    await h.fake.cancel(handle, stop=Stop.END, pin=None, reason="deadline")
    assert (await h.fake.observe(handle, pin=None, holds=holds)).observation.status is (
        ObservedStatus.TERMINATED
    )
    assert h.fake.runs(handle) == 1


async def test_its_handles_are_its_own() -> None:
    fake = FakeEngine()
    seller = party("s")
    plan = JobPlan(
        new_job_id(), OfferingId.parse(f"{seller}/x@1.0.0"), new_endpoint_name(), party("b"), seller
    )
    handle = fake.bind(plan)
    assert handle.startswith(b"fake/1\n")
    with pytest.raises(InvalidHandle):
        await fake.observe(handle.replace(b"fake/1", b"temporal/1"), pin=None, holds=())
    fake.unreachable = True
    with pytest.raises(EngineError):
        await fake.observe(handle, pin=None, holds=())
    assert fake.bind(plan) == handle  # bind is pure: no engine needed


async def test_the_seller_side_refuses_plaintext_items_and_one_open_run() -> None:
    h = await FakeHarness().setup()
    job_id = new_job_id()
    handle = h.fake.bind(JobPlan(job_id, h.offering, h.route, h.buyer, h.seller))
    assert h.fake.seller_start(handle, seal_text("mine", h.recipient))
    assert h.fake.seller_revive(handle) is False
    from kontra.testing.engine import RunContext, _Run  # pyright: ignore[reportPrivateUsage]

    ctx = RunContext(_Run("pin-1", by_gate=False, hold=None, started_at=_far()))
    with pytest.raises(NotSealed):
        ctx.publish("events", b"plaintext")


def _far():  # type: ignore[no-untyped-def]
    from datetime import UTC, datetime, timedelta

    return datetime.now(UTC) + timedelta(hours=1)
