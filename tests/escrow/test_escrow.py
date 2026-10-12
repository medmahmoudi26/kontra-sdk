"""The escrow rule for delivery provenance (decisions I15), as pure functions: only a run
the buyer's own call started can deliver, escrow pins that run's chain, and every other
run of ``job-<id>`` is ignored. The same functions drive the gate and the fake gate; the
lookup that produces ``provenance`` is tested against Temporal in ``tests/control``."""

from __future__ import annotations

import random
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from kontra import escrow
from kontra.escrow import (
    BUYER_CANCEL_REASON,
    NOT_FROM_BUYER_REASON,
    OCCUPIED_REASON,
    EffectKind,
    JobRecord,
    Observation,
    ObservedStatus,
    Provenance,
)
from kontra.gate_client.models import Escrow, JobState
from kontra.ledger import Money

T0 = datetime(2026, 10, 11, 12, 0, tzinfo=UTC)
RUN = "run-genuine"


def job() -> JobRecord:
    """A job just hired: max_duration 1h, so the deadline is 1h30s after T0."""
    return escrow.new_job(
        job_id="a" * 32,
        offering="acme/enrich@1.0.0",
        buyer="bob",
        seller="acme",
        seller_ns="acme",
        amount=Money(1000, "USD"),
        max_duration=timedelta(hours=1),
        review_window=timedelta(hours=1),
        now=T0,
        grace=timedelta(seconds=30),
    )


def seen(
    status: ObservedStatus,
    provenance: Provenance = Provenance.VERIFIED,
    run_id: str | None = RUN,
    closed_at: datetime | None = None,
    result_sha256: str | None = None,
    result_size: int | None = None,
    started_at: datetime | None = None,
) -> Observation:
    return Observation(
        status=status,
        provenance=provenance,
        run_id=run_id,
        started_at=started_at,
        closed_at=closed_at,
        result_sha256=result_sha256,
        result_size=result_size,
    )


AFTER_DEADLINE = T0 + timedelta(hours=1, minutes=1)
SOON = T0 + timedelta(minutes=5)
NOT_THE_BUYERS = [Provenance.UNVERIFIED, Provenance.NOT_FROM_BUYER]


def test_an_observation_is_unverified_unless_the_observer_says_otherwise() -> None:
    """The fail-safe default: an observer that does not establish provenance (an old
    gate, a test double) never produces a delivery."""
    obs = Observation(status=ObservedStatus.COMPLETED, result_sha256="x", result_size=1)
    assert (obs.provenance, obs.run_id) == (Provenance.UNVERIFIED, None)
    record, effects = escrow.observe(job(), obs, SOON)
    assert (record, effects) == (job(), [])


def test_a_verified_completion_delivers_and_pins_its_run() -> None:
    obs = seen(ObservedStatus.COMPLETED, closed_at=SOON, result_sha256="abc", result_size=3)
    record, effects = escrow.observe(job(), obs, SOON)
    assert (record.state, record.escrow, record.run_id) == (JobState.COMPLETED, Escrow.HELD, RUN)
    assert (record.delivery_sha256, record.delivery_size) == ("abc", 3)
    assert effects == []


def test_a_verified_running_run_is_pinned_while_it_works() -> None:
    record, effects = escrow.observe(job(), seen(ObservedStatus.RUNNING, started_at=T0), SOON)
    assert (record.state, record.run_id, record.started_at, effects) == (
        JobState.WORKING,
        RUN,
        T0,
        [],
    )


@pytest.mark.parametrize("provenance", NOT_THE_BUYERS)
def test_a_completion_not_from_the_buyers_call_is_no_delivery(provenance: Provenance) -> None:
    obs = seen(
        ObservedStatus.COMPLETED, provenance, closed_at=SOON, result_sha256="abc", result_size=3
    )
    record, effects = escrow.observe(job(), obs, SOON)
    assert (record, effects) == (job(), [])  # nothing moves, nothing is pinned
    record, effects = escrow.observe(record, obs, AFTER_DEADLINE)
    assert (record.state, record.escrow, record.reason) == (
        JobState.FAILED,
        Escrow.REFUNDED,
        NOT_FROM_BUYER_REASON,
    )
    assert (record.delivered_at, record.delivery_sha256, record.run_id) == (None, None, None)
    # The run is not the job's, so escrow does not stop it.
    assert [e.kind for e in effects] == [EffectKind.REFUND]


@pytest.mark.parametrize("provenance", NOT_THE_BUYERS)
@pytest.mark.parametrize(
    "status",
    [
        ObservedStatus.RUNNING,
        ObservedStatus.FAILED,
        ObservedStatus.CANCELED,
        ObservedStatus.TERMINATED,
        ObservedStatus.TIMED_OUT,
    ],
)
def test_any_other_run_not_from_the_buyers_call_is_as_if_there_were_none(
    status: ObservedStatus, provenance: Provenance
) -> None:
    """Not even a close moves the job: a seller cannot fail or cancel a job (and so close
    it for good) through a run the buyer never started."""
    obs = seen(status, provenance)
    assert escrow.observe(job(), obs, SOON) == (job(), [])
    record, effects = escrow.observe(job(), obs, AFTER_DEADLINE)
    assert (record.state, record.escrow, record.reason) == (
        JobState.FAILED,
        Escrow.REFUNDED,
        "seller workflow not started by the buyer's call",
    )
    assert [e.kind for e in effects] == [EffectKind.REFUND]


def test_a_verified_observation_without_a_run_id_is_not_trusted() -> None:
    obs = seen(ObservedStatus.COMPLETED, run_id=None, closed_at=SOON)
    assert escrow.observe(job(), obs, SOON) == (job(), [])


def test_a_pinned_job_ignores_every_other_run() -> None:
    working, _ = escrow.observe(job(), seen(ObservedStatus.RUNNING), SOON)
    assert working.run_id == RUN
    for status in ObservedStatus:
        if status is ObservedStatus.NOT_FOUND:
            continue
        other = seen(status, run_id="run-the-sellers-own", closed_at=SOON, result_sha256="x")
        assert escrow.observe(working, other, SOON) == (working, [])
    # Past the deadline, another run is the same as no news: the pinned run ran over.
    other = seen(ObservedStatus.COMPLETED, run_id="run-the-sellers-own", closed_at=SOON)
    record, effects = escrow.observe(working, other, AFTER_DEADLINE)
    assert (record.state, record.reason) == (JobState.FAILED, "max_duration exceeded")
    assert [e.kind for e in effects] == [EffectKind.TERMINATE_WORKFLOW, EffectKind.REFUND]


def test_the_pinned_run_closing_decides_the_job() -> None:
    working, _ = escrow.observe(job(), seen(ObservedStatus.RUNNING), SOON)
    record, _ = escrow.observe(working, seen(ObservedStatus.TERMINATED), SOON)
    assert (record.state, record.escrow, record.reason) == (
        JobState.FAILED,
        Escrow.REFUNDED,
        "seller workflow terminated",
    )
    record, _ = escrow.observe(working, seen(ObservedStatus.COMPLETED, closed_at=SOON), SOON)
    assert (record.state, record.run_id) == (JobState.COMPLETED, RUN)


def test_a_pinned_run_that_vanished_refunds_at_the_deadline() -> None:
    working, _ = escrow.observe(job(), seen(ObservedStatus.RUNNING), SOON)
    gone = Observation(status=ObservedStatus.NOT_FOUND)
    assert escrow.observe(working, gone, SOON) == (working, [])
    record, _ = escrow.observe(working, gone, AFTER_DEADLINE)
    assert (record.escrow, record.reason) == (Escrow.REFUNDED, "the job's workflow is gone")
    record, _ = escrow.observe(job(), gone, AFTER_DEADLINE)
    assert record.reason == "seller never started the job"


def test_no_sequence_of_runs_not_from_the_buyer_ever_pays_the_seller() -> None:
    """Whatever the seller's own runs do, in whatever order the observer sees them, and
    whatever the buyer does meanwhile, the money never goes to the seller."""
    rng = random.Random(15)
    statuses = [s for s in ObservedStatus if s is not ObservedStatus.NOT_FOUND]
    for _ in range(2000):
        record, now = job(), T0
        for _ in range(rng.randint(1, 8)):
            now += timedelta(minutes=rng.randint(0, 40))
            if rng.random() < 0.15:
                record, effects = escrow.tick(record, now)
            else:
                obs = seen(
                    rng.choice(statuses),
                    rng.choice([*NOT_THE_BUYERS, Provenance.OCCUPIED]),
                    run_id=rng.choice([None, "run-x", RUN]),
                    closed_at=now,
                    result_sha256="x",
                    result_size=1,
                )
                record, effects = escrow.observe(record, obs, now)
            assert record.state is not JobState.COMPLETED
            assert record.escrow in (Escrow.HELD, Escrow.REFUNDED)
            assert record.run_id is None
            assert all(e.kind is not EffectKind.RELEASE for e in effects)
            with pytest.raises(escrow.EscrowError):
                escrow.accept(record, "bob", now)


# --------------------------------------------------------------------------- decisions I16


@pytest.mark.parametrize(
    "status",
    [ObservedStatus.RUNNING, ObservedStatus.TERMINATED, ObservedStatus.TIMED_OUT],
)
def test_the_control_planes_placeholder_is_no_run_and_blames_no_seller(
    status: ObservedStatus,
) -> None:
    """``Control.occupy_job``'s placeholder (a cancel took ``job-<id>`` before any run
    existed) moves nothing and is never pinned; an open job that only ever shows it refunds
    at the deadline saying so, not "seller workflow terminated"."""
    obs = seen(status, Provenance.OCCUPIED, run_id="run-placeholder")
    assert escrow.observe(job(), obs, SOON) == (job(), [])
    record, effects = escrow.observe(job(), obs, AFTER_DEADLINE)
    assert (record.state, record.escrow, record.reason, record.run_id) == (
        JobState.FAILED,
        Escrow.REFUNDED,
        OCCUPIED_REASON,
        None,
    )
    assert [e.kind for e in effects] == [EffectKind.REFUND]  # nothing to stop


def test_a_run_the_buyer_canceled_through_its_own_call_is_the_buyers_cancel() -> None:
    """A verified run that ended canceled because the buyer asked (its own call's record
    says so) refunds as a buyer cancel, which no gate counts against the seller; one the
    buyer did not ask for stays plain "canceled"."""
    working, _ = escrow.observe(job(), seen(ObservedStatus.RUNNING), SOON)
    asked = replace(seen(ObservedStatus.CANCELED), buyer_canceled=True)
    record, effects = escrow.observe(working, asked, SOON)
    assert (record.state, record.escrow, record.reason) == (
        JobState.CANCELED,
        Escrow.REFUNDED,
        BUYER_CANCEL_REASON,
    )
    assert [(e.kind, e.reason) for e in effects] == [(EffectKind.REFUND, BUYER_CANCEL_REASON)]
    record, effects = escrow.observe(working, seen(ObservedStatus.CANCELED), SOON)
    assert (record.state, record.reason) == (JobState.CANCELED, "canceled")
    assert [(e.kind, e.reason) for e in effects] == [(EffectKind.REFUND, "canceled")]
    # A cancel through the gate is the same reason, so a gate treats both alike.
    record, effects = escrow.cancel(job(), "bob", SOON)
    assert record.reason == BUYER_CANCEL_REASON
    assert {e.reason for e in effects} == {BUYER_CANCEL_REASON}


def test_a_buyer_cancel_flag_on_a_run_not_from_the_buyer_moves_nothing() -> None:
    """The flag is only ever set on a verified run; on anything else it changes nothing."""
    for provenance in (*NOT_THE_BUYERS, Provenance.OCCUPIED):
        obs = replace(seen(ObservedStatus.CANCELED, provenance), buyer_canceled=True)
        assert escrow.observe(job(), obs, SOON) == (job(), [])
