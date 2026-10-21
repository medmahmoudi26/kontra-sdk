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
    UNSEALED_REASON,
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
        amount=Money(1000, "USD"),
        max_duration=timedelta(hours=1),
        review_window=timedelta(hours=1),
        now=T0,
        grace=timedelta(seconds=30),
    )


def seen(
    status: ObservedStatus,
    provenance: Provenance = Provenance.VERIFIED,
    pin: str | None = RUN,
    closed_at: datetime | None = None,
    result_sha256: str | None = None,
    result_size: int | None = None,
    started_at: datetime | None = None,
) -> Observation:
    return Observation(
        status=status,
        provenance=provenance,
        pin=pin,
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
    assert (obs.provenance, obs.pin) == (Provenance.UNVERIFIED, None)
    record, effects = escrow.observe(job(), obs, SOON)
    assert (record, effects) == (job(), [])


def test_a_verified_completion_delivers_and_pins_its_run() -> None:
    obs = seen(ObservedStatus.COMPLETED, closed_at=SOON, result_sha256="abc", result_size=3)
    record, effects = escrow.observe(job(), obs, SOON)
    assert (record.state, record.escrow, record.pin) == (JobState.COMPLETED, Escrow.HELD, RUN)
    assert (record.delivery_sha256, record.delivery_size) == ("abc", 3)
    assert effects == []


def test_a_verified_running_run_is_pinned_while_it_works() -> None:
    record, effects = escrow.observe(job(), seen(ObservedStatus.RUNNING, started_at=T0), SOON)
    assert (record.state, record.pin, record.started_at, effects) == (
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
    assert (record.delivered_at, record.delivery_sha256, record.pin) == (None, None, None)
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


def test_a_verified_observation_without_a_pin_is_not_trusted() -> None:
    obs = seen(ObservedStatus.COMPLETED, pin=None, closed_at=SOON)
    assert escrow.observe(job(), obs, SOON) == (job(), [])


def test_a_pinned_job_ignores_every_other_run() -> None:
    working, _ = escrow.observe(job(), seen(ObservedStatus.RUNNING), SOON)
    assert working.pin == RUN
    for status in ObservedStatus:
        if status is ObservedStatus.NOT_FOUND:
            continue
        other = seen(status, pin="run-the-sellers-own", closed_at=SOON, result_sha256="x")
        assert escrow.observe(working, other, SOON) == (working, [])
    # Past the deadline, another run is the same as no news: the pinned run ran over.
    other = seen(ObservedStatus.COMPLETED, pin="run-the-sellers-own", closed_at=SOON)
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
    assert (record.state, record.pin) == (JobState.COMPLETED, RUN)


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
                    pin=rng.choice([None, "run-x", RUN]),
                    closed_at=now,
                    result_sha256="x",
                    result_size=1,
                )
                record, effects = escrow.observe(record, obs, now)
            assert record.state is not JobState.COMPLETED
            assert record.escrow in (Escrow.HELD, Escrow.REFUNDED)
            assert record.pin is None
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
    obs = seen(status, Provenance.OCCUPIED, pin="run-placeholder")
    assert escrow.observe(job(), obs, SOON) == (job(), [])
    record, effects = escrow.observe(job(), obs, AFTER_DEADLINE)
    assert (record.state, record.escrow, record.reason, record.pin) == (
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


# --------------------------------------------------------------------------- cancel of a running job


def working() -> JobRecord:
    record, _ = escrow.observe(job(), seen(ObservedStatus.RUNNING, started_at=T0), SOON)
    assert (record.state, record.pin) == (JobState.WORKING, RUN)
    return record


def test_a_job_with_no_observed_run_is_refunded_at_once() -> None:
    """Nothing pinned: no run is the buyer's call, so none can deliver to it."""
    record, effects = escrow.cancel(job(), "bob", SOON)
    assert (record.state, record.escrow, record.reason) == (
        JobState.CANCELED,
        Escrow.REFUNDED,
        BUYER_CANCEL_REASON,
    )
    assert [e.kind for e in effects] == [EffectKind.CANCEL_WORKFLOW, EffectKind.REFUND]


def test_cancelling_an_observed_run_refunds_nothing_until_it_is_seen_terminated() -> None:
    """Step one asks for the termination and moves no money; asking again, or seeing the
    run still open, asks again; only the pinned chain seen terminated refunds, as the
    buyer's cancel (never "seller workflow terminated")."""
    record, effects = escrow.cancel(working(), "bob", SOON)
    assert (record.state, record.escrow, record.cancel_requested_at) == (
        JobState.WORKING,
        Escrow.HELD,
        SOON,
    )
    assert [(e.kind, e.reason) for e in effects] == [
        (EffectKind.TERMINATE_RUN, BUYER_CANCEL_REASON)
    ]
    later = SOON + timedelta(minutes=1)
    again, effects = escrow.cancel(record, "bob", later)
    assert again == record  # the first request's time stands
    assert [e.kind for e in effects] == [EffectKind.TERMINATE_RUN]
    still, effects = escrow.observe(record, seen(ObservedStatus.RUNNING), later)
    assert (still, [e.kind for e in effects]) == (record, [EffectKind.TERMINATE_RUN])
    with pytest.raises(escrow.EscrowError):
        escrow.cancel(record, "acme", later)  # the seller cannot ask for it
    done, effects = escrow.observe(record, seen(ObservedStatus.TERMINATED), later)
    assert (done.state, done.escrow, done.reason) == (
        JobState.CANCELED,
        Escrow.REFUNDED,
        BUYER_CANCEL_REASON,
    )
    assert [(e.kind, e.reason) for e in effects] == [(EffectKind.REFUND, BUYER_CANCEL_REASON)]
    assert escrow.cancel(done, "bob", later) == (done, [])


def test_a_run_that_completed_before_the_termination_is_a_delivery() -> None:
    record, _ = escrow.cancel(working(), "bob", SOON)
    delivered, effects = escrow.observe(
        record, seen(ObservedStatus.COMPLETED, closed_at=SOON, result_sha256="x"), SOON
    )
    assert (delivered.state, delivered.escrow, delivered.delivery_sha256) == (
        JobState.COMPLETED,
        Escrow.HELD,
        "x",
    )
    assert effects == []
    with pytest.raises(escrow.EscrowError) as refused:
        escrow.cancel(delivered, "bob", SOON)
    assert refused.value.code == "not_open"
    released, effects = escrow.accept(delivered, "bob", SOON)
    assert (released.escrow, [e.kind for e in effects]) == (Escrow.RELEASED, [EffectKind.RELEASE])


@pytest.mark.parametrize(
    ("status", "state", "reason"),
    [
        (ObservedStatus.FAILED, JobState.FAILED, "seller workflow failed"),
        (ObservedStatus.TIMED_OUT, JobState.FAILED, "seller workflow timed out"),
        (ObservedStatus.CANCELED, JobState.CANCELED, BUYER_CANCEL_REASON),
    ],
)
def test_a_run_that_closed_on_its_own_first_refunds_as_it_would_have(
    status: ObservedStatus, state: JobState, reason: str
) -> None:
    record, _ = escrow.cancel(working(), "bob", SOON)
    closed, effects = escrow.observe(record, seen(status, closed_at=SOON), SOON)
    assert (closed.state, closed.escrow, closed.reason) == (state, Escrow.REFUNDED, reason)
    assert [e.kind for e in effects] == [EffectKind.REFUND]


def test_a_cancel_still_pending_at_the_deadline_is_the_buyers() -> None:
    """The platform could not terminate the run before the deadline: the job refunds then,
    as any job does, and it is the buyer's cancel rather than the seller's overrun."""
    record, _ = escrow.cancel(working(), "bob", SOON)
    for closed, effects in (
        escrow.tick(record, AFTER_DEADLINE),
        escrow.observe(record, seen(ObservedStatus.RUNNING), AFTER_DEADLINE),
    ):
        assert (closed.state, closed.escrow, closed.reason) == (
            JobState.CANCELED,
            Escrow.REFUNDED,
            BUYER_CANCEL_REASON,
        )
        assert [e.kind for e in effects] == [EffectKind.TERMINATE_WORKFLOW, EffectKind.REFUND]
    # Without the cancel, the same deadline is the seller's.
    closed, _ = escrow.tick(working(), AFTER_DEADLINE)
    assert closed.reason == "max_duration exceeded"


def test_no_interleaving_refunds_a_cancel_whose_run_may_still_deliver() -> None:
    """Whatever the buyer, the pinned run and the clock do, in whatever order: a REFUND
    after a cancel was requested comes only with a close that is not a completion, or at
    the deadline; a completion seen at any point is a delivery and never refunded; and
    every REFUND comes last among the effects that carry it."""
    rng = random.Random(16)
    statuses = [s for s in ObservedStatus if s is not ObservedStatus.NOT_FOUND]
    for _ in range(3000):
        record, now = working(), SOON
        for _ in range(rng.randint(1, 10)):
            now += timedelta(minutes=rng.randint(0, 25))
            before = record
            roll = rng.random()
            obs: Observation | None = None
            if roll < 0.35:
                try:
                    record, effects = escrow.cancel(record, "bob", now)
                except escrow.EscrowError:
                    continue
            elif roll < 0.45:
                record, effects = escrow.tick(record, now)
            else:
                obs = seen(rng.choice(statuses), closed_at=now, result_sha256="x", result_size=1)
                record, effects = escrow.observe(record, obs, now)
            kinds = [e.kind for e in effects]
            if EffectKind.REFUND in kinds:
                assert kinds[-1] is EffectKind.REFUND
                if before.cancel_requested_at is not None or record.pin is not None:
                    closed_alone = obs is not None and obs.status not in (
                        ObservedStatus.RUNNING,
                        ObservedStatus.COMPLETED,
                    )
                    assert closed_alone or now >= before.deadline, (before, obs, now)
            delivered = before.state is JobState.COMPLETED
            completes_in_time = (
                before.open
                and now < before.deadline
                and obs is not None
                and obs.status is ObservedStatus.COMPLETED
            )
            if delivered or completes_in_time:
                assert record.state is JobState.COMPLETED
                assert EffectKind.REFUND not in kinds


# --------------------------------------------------------------------------- unsealed


def test_an_unsealed_completion_refunds_against_the_seller() -> None:
    """A verified completion whose result is not Kontra ciphertext (decisions I32, owner
    decision OD3 of #55): the adapter withheld its bytes; never a delivery, refunded at once
    with the reason a gate counts against the seller."""
    record, effects = escrow.observe(job(), seen(ObservedStatus.UNSEALED, closed_at=SOON), SOON)
    assert (record.state, record.escrow, record.reason, record.pin) == (
        JobState.FAILED,
        Escrow.REFUNDED,
        UNSEALED_REASON,
        RUN,
    )
    assert (record.delivered_at, record.delivery_sha256) == (None, None)
    assert [e.kind for e in effects] == [EffectKind.REFUND]


def test_an_unsealed_completion_refunds_whatever_a_cancel_asked() -> None:
    """A buyer's cancel pending (step two of :func:`escrow.cancel`) does not make an
    unsealed completion the buyer's cancel: the seller's run completed, unsealed."""
    working, _ = escrow.observe(job(), seen(ObservedStatus.RUNNING, started_at=T0), SOON)
    requested, effects = escrow.cancel(working, "bob", SOON)
    assert [e.kind for e in effects] == [EffectKind.TERMINATE_RUN]
    record, effects = escrow.observe(requested, seen(ObservedStatus.UNSEALED, closed_at=SOON), SOON)
    assert (record.state, record.escrow, record.reason) == (
        JobState.FAILED,
        Escrow.REFUNDED,
        UNSEALED_REASON,
    )


@pytest.mark.parametrize("provenance", NOT_THE_BUYERS)
def test_an_unsealed_run_not_the_buyers_moves_nothing(provenance: Provenance) -> None:
    record, effects = escrow.observe(job(), seen(ObservedStatus.UNSEALED, provenance), SOON)
    assert (record, effects) == (job(), [])


def test_an_unsealed_run_outside_the_pin_is_ignored() -> None:
    working, _ = escrow.observe(job(), seen(ObservedStatus.RUNNING, started_at=T0), SOON)
    other = seen(ObservedStatus.UNSEALED, pin="run-the-sellers-own", closed_at=SOON)
    record, effects = escrow.observe(working, other, SOON)
    assert (record.state, record.escrow, effects) == (JobState.WORKING, Escrow.HELD, [])


def test_the_job_record_is_engine_neutral() -> None:
    """Owner decision OD2 of #55: the record names the lineage by an opaque pin and keeps no
    engine coordinate (``seller_ns`` lives in the engine's handle)."""
    fields = set(JobRecord.__dataclass_fields__)
    assert "pin" in fields
    assert not fields & {"run_id", "seller_ns", "workflow_id", "namespace"}
    assert "pin" in Observation.__dataclass_fields__
    assert "run_id" not in Observation.__dataclass_fields__


#: What reputation reads beside escrow's own observation (decisions I40), by name.
_REACH_NAMES = frozenset({"reach", "Reach", "buyer_start", "BuyerStart", "Observed"})


def test_reach_is_reputation_only_and_escrow_never_reads_it() -> None:
    """Decisions I40: how far a job's start got (``Observed.reach``) may cost a seller
    reputation, never money. Escrow is handed the engine's ``Observation`` alone, which
    carries no reach, and its source names none of it (nor imports the engine's types) -- so
    no rule here can move a cent on it -- and the same observation moves money the same way
    whatever reach came with it."""
    import ast
    from pathlib import Path

    from kontra.engine import BuyerStart, Observed, Reach

    tree = ast.parse(Path(escrow.__file__).read_text(encoding="utf-8"))
    names = {n.id for n in ast.walk(tree) if isinstance(n, ast.Name)}
    names |= {n.attr for n in ast.walk(tree) if isinstance(n, ast.Attribute)}
    names |= {
        n.value for n in ast.walk(tree) if isinstance(n, ast.Constant) and isinstance(n.value, str)
    }
    assert not names & _REACH_NAMES
    imported = {
        node.module or "" for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)
    } | {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    }
    assert not {m for m in imported if m.split(".")[:2] == ["kontra", "engine"]}
    for record in (Observation, JobRecord):
        assert not set(record.__dataclass_fields__) & {"reach", "buyer_start"}

    nothing = Observation(ObservedStatus.NOT_FOUND)
    late = T0 + timedelta(hours=2)
    outcomes = {
        repr(
            escrow.observe(
                job(), Observed(nothing, reach=reach, buyer_start=start).observation, late
            )
        )
        for reach in (*Reach, None)
        for start in ((BuyerStart(T0, None, True),) if reach is not None else (None,))
    }
    assert len(outcomes) == 1
    (only,) = outcomes
    assert "seller never started the job" in only  # escrow's own wording, whoever was at fault
