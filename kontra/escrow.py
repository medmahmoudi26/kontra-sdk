"""The job lifecycle and escrow rules (PRD §8), as pure functions.

The gate (platform) and the fake gate (:mod:`kontra.testing`) both drive jobs with these
functions, so "who gets paid when" has one implementation, readable by every seller and
buyer. Nothing here does I/O: callers persist the returned :class:`JobRecord` and then
carry out the returned :class:`Effect` s (ledger calls, workflow cancellation). Ledger calls
are idempotent, so a crash between the two is repaired by :func:`reconcile` on the next
sweep.

Every input is a fact the platform can observe without reading a payload (§8.3): the
state of the seller's handler workflow ``job-<id>`` in the seller's namespace, the clock,
and the buyer's accept / reject / cancel calls.

Only a run the buyer's own Nexus call started can deliver (decisions I15). A seller can
start ``job-<id>`` itself, with any input, or stop the run the buyer's call started and
complete a later one; the observer establishes by lookup in the buyer's namespace which
run (if any) the buyer's call started (:class:`Provenance`), escrow pins that run's chain
(:attr:`JobRecord.run_id`) and ignores every other run of the id.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from enum import StrEnum

from kontra.gate_client.models import Escrow, JobState
from kontra.ledger import HoldState, Money

__all__ = [
    "DEFAULT_GRACE",
    "NOT_FROM_BUYER_REASON",
    "Effect",
    "EffectKind",
    "EscrowError",
    "JobRecord",
    "Observation",
    "ObservedStatus",
    "Provenance",
    "accept",
    "cancel",
    "new_job",
    "observe",
    "reconcile",
    "reject",
    "resolve",
    "tick",
]

#: Added to ``max_duration`` for the refund deadline (acceptance criterion 4 allows one
#: minute; the sweeper interval must stay well below what is left).
DEFAULT_GRACE = timedelta(seconds=30)

_OPEN = frozenset({JobState.SUBMITTED, JobState.WORKING})

#: Why a job whose only completed run was not started by the buyer's call refunds.
NOT_FROM_BUYER_REASON = "delivery not started by the buyer's call"


class EscrowError(Exception):
    """A buyer call that the rules refuse (wrong state, window closed, not the buyer)."""

    def __init__(self, code: str, message: str) -> None:
        self.code = code
        super().__init__(message)


class ObservedStatus(StrEnum):
    NOT_FOUND = "not_found"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELED = "canceled"
    TERMINATED = "terminated"
    TIMED_OUT = "timed_out"


class Provenance(StrEnum):
    """Whether the observed run of ``job-<id>`` is the one the buyer's ``execute`` call
    started, as an authoritative lookup in the buyer's namespace established it. Only
    ``VERIFIED`` runs count for anything; the other two differ only in what the observer
    could tell (logs, support), never in what escrow does."""

    #: The chain's first run was started with the request id of a Nexus ``execute`` call
    #: recorded in the buyer's namespace, on this job's endpoint, carrying this job's hold.
    VERIFIED = "verified"
    #: Not established: a record the lookup needs does not exist (any more). Never a
    #: delivery.
    UNVERIFIED = "unverified"
    #: Established to be some other run: it names no call of this job in the buyer's
    #: namespace, or one whose endpoint, operation, request id or hold differs, or the
    #: buyer's call recorded a different run as the one it started.
    NOT_FROM_BUYER = "not_from_buyer"


@dataclass(frozen=True, slots=True)
class Observation:
    """What the platform saw of ``job-<id>`` in the seller's namespace: the latest run of
    the continue-as-new chain the buyer's call started once escrow has pinned it, else the
    latest run of the id.

    The default ``provenance`` is ``UNVERIFIED`` on purpose: an observer that does not
    establish provenance gets no delivery, never an unchecked one."""

    status: ObservedStatus
    started_at: datetime | None = None
    closed_at: datetime | None = None
    #: sha256 / size of the completion's result payload as stored (ciphertext).
    result_sha256: str | None = None
    result_size: int | None = None
    provenance: Provenance = Provenance.UNVERIFIED
    #: The run id of the observed chain's first run: what escrow pins once it is verified.
    run_id: str | None = None


class EffectKind(StrEnum):
    RELEASE = "release"
    REFUND = "refund"
    DISPUTE = "dispute"
    CANCEL_WORKFLOW = "cancel_workflow"
    TERMINATE_WORKFLOW = "terminate_workflow"
    #: Carry out the resolver's split (``Ledger.resolve``). The split is the resolver's
    #: decision, recorded by the caller with it; the effect only names the job.
    RESOLVE = "resolve"


@dataclass(frozen=True, slots=True)
class Effect:
    kind: EffectKind
    job_id: str
    reason: str = ""


@dataclass(frozen=True, slots=True)
class JobRecord:
    job_id: str
    offering: str
    buyer: str
    seller: str
    seller_ns: str
    amount: Money
    review_window: timedelta
    created_at: datetime
    deadline: datetime
    state: JobState = JobState.SUBMITTED
    escrow: Escrow = Escrow.HELD
    started_at: datetime | None = None
    delivered_at: datetime | None = None
    review_deadline: datetime | None = None
    delivery_sha256: str | None = None
    delivery_size: int | None = None
    reason: str | None = None
    #: The first run of the chain the buyer's call started, pinned by the first verified
    #: observation. From then on only that chain counts: a later run of ``job-<id>`` (the
    #: seller's own start after terminating it) is ignored.
    run_id: str | None = None

    @property
    def open(self) -> bool:
        return self.state in _OPEN


def new_job(
    *,
    job_id: str,
    offering: str,
    buyer: str,
    seller: str,
    seller_ns: str,
    amount: Money,
    max_duration: timedelta,
    review_window: timedelta,
    now: datetime,
    grace: timedelta = DEFAULT_GRACE,
) -> JobRecord:
    """A job right after its hold was locked: ``submitted``, escrow ``held``."""
    return JobRecord(
        job_id=job_id,
        offering=offering,
        buyer=buyer,
        seller=seller,
        seller_ns=seller_ns,
        amount=amount,
        review_window=review_window,
        created_at=now,
        deadline=now + max_duration + grace,
    )


def _fail(
    job: JobRecord, reason: str, *, terminate: bool = False
) -> tuple[JobRecord, list[Effect]]:
    effects = [Effect(EffectKind.REFUND, job.job_id, reason)]
    if terminate:
        effects.insert(0, Effect(EffectKind.TERMINATE_WORKFLOW, job.job_id, reason))
    return replace(job, state=JobState.FAILED, escrow=Escrow.REFUNDED, reason=reason), effects


def _deliver(job: JobRecord, obs: Observation, now: datetime) -> tuple[JobRecord, list[Effect]]:
    delivered_at = obs.closed_at or now
    job = replace(
        job,
        state=JobState.COMPLETED,
        started_at=job.started_at or obs.started_at,
        delivered_at=delivered_at,
        review_deadline=delivered_at + job.review_window,
        delivery_sha256=obs.result_sha256,
        delivery_size=obs.result_size,
    )
    # A zero review window means "accept on delivery"; otherwise the window may already
    # have passed if the platform was down when the job completed.
    return tick(job, now)


def observe(job: JobRecord, obs: Observation, now: datetime) -> tuple[JobRecord, list[Effect]]:
    """Apply one observation of the seller's handler workflow.

    A run that is not the buyer's call (:class:`Provenance` other than ``VERIFIED``, or
    another run than the pinned one) moves nothing: the job waits for the buyer's call as
    if no run existed, and refunds at the deadline. A completed one is named in the reason,
    so the refund says what happened."""
    if not job.open:
        return job, []
    if obs.status is ObservedStatus.NOT_FOUND:
        if now >= job.deadline:
            gone = "the job's workflow is gone" if job.run_id else "seller never started the job"
            return _fail(job, gone)
        return job, []
    if job.run_id is not None and obs.run_id != job.run_id:
        return tick(job, now)
    if obs.provenance is not Provenance.VERIFIED or not obs.run_id:
        if now >= job.deadline:
            if obs.status is ObservedStatus.COMPLETED:
                return _fail(job, NOT_FROM_BUYER_REASON)
            return _fail(job, "seller workflow not started by the buyer's call")
        return job, []
    if job.run_id is None:
        job = replace(job, run_id=obs.run_id)
    match obs.status:
        case ObservedStatus.COMPLETED:
            if obs.closed_at is not None and obs.closed_at > job.deadline:
                return _fail(job, "delivered after max_duration")
            return _deliver(job, obs, now)
        case ObservedStatus.RUNNING:
            if now >= job.deadline:
                return _fail(job, "max_duration exceeded", terminate=True)
            if job.state is JobState.SUBMITTED:
                job = replace(job, state=JobState.WORKING, started_at=obs.started_at or now)
            return job, []
        case ObservedStatus.CANCELED:
            return (
                replace(
                    job,
                    state=JobState.CANCELED,
                    escrow=Escrow.REFUNDED,
                    reason=job.reason or "canceled",
                ),
                [Effect(EffectKind.REFUND, job.job_id, "canceled")],
            )
        case ObservedStatus.TIMED_OUT:
            return _fail(job, "seller workflow timed out")
        case ObservedStatus.TERMINATED:
            return _fail(job, "seller workflow terminated")
        case ObservedStatus.FAILED:
            return _fail(job, "seller workflow failed")


def tick(job: JobRecord, now: datetime) -> tuple[JobRecord, list[Effect]]:
    """Clock-driven transitions with no new observation: the review window closing, and
    the refund deadline of a job the observer cannot see."""
    if job.state is JobState.COMPLETED and job.escrow is Escrow.HELD:
        assert job.review_deadline is not None
        if now >= job.review_deadline:
            return replace(job, escrow=Escrow.RELEASED), [
                Effect(EffectKind.RELEASE, job.job_id, "review window closed")
            ]
    elif job.open and now >= job.deadline:
        return _fail(job, "max_duration exceeded", terminate=job.state is JobState.WORKING)
    return job, []


def _require_buyer(job: JobRecord, party: str) -> None:
    if party != job.buyer:
        raise EscrowError("forbidden", "only the buyer can do that")


def accept(job: JobRecord, party: str, now: datetime) -> tuple[JobRecord, list[Effect]]:
    _require_buyer(job, party)
    if job.state is not JobState.COMPLETED:
        raise EscrowError(
            "not_delivered", f"job is {job.state.value}; only a delivered job can be accepted"
        )
    if job.escrow is Escrow.RELEASED:
        return job, []  # idempotent
    if job.escrow is not Escrow.HELD:
        raise EscrowError("escrow_closed", f"escrow is {job.escrow.value}")
    return replace(job, escrow=Escrow.RELEASED), [
        Effect(EffectKind.RELEASE, job.job_id, "accepted")
    ]


def reject(
    job: JobRecord, party: str, reason: str, now: datetime
) -> tuple[JobRecord, list[Effect]]:
    _require_buyer(job, party)
    if job.state is not JobState.COMPLETED:
        raise EscrowError(
            "not_delivered", f"job is {job.state.value}; only a delivered job can be rejected"
        )
    if job.escrow is Escrow.DISPUTED:
        return job, []  # idempotent
    if job.escrow is not Escrow.HELD:
        raise EscrowError("escrow_closed", f"escrow is {job.escrow.value}")
    assert job.review_deadline is not None
    if now >= job.review_deadline:
        raise EscrowError(
            "window_closed", "the review window has closed; the delivery was accepted"
        )
    return replace(job, escrow=Escrow.DISPUTED, reason=reason), [
        Effect(EffectKind.DISPUTE, job.job_id, reason)
    ]


def cancel(job: JobRecord, party: str, now: datetime) -> tuple[JobRecord, list[Effect]]:
    """Buyer cancels before delivery: refund now (no partial payment, §2) and ask the
    seller's workflow to stop. A delivery that lands afterwards is ignored."""
    _require_buyer(job, party)
    if job.state is JobState.CANCELED:
        return job, []
    if not job.open:
        raise EscrowError("not_open", f"job is {job.state.value}")
    return (
        replace(job, state=JobState.CANCELED, escrow=Escrow.REFUNDED, reason="canceled by buyer"),
        [
            Effect(EffectKind.CANCEL_WORKFLOW, job.job_id, "canceled by buyer"),
            Effect(EffectKind.REFUND, job.job_id, "canceled by buyer"),
        ],
    )


def resolve(job: JobRecord, now: datetime) -> tuple[JobRecord, list[Effect]]:
    """The resolver of last resort decides a disputed job (§8.3). Who may resolve, and the
    split, are the platform's to check and record; the rule here is that only a dispute
    is resolved, and only once."""
    if job.escrow is Escrow.RESOLVED:
        return job, []  # idempotent
    if job.escrow is not Escrow.DISPUTED:
        raise EscrowError(
            "not_disputed", f"escrow is {job.escrow.value}; only a disputed job can be resolved"
        )
    return replace(job, escrow=Escrow.RESOLVED), [
        Effect(EffectKind.RESOLVE, job.job_id, "resolved")
    ]


_ESCROW_TO_HOLD = {
    Escrow.HELD: HoldState.HELD,
    Escrow.RELEASED: HoldState.RELEASED,
    Escrow.REFUNDED: HoldState.REFUNDED,
    Escrow.DISPUTED: HoldState.DISPUTED,
    Escrow.RESOLVED: HoldState.RESOLVED,
}


def reconcile(job: JobRecord, hold_state: HoldState | None) -> list[Effect]:
    """Ledger calls still owed: the job record says the escrow should be X, the ledger
    says Y. Re-issuing is safe because ledger calls are idempotent."""
    want = _ESCROW_TO_HOLD[job.escrow]
    if hold_state is want or hold_state is None:
        return []
    if want is HoldState.RESOLVED:
        # A resolution reaches the ledger through a dispute; the split comes with the
        # caller's record of the decision.
        resolve_ = Effect(EffectKind.RESOLVE, job.job_id, "reconcile")
        if hold_state is HoldState.DISPUTED:
            return [resolve_]
        if hold_state is HoldState.HELD:
            return [Effect(EffectKind.DISPUTE, job.job_id, "reconcile"), resolve_]
        return []
    if hold_state is HoldState.HELD:
        kind = {
            HoldState.RELEASED: EffectKind.RELEASE,
            HoldState.REFUNDED: EffectKind.REFUND,
            HoldState.DISPUTED: EffectKind.DISPUTE,
        }.get(want)
        return [Effect(kind, job.job_id, "reconcile")] if kind else []
    return []
