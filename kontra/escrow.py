"""The job lifecycle and escrow rules (PRD §8), as pure functions.

The gate (platform) and the fake gate (:mod:`kontra.testing`) both drive jobs with these
functions, so "who gets paid when" has one implementation, readable by every seller and
buyer. Nothing here does I/O: callers persist the returned :class:`JobRecord` and then
carry out the returned :class:`Effect` s (ledger calls, workflow cancellation). Ledger calls
are idempotent, so a crash between the two is repaired by :func:`reconcile` on the next
sweep.

Every input is a fact the platform can observe without reading a payload (§8.3): the
state of the job's run as the engine records it (an :class:`Observation`, which the gate
reads through seam ⑥, ``kontra.engine``: on Temporal, the seller's handler workflow
``job-<id>``), the clock, and the buyer's accept / reject / cancel calls. Nothing here knows
which engine ran the job: a run is named by its opaque :attr:`Observation.pin`, and a stop
is an :class:`EffectKind` the gate maps onto its engine (``kontra.engine.STOPS``).

Effects are carried out in the order given, and no ``REFUND`` is ever returned before the
fact that justifies it. A buyer's cancel of a job whose run was observed is the one rule
that needs a fact the call itself cannot have -- whether the run completed first, and its
delivery already reached the buyer's own start (on Temporal, its Nexus call) -- so it takes
two steps:
:func:`cancel` records the request and asks for the pinned run's termination
(:attr:`EffectKind.TERMINATE_RUN`), and the platform completes it by observing the pinned
chain afterwards and applying what it shows with :func:`observe`.

Only a run the buyer's own start began can deliver (decisions I15). A seller can start
the job's run itself, with any input, or stop the run the buyer's start began and complete a
later one; the observer establishes from records no party can write after the fact (on
Temporal, by lookup in the buyer's namespace) which run, if any, the buyer's start began
(:class:`Provenance`), escrow pins that run's lineage (:attr:`JobRecord.pin`) and ignores
every other run of the job.

A completion whose result is not Kontra ciphertext is never a delivery
(:attr:`ObservedStatus.UNSEALED`, decisions I32): end-to-end encryption is a guarantee of
seam ⑥, and an engine adapter withholds what is not sealed.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from enum import StrEnum

from kontra.gate_client.models import Escrow, JobState
from kontra.ledger import HoldState, Money

__all__ = [
    "BUYER_CANCEL_REASON",
    "DEFAULT_GRACE",
    "NOT_FROM_BUYER_REASON",
    "OCCUPIED_REASON",
    "UNSEALED_REASON",
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
#: Why a job the buyer canceled refunds: through the gate (:func:`cancel`), or by withdrawing
#: its own start (on Temporal, cancelling its Nexus call: :attr:`Observation.buyer_canceled`).
#: A gate counts neither against the seller.
BUYER_CANCEL_REASON = "canceled by buyer"
#: Why an open job whose ``job-<id>`` holds only the control plane's placeholder
#: (:attr:`Provenance.OCCUPIED`) refunds at its deadline: no call could start it.
OCCUPIED_REASON = "no run: the job's workflow id was occupied before any call started it"
#: Why a job whose verified run completed with a result that is not Kontra ciphertext
#: refunds (:attr:`ObservedStatus.UNSEALED`): counted against the seller, whose worker
#: bypassed the codec; the platform never stores those bytes.
UNSEALED_REASON = "delivery not sealed"


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
    #: A completion whose result is not Kontra ciphertext (decisions I32): the adapter
    #: withheld its bytes, and it is never a delivery (:data:`UNSEALED_REASON`).
    UNSEALED = "unsealed"


class Provenance(StrEnum):
    """Whether the observed run of the job is the one the buyer's start began, as an
    authoritative lookup established it (on Temporal: the buyer's ``execute`` call, looked up
    in the buyer's namespace). Only
    ``VERIFIED`` runs count for anything; the others differ only in what the observer could
    tell (logs, support, the reason a refund gives), never in whether escrow pays."""

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
    #: The control plane's placeholder (``kontra._control.Control.occupy_job``): a cancel
    #: took ``job-<id>`` before any run existed, so that the hold token -- valid until it
    #: expires, and checked offline -- can no longer start the job. Never a delivery and
    #: never pinned. It is recognised by the fields it sets, which any start request can
    #: set too, so a seller can start a look-alike: an open job that shows only it refunds
    #: at the deadline (:data:`OCCUPIED_REASON`) and counts like any refund at the deadline,
    #: or the label would excuse a seller that never started the job. (A cancel through the
    #: gate that occupied the id refunds at once, as the buyer's cancel.)
    OCCUPIED = "occupied"


@dataclass(frozen=True, slots=True)
class Observation:
    """What the platform saw of the job's run as the engine records it: the latest run of
    the lineage the buyer's start began once escrow has pinned it, else the latest run of
    the job (on Temporal: ``job-<id>`` in the seller's namespace, a lineage being a
    continue-as-new chain).

    The default ``provenance`` is ``UNVERIFIED`` on purpose: an observer that does not
    establish provenance gets no delivery, never an unchecked one."""

    status: ObservedStatus
    started_at: datetime | None = None
    closed_at: datetime | None = None
    #: sha256 / size of the completion's result payload as stored (ciphertext).
    result_sha256: str | None = None
    result_size: int | None = None
    provenance: Provenance = Provenance.UNVERIFIED
    #: The observed lineage, as the engine adapter names it (opaque; on Temporal, the run id
    #: of the chain's first run): what escrow pins once it is verified, and compares for
    #: equality, nothing more.
    pin: str | None = None
    #: A verified run that ended ``CANCELED``: the buyer's own call recorded that the buyer
    #: asked to cancel it (its standalone operation's cancellation, or a
    #: ``NexusOperationCancelRequested`` event for it in the buyer's workflow), read from the
    #: buyer's namespace by exact equality. The refund is then the buyer's doing
    #: (:data:`BUYER_CANCEL_REASON`), not the seller's failure.
    buyer_canceled: bool = False


class EffectKind(StrEnum):
    RELEASE = "release"
    REFUND = "refund"
    DISPUTE = "dispute"
    CANCEL_WORKFLOW = "cancel_workflow"
    TERMINATE_WORKFLOW = "terminate_workflow"
    #: Step one of a buyer's cancel of a job with an observed run (:func:`cancel`):
    #: terminate the open run of the pinned chain (:attr:`JobRecord.pin`), then observe
    #: that chain and apply the observation (:func:`observe`) -- the refund waits for it.
    #: A cancel *request* would not do: a run that ignores it, or completes first, delivers
    #: to the buyer's own call, and the buyer would have both the result and the money.
    #: Termination ends a chain for good, so once it landed the run can deliver nothing;
    #: a run that completed before it did is a delivery and the cancel is refused.
    #: Re-issued by every observation that still finds the run open.
    TERMINATE_RUN = "terminate_run"
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
    #: The lineage the buyer's start began (:attr:`Observation.pin`, opaque), pinned by the
    #: first verified observation. From then on only that lineage counts: a later run of
    #: the job (the seller's own start after terminating it) is ignored.
    pin: str | None = None
    #: When the buyer asked to cancel a job whose run was observed (:func:`cancel`). The
    #: job stays open until an observation of the pinned chain decides it: terminated (the
    #: platform's doing, for this cancel) or canceled refunds as the buyer's cancel;
    #: completed first is a delivery. Recorded *before* the termination is attempted, so
    #: the termination is never mistaken for the seller's own failure.
    cancel_requested_at: datetime | None = None

    @property
    def open(self) -> bool:
        return self.state in _OPEN


def new_job(
    *,
    job_id: str,
    offering: str,
    buyer: str,
    seller: str,
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


def _buyer_canceled(
    job: JobRecord, *, stop: EffectKind | None = None
) -> tuple[JobRecord, list[Effect]]:
    """Close a job the buyer canceled: refunded, with the reason no gate counts against
    the seller. ``stop``: a workflow stop to issue first."""
    effects = [Effect(EffectKind.REFUND, job.job_id, BUYER_CANCEL_REASON)]
    if stop is not None:
        effects.insert(0, Effect(stop, job.job_id, BUYER_CANCEL_REASON))
    return (
        replace(job, state=JobState.CANCELED, escrow=Escrow.REFUNDED, reason=BUYER_CANCEL_REASON),
        effects,
    )


def _past_deadline(job: JobRecord, *, running: bool) -> tuple[JobRecord, list[Effect]]:
    """An open job at its refund deadline: refunded, a ``running`` run terminated. A job
    the buyer had asked to cancel stays the buyer's cancel: its run outlived the
    termination only because the platform could not reach Temporal, which is not the
    seller's failure. No run delivers after the deadline: the buyer's call and the
    handler's run both end by it (decisions I16)."""
    if job.cancel_requested_at is not None:
        return _buyer_canceled(job, stop=EffectKind.TERMINATE_WORKFLOW if running else None)
    return _fail(job, "max_duration exceeded", terminate=running)


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
    """Apply one observation of the job's run.

    A run that is not the buyer's call (:class:`Provenance` other than ``VERIFIED``, or
    another run than the pinned one) moves nothing: the job waits for the buyer's call as
    if no run existed, and refunds at the deadline. A completed one is named in the reason,
    so the refund says what happened.

    With a cancel requested (:attr:`JobRecord.cancel_requested_at`), this is step two of
    :func:`cancel`: the pinned chain terminated or canceled refunds as the buyer's cancel,
    completed is a delivery (it beat the termination), failed or timed out refunds as it
    would have anyway (it closed on its own first), and still running asks for the
    termination again.

    An :attr:`ObservedStatus.UNSEALED` completion of the pinned lineage refunds at once, as
    the seller's failure (:data:`UNSEALED_REASON`): its result is not ciphertext, so it is no
    delivery whatever the cancel or the clock say."""
    if not job.open:
        return job, []
    if obs.status is ObservedStatus.NOT_FOUND:
        if now >= job.deadline:
            gone = "the job's workflow is gone" if job.pin else "seller never started the job"
            return _fail(job, gone)
        return job, []
    if job.pin is not None and obs.pin != job.pin:
        return tick(job, now)
    if obs.provenance is not Provenance.VERIFIED or not obs.pin:
        if now >= job.deadline:
            if obs.provenance is Provenance.OCCUPIED:
                return _fail(job, OCCUPIED_REASON)
            if obs.status is ObservedStatus.COMPLETED:
                return _fail(job, NOT_FROM_BUYER_REASON)
            return _fail(job, "seller workflow not started by the buyer's call")
        return job, []
    if job.pin is None:
        job = replace(job, pin=obs.pin)
    match obs.status:
        case ObservedStatus.UNSEALED:
            return _fail(job, UNSEALED_REASON)
        case ObservedStatus.COMPLETED:
            if obs.closed_at is not None and obs.closed_at > job.deadline:
                return _fail(job, "delivered after max_duration")
            return _deliver(job, obs, now)
        case ObservedStatus.RUNNING:
            if now >= job.deadline:
                return _past_deadline(job, running=True)
            if job.state is JobState.SUBMITTED:
                job = replace(job, state=JobState.WORKING, started_at=obs.started_at or now)
            if job.cancel_requested_at is not None:
                return job, [Effect(EffectKind.TERMINATE_RUN, job.job_id, BUYER_CANCEL_REASON)]
            return job, []
        case ObservedStatus.CANCELED:
            # Who asked is in the buyer's records (Observation.buyer_canceled): a buyer that
            # cancels its own call must not have the refund counted against the seller.
            asked = obs.buyer_canceled or job.cancel_requested_at is not None
            why = BUYER_CANCEL_REASON if asked else "canceled"
            return (
                replace(
                    job,
                    state=JobState.CANCELED,
                    escrow=Escrow.REFUNDED,
                    reason=job.reason or why,
                ),
                [Effect(EffectKind.REFUND, job.job_id, why)],
            )
        case ObservedStatus.TIMED_OUT:
            return _fail(job, "seller workflow timed out")
        case ObservedStatus.TERMINATED:
            if job.cancel_requested_at is not None:
                return _buyer_canceled(job)  # the termination step one asked for
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
        return _past_deadline(job, running=job.state is JobState.WORKING)
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
    """The buyer cancels before delivery; nothing is paid for a canceled job (§2).

    A job with no observed run (nothing pinned) refunds at once and any run of the id is
    asked to stop: none is the buyer's call, so none can deliver to it. (The platform
    first closes ``job-<id>`` to late starts, decisions I16.)

    A job with an observed run takes two steps, because that run can complete at any
    moment and its delivery reaches the buyer's own start whatever escrow says: a
    refund issued first would let a buyer cancel a nearly finished job and keep both the
    result and the money. Step one, here: the request is recorded
    (:attr:`JobRecord.cancel_requested_at`), the job stays open, and the only effect is
    :attr:`EffectKind.TERMINATE_RUN`. Step two is the platform's: once the termination was
    attempted it observes the pinned chain and applies that with :func:`observe` --
    terminated refunds as the buyer's cancel; completed first is a delivery, and the
    cancel is then refused like any cancel of a delivered job. Asking again while step two
    is pending asks for the termination again."""
    _require_buyer(job, party)
    if job.state is JobState.CANCELED:
        return job, []
    if not job.open:
        raise EscrowError("not_open", f"job is {job.state.value}")
    if job.pin is None:
        return _buyer_canceled(job, stop=EffectKind.CANCEL_WORKFLOW)
    requested = replace(job, cancel_requested_at=job.cancel_requested_at or now)
    return requested, [Effect(EffectKind.TERMINATE_RUN, job.job_id, BUYER_CANCEL_REASON)]


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
