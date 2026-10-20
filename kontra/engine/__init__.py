"""Seam ⑥: the gate<->engine Protocol (decisions I32).

::

    seller <-> engine : native, no abstraction (I29; the facade is never rebuilt here)
    gate   <-> engine : this Protocol

The gate reaches the engine an offering version was published on (``PublishRequest.engine``)
only through :class:`Engine`. An adapter implements it on one engine --
:mod:`kontra.engine.temporal` is the only one -- and every adapter passes
:mod:`kontra.testing.engine_contract` unchanged, as every ledger passes the ledger contract
(seam ④). :class:`kontra.testing.FakeEngine` is the in-memory one.

Nothing here names an engine's own concepts: no namespace, task queue, run id, Nexus endpoint
or history appears in a type or a parameter (the Protocol was designed against Golem, a Wasm
engine, as well as Temporal). The one exemption is not this seam's type: the catalog's
:class:`~kontra.contract.names.OfferingId` that :meth:`Engine.expose`, :meth:`Engine.quote` and
:class:`JobPlan` take carries ``namespace``, ``task_queue`` and ``service_name``, which are
:mod:`kontra.contract.names` derivations of the offering (constructed, never parsed) that only
the Temporal adapter reads; another adapter ignores them.

A job is a :data:`JobHandle` the adapter minted, the lineage escrow pins is a :data:`Pin`, a
party's place in the engine is an :class:`Isolation`, and content leaves an adapter only as
:class:`SealedPayload` (a delivery) or :class:`Sealed` (a relayed stream item): ciphertext,
checked when the value is built. End-to-end encryption is a guarantee of this seam (I7), not
an engine feature: the seller side of the engine seals (Temporal: ``KontraCodec`` under
``KontraPlugin``), and an adapter verifies, and withholds what is not sealed
(:attr:`~kontra.escrow.ObservedStatus.UNSEALED`). The one plaintext that crosses is a quote's
answer: a price (I4).

A handle is opaque bytes. Only the adapter that minted it parses it, strictly and before any
I/O (:class:`InvalidHandle`); it is never shown to a party and never used to authorize
(provenance compares engine records with the gate's own ``holds``).

An observation also says how far the buyer's start got (:class:`Reach`, decisions I40), so a
gate can tell whether a job that never ran is its seller's fault without an engine call of
its own. It is reputation only: escrow is handed the :class:`~kontra.escrow.Observation`
alone, and a reach the engine cannot read is None, never an error.

The guardrail: this Protocol never grows a workflow-authoring member. Every member is a call a
gate already makes to its engine; none runs, defines or schedules seller code. The day someone
wants ``engine.execute_activity(...)``, that is the facade crawling back, and the answer is no.
:data:`MEMBERS` pins the member set and a test fails on any change.

This module imports nothing from ``temporalio`` (a test checks it in a subprocess);
:class:`SealedPayload` reads the codec's envelope lazily.
"""

from __future__ import annotations

import re
from collections.abc import AsyncIterator, Awaitable, Callable, Collection, Mapping
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from hashlib import sha256
from typing import Final, Literal, NewType, Protocol, final, runtime_checkable

from kontra.contract.names import OfferingId, is_handle, is_job_id
from kontra.escrow import EffectKind, Observation, ObservedStatus, Provenance
from kontra.gate_client.models import DEFAULT_ENGINE, ENGINE_NAMES, EngineName

__all__ = [
    "AGE_MAGIC",
    "DEFAULT_ENGINE",
    "ENGINE_NAMES",
    "HANDLE_MAX",
    "MEMBERS",
    "PIN_MAX",
    "STOPS",
    "BuyerStart",
    "Engine",
    "EngineError",
    "EngineName",
    "Fenced",
    "InvalidHandle",
    "Isolation",
    "JobHandle",
    "JobPlan",
    "Liveness",
    "NotSealed",
    "ObservationIncomplete",
    "Observed",
    "PartyLookupError",
    "Pin",
    "QuoteAnswer",
    "QuoteKind",
    "Reach",
    "RouteConflict",
    "Sealed",
    "SealedPayload",
    "Stop",
    "StopReason",
    "StreamBatch",
    "StreamItem",
    "StreamStopped",
    "hold_digest",
]

#: Opaque bytes an adapter minted for one job (:meth:`Engine.bind`). The gate stores them on
#: the job row and hands them back verbatim; nothing outside the adapter reads them.
JobHandle = NewType("JobHandle", bytes)
#: Opaque text an adapter minted for the lineage a buyer's start began: what escrow pins
#: (:attr:`~kontra.escrow.JobRecord.pin`) and compares for equality, nothing more. Printable
#: ASCII, at most :data:`PIN_MAX` characters.
Pin = NewType("Pin", str)

#: The largest handle an adapter may mint (the gate stores it on every job row).
HANDLE_MAX: Final = 1024
PIN_MAX: Final = 128
#: Every age v1 file starts with this line.
AGE_MAGIC: Final = b"age-encryption.org/v1\n"

_PIN = re.compile(rf"[\x21-\x7e]{{1,{PIN_MAX}}}", re.ASCII)
#: What HTTP drops from both ends of a header value on its way to the seller's code (server
#: 1.32, measured on Temporal; the hold travels in one).
_HTTP_OWS = " \t"


def hold_digest(token: str) -> str:
    """How a hold is matched (I15): sha256 hex of the bytes its signature covers (the JWS
    signing input, everything before the last ``.``; all of it when there is none), after
    dropping the spaces and tabs HTTP drops on the way to the seller's code.

    The gate digests each hold it issues with this and passes the digests to
    :meth:`Engine.observe` (``holds``); an adapter digests what the buyer's start carried with
    it. The hold is the gate's signed statement of a job (I10), the same on every engine. The
    signature segment is left out because it is not the gate's to fix: ES256 accepts
    ``(r, n - s)`` for ``(r, s)`` and the verifier's base64url decoder ignores padding, so one
    issued token has many spellings the seller's code accepts. Matched byte for byte, a buyer
    could start with one of them, have the job done, and have escrow refund it as "not the
    buyer's start". A string with the gate's signed part is the gate's statement whatever
    signature follows it. The token is never decoded: no claim is read, only bytes compared.
    (``kontra._control`` re-exports it.)"""
    value = token.strip(_HTTP_OWS).encode("utf-8")
    signed, dot, _signature = value.rpartition(b".")
    return sha256(signed if dot else value).hexdigest()


# ----------------------------------------------------------------------------- errors


class EngineError(Exception):
    """The engine could not be asked, or could not say. Never a fact about the job: the
    caller asks again (the sweeper), and a buyer's cancel is refused meanwhile (I17)."""


class PartyLookupError(EngineError):
    """The seller's side was read but the buyer's records could not be (I16). The buyer
    controls their load: a gate must never give up into a deadline refund on this error, or a
    buyer could take a delivery and have the job refunded by keeping its records busy."""


class ObservationIncomplete(EngineError):
    """The pinned lineage is longer than one observation's budget; the next call continues.
    Reporting it as still running instead would let a buyer cancel after a delivery."""


class RouteConflict(EngineError):
    """:meth:`Engine.expose` for a route that exists with another target. A route is never
    repointed."""


class InvalidHandle(ValueError):
    """Not a handle this adapter minted. Refused before any I/O; never acted on."""


class NotSealed(ValueError):
    """Bytes an adapter was about to hand the gate are not Kontra ciphertext."""


# ----------------------------------------------------------------------------- content


@final
@dataclass(frozen=True, slots=True)
class Sealed:
    """One age v1 file: a relayed stream item, sealed by the seller side of the engine per
    item (decisions I31). Checked on construction, structurally
    (:func:`kontra.codec.age.is_age_file`, imported lazily: the version line, a recipient
    stanza, the header MAC, the nonce, a sealed chunk): anything else is
    :class:`NotSealed`."""

    data: bytes

    def __post_init__(self) -> None:
        if not isinstance(self.data, bytes) or not self.data.startswith(AGE_MAGIC):  # pyright: ignore[reportUnnecessaryIsInstance]
            raise NotSealed("a stream item must be an age v1 file")
        from kontra.codec.age import is_age_file

        if not is_age_file(self.data):
            raise NotSealed("a stream item must be an age v1 file, not its first line")


@final
@dataclass(frozen=True, slots=True)
class SealedPayload:
    """A delivery: one serialized Kontra payload envelope, ``binary/kontra-age`` (``kontra-v``
    1, its data an age v1 file) or ``binary/kontra-claim-check`` (a reference to ciphertext in
    the blob store), in canonical serialization. Checked on construction by
    :func:`kontra.codec.check_sealed` (imported lazily): anything else is :class:`NotSealed`.

    The envelope is Kontra's codec format (I7). That its container is the protobuf message
    ``temporal.api.common.v1.Payload`` is a format fact, not an engine call: every adapter
    hands over the same envelope, so a buyer opens every delivery one way."""

    data: bytes

    def __post_init__(self) -> None:
        if not isinstance(self.data, bytes):  # pyright: ignore[reportUnnecessaryIsInstance]
            raise NotSealed("a delivery is bytes")
        from kontra.codec import check_sealed

        check_sealed(self.data)


# ----------------------------------------------------------------------------- inputs / outputs


@dataclass(frozen=True, slots=True)
class JobPlan:
    """What the gate knows of a job at hire, before anything runs: the input of
    :meth:`Engine.bind`. ``route`` is the catalog's opaque route name of the version hired
    (what :meth:`Engine.expose` made reachable); ``buyer`` and ``seller`` are party handles,
    and ``offering`` is the seller's."""

    job_id: str
    offering: OfferingId
    route: str
    buyer: str
    seller: str

    def __post_init__(self) -> None:
        if not is_job_id(self.job_id):
            raise ValueError("not a job id")
        if not (is_handle(self.buyer) and is_handle(self.seller)):
            raise ValueError("buyer and seller are party handles")
        if self.offering.party != self.seller:
            raise ValueError("the offering is not the seller's")


@dataclass(frozen=True, slots=True)
class Isolation:
    """A party's place in an engine (:meth:`Engine.isolate`). ``home`` and ``address`` are
    shown to that party only (its worker connects there); ``credential`` reaches that home and
    nothing else, and is revocable by the gate session it was minted for."""

    home: str
    address: str
    credential: str | None = None
    expires_at: datetime | None = None


class Stop(StrEnum):
    """How :meth:`Engine.cancel` stops a job."""

    #: Ask the run to stop: the seller's code may clean up, and may still complete. No refund
    #: ever waits on an ASK.
    ASK = "ask"
    #: End it for good. Once :meth:`Engine.cancel` returns, the ended lineage can deliver
    #: nothing, and no party's credential can revive it.
    END = "end"


class Fenced(StrEnum):
    """What :meth:`Engine.fence` found."""

    #: No later start of the job, the gate's or a party's own, can deliver.
    CLOSED = "closed"
    #: A run exists already: observe it, then stop it as a running job.
    STARTED = "started"


class Reach(StrEnum):
    """How far the buyer's start of a job got toward the seller's code (decisions I40): what a
    gate's reputation reads for a job that ended with no verified lineage, to tell whether its
    refund counts against the seller (kontra-cloud's FIXUPS #22). Reputation only: escrow never
    reads it, and no money moves on it.

    Read from what the engine recorded of the buyer's start, never from a party's report, and
    a fact, not a judgement: whether an :attr:`UNANSWERED` start waited long enough to blame
    the seller is the gate's to decide, from :class:`BuyerStart`'s times. Only
    :attr:`STARTED`, :attr:`FAILED` and a long enough :attr:`UNANSWERED` can cost the seller
    anything; :attr:`Observed.reach` is None when the engine cannot say, which costs no one."""

    #: No start of this job is seen to have been handed to the seller's side: none was made
    #: where the engine can see it (on Temporal, a buyer workflow's own call is not seen), none
    #: is this job's (its route, its offering, one of its holds), or none of its attempts to
    #: reach the seller's code has finished yet. A run the seller started itself is no start of
    #: the buyer's: a job with only such a run is NEVER_DISPATCHED.
    NEVER_DISPATCHED = "never_dispatched"
    #: The buyer withdrew its start before the seller's side took it: it ended the start, or
    #: asked to cancel it and the start ended canceled.
    WITHDRAWN = "withdrawn"
    #: The start was handed over and nothing on the seller's side answered it: no worker of the
    #: seller's took it, or the seller's code took longer than one attempt (an engine cannot
    #: tell the two apart; on Temporal both are an upstream timeout).
    UNANSWERED = "unanswered"
    #: The seller's code refused the start before anything ran (its hold, its input or its
    #: price), or ended it canceled though the buyer never asked.
    REFUSED = "refused"
    #: The seller's code failed the start and declared the failure final: its own.
    FAILED = "failed"
    #: The start reached the seller's code and failed in a way either side, or the platform,
    #: may have caused (an input the buyer never uploaded, a store that did not answer).
    INCONCLUSIVE = "inconclusive"
    #: The seller's side took the start: a lineage began (every verified observation's reach),
    #: or the seller's code accepted the start and none of its runs verified.
    STARTED = "started"


@dataclass(frozen=True, slots=True)
class BuyerStart:
    """The buyer's start of a job, as the engine recorded it, beside :attr:`Observed.reach`
    (decisions I40): what a gate reads to judge an :attr:`Reach.UNANSWERED` start (did it wait
    long enough for the seller to have been away, not restarting?) and to word a refund the
    seller is not blamed for (did the buyer withdraw it, or never start?). The buyer's own
    client chooses both times for a start it makes itself.

    ``made_at``: when the start was made (None: not recorded; such a start never waited).
    ``gives_up_at``: when it stops waiting for the seller's side (None: never).
    ``withdrawal_asked``: the buyer asked to withdraw it (only the buyer can; always False
    where buyers have no presence in the engine). A request is not a withdrawal: a start the
    buyer asked to cancel that nothing took stays live, and still waits."""

    made_at: datetime | None = None
    gives_up_at: datetime | None = None
    withdrawal_asked: bool = False


@dataclass(frozen=True, slots=True)
class Observed:
    """One observation (:meth:`Engine.observe`). ``observation`` is escrow's own type.
    ``result`` is set for a verified completion, and only for one: escrow pays a verified
    completion as a delivery, so one without its sealed bytes cannot be built (an adapter
    with no bytes reports UNSEALED, or raises :class:`EngineError`). The observation's
    ``result_sha256`` and ``result_size`` are over exactly ``result.data``, and absent
    without a result. Checked on construction, so an adapter cannot hand over a delivery
    escrow would not count, or count one it did not get.

    ``reach`` (decisions I40): how far the buyer's start got, for reputation only -- escrow
    is handed ``observation`` and never sees it. None: the engine cannot say (the buyer's
    records could not be read), which blames no one; never an exception, so a failed read for
    reputation never holds up money. A verified lineage was begun by the buyer's start, so its
    reach is STARTED (or None); an observation of a pinned lineage likewise. ``buyer_start``:
    the start's own record, when the engine found one (never without a reach)."""

    observation: Observation
    result: SealedPayload | None = None
    reach: Reach | None = None
    buyer_start: BuyerStart | None = None

    def __post_init__(self) -> None:
        o = self.observation
        if o.pin is not None and _PIN.fullmatch(o.pin) is None:
            raise ValueError(f"a pin is printable ASCII, at most {PIN_MAX} characters")
        if self.reach is not None and not isinstance(self.reach, Reach):  # pyright: ignore[reportUnnecessaryIsInstance]
            raise ValueError("a reach is a Reach")
        if self.buyer_start is not None and self.reach is None:
            raise ValueError("a buyer's start record comes with its reach")
        if o.provenance is Provenance.VERIFIED and self.reach not in (Reach.STARTED, None):
            raise ValueError("a verified lineage was begun by the buyer's start: STARTED")
        delivered = o.status is ObservedStatus.COMPLETED and o.provenance is Provenance.VERIFIED
        if self.result is None:
            if delivered:
                raise ValueError("a verified completion carries its result (else: UNSEALED)")
            if o.result_sha256 is not None or o.result_size is not None:
                raise ValueError("a digest without a result: of bytes nobody handed over")
            return
        if not delivered:
            raise ValueError("only a verified completion carries a result")
        if (o.result_sha256, o.result_size) != (
            sha256(self.result.data).hexdigest(),
            len(self.result.data),
        ):
            raise ValueError("the result's digest and size are not of the bytes handed over")


@dataclass(frozen=True, slots=True)
class Liveness:
    """Platform-observable progress of a pinned lineage (:meth:`Engine.liveness`), from engine
    metadata only, never a payload. The gate maps it onto the buyer's stream ``history``
    event, whose field names stay as they are on the wire (``last_event``,
    ``activities_completed``, ``activities_running``, ``continue_as_new_count``)."""

    state: ObservedStatus
    #: What happened last (Temporal: the last event type that is not workflow-task
    #: bookkeeping).
    last_step: str | None = None
    #: Steps completed across the lineage (Temporal: activities).
    steps_done: int = 0
    #: The steps running now, by name (Temporal: pending activity types).
    steps_running: tuple[str, ...] = ()
    #: Restarts within the lineage (Temporal: continue-as-new).
    restarts: int = 0


class QuoteKind(StrEnum):
    #: The seller's code answered: ``document`` is its answer.
    OK = "ok"
    #: The seller's code refused or crashed; ``message`` says why (logs only).
    REFUSED = "refused"
    #: Something is serving the offering, but no answer came in time.
    TIMEOUT = "timeout"
    #: Nobody can answer: nothing serves the offering.
    UNAVAILABLE = "unavailable"


@dataclass(frozen=True, slots=True)
class QuoteAnswer:
    """The answer of :meth:`Engine.quote`. ``document``: the seller's ``KontraQuote`` JSON,
    the one plaintext that crosses this seam (a price, I4); :mod:`kontra.contract` validates
    it, the adapter does not interpret it."""

    kind: QuoteKind
    document: bytes | None = None
    message: str = ""


@dataclass(frozen=True, slots=True)
class StreamItem:
    """One relayed item: a topic the buyer may read (in the clear, I31), its offset in the
    job's stream, and its ciphertext -- or None for an item too large to relay (``size`` says
    how large: the gate stores a gap)."""

    topic: str
    offset: int
    sealed: Sealed | None
    size: int


@dataclass(frozen=True, slots=True)
class StreamBatch:
    """One answer of the relay. The gate stores ``items``, then persists ``next_offset`` (the
    at-least-once boundary) and passes it as ``after`` when it relays the job again."""

    items: tuple[StreamItem, ...]
    next_offset: int
    #: Items the engine held that the buyer may not read (or were not asked for): dropped by
    #: the adapter, as defence in depth. A count, for the gate's metrics.
    dropped: int = 0


#: Why :meth:`Engine.stream` stopped: the job's budget for being relayed is spent, the engine
#: answered something that is not a stream, the engine refused the relay's credential, or the
#: adapter relays no streams (an engine without them).
StopReason = Literal["budget", "protocol", "denied", "unsupported"]


class StreamStopped(Exception):
    """The relay cannot go on; ``reason`` says why (:data:`StopReason`)."""

    def __init__(self, reason: StopReason, message: str = "") -> None:
        super().__init__(f"{reason}: {message}" if message else reason)
        self.reason: StopReason = reason


# ----------------------------------------------------------------------------- the Protocol


@runtime_checkable
class Engine(Protocol):
    """Seam ⑥. Every member is something a gate already does to its engine; none authors,
    runs or schedules seller code (I32). The "Temporal:" line of each says what
    :mod:`kontra.engine.temporal` does."""

    @property
    def name(self) -> EngineName:
        """The engine's name: what ``PublishRequest.engine`` says, what the gate dispatches
        by."""
        ...

    # -- parties ----------------------------------------------------------------------

    async def isolate(self, party: str, *, session: str | None = None) -> Isolation:
        """Provision ``party``'s home in this engine if it is not there yet (idempotent;
        concurrent callers converge; nothing an operator set is lowered). With ``session``
        (the gate session that can revoke it), also mint a credential confined to that home:
        it reaches the party's own jobs and nothing of any other party's, and holds no control
        power -- no fence, no route, no provisioning, and no stop of a lineage the party does
        not own. Untrusted code runs with it. Raises :class:`EngineError`, and ValueError for
        a ``party`` that is not a party handle.

        Temporal: the party's namespace (``ensure_namespace``, retention per I15) and a worker
        JWT ``{sub: party, sid: session, permissions: ["<party>:worker"], aud:
        "kontra:temporal"}`` that the authorizer's party rule judges (I3, I30)."""
        ...

    # -- offerings --------------------------------------------------------------------

    async def expose(self, offering: OfferingId, route: str) -> None:
        """Make ``offering`` reachable under ``route``, targeting its seller's home, at
        publish. ``route`` is the catalog's opaque name: nothing derives anything from it
        (I1). Idempotent for the same target; a route is never repointed
        (:class:`RouteConflict`). Raises :class:`EngineError`.

        Temporal: the Nexus endpoint (``create_endpoint``)."""
        ...

    async def quote(
        self,
        offering: OfferingId,
        input: bytes,
        *,
        timeout: float,  # noqa: ASYNC109 -- the call's own timeout, enforced by the adapter
    ) -> QuoteAnswer:
        """Ask the seller's code for a price. ``input`` is the buyer's serialized payload (a
        ``WirePayload``'s bytes), relayed byte for byte. Never raises for the seller's own
        failure (:attr:`QuoteKind.REFUSED` / ``TIMEOUT`` / ``UNAVAILABLE``);
        :class:`EngineError` when the engine itself cannot be asked, ValueError for an
        ``input`` that is not a payload.

        Temporal: a synchronous Nexus call over the frontend's HTTP API (``nexus_sync``)."""
        ...

    # -- jobs -------------------------------------------------------------------------

    def bind(self, plan: JobPlan) -> JobHandle:
        """The job's handle, before anything runs. Pure (no I/O) and deterministic: the same
        plan gives the same bytes in any process. At most :data:`HANDLE_MAX` bytes. The gate
        stores it at hire, because a job may start without the gate (Temporal: the buyer's
        own Nexus call, I4) and must be observable from its first moment. ValueError for a
        plan the adapter cannot run.

        Temporal: ``temporal/1`` and the plan's fields, canonical JSON."""
        ...

    async def start(
        self, handle: JobHandle, input: bytes, *, hold: str, deadline: datetime
    ) -> None:
        """Durably start the job on the buyer's behalf, with the buyer's serialized payload
        ``input`` (relayed byte for byte) and the ``hold`` token the seller's code verifies
        (I10). The engine gives up on the start by ``deadline`` (the job's refund deadline).
        Idempotent by job: any number of starts, concurrent or after the run closed, begin at
        most one lineage, and a fenced job begins none (refusing or ignoring are both
        correct). Raises :class:`EngineError` (the start may or may not have happened:
        repeat it) and :class:`InvalidHandle`.

        Temporal: a standalone Nexus operation in the buyer's namespace, id ``job-<id>``
        (``start_operation``, I4)."""
        ...

    async def observe(
        self, handle: JobHandle, *, pin: Pin | None, holds: Collection[str]
    ) -> Observed:
        """What escrow needs (:func:`kontra.escrow.observe`), read only from what the engine
        records, never from a party's report:

        * status and times;
        * provenance: VERIFIED only when the lineage was begun by a start carrying one of
          ``holds`` (the :func:`hold_digest` of every hold the gate issued for the job),
          established from records no party can write after the fact (I15);
        * the lineage's ``pin`` once verified;
        * ``buyer_canceled``: the buyer withdrew its own start (I16; always False where
          buyers have no presence in the engine);
        * for a verified completion only, the result as :class:`SealedPayload`, with its
          sha256 and size;
        * for reputation only, how far the buyer's start got (:class:`Reach`, decisions I40):
          STARTED for a verified or pinned lineage, else read from the engine's records of
          the buyer's start of the job (:class:`BuyerStart`), never from the job's runs.

        Before any start the status is NOT_FOUND, never an exception and never RUNNING. With
        ``pin``, the observation is of that lineage only, never of another run of the job. A
        verified completion whose result is not Kontra ciphertext, or that has none, is
        reported UNSEALED and its bytes are withheld (:class:`Observed` refuses a verified
        completion without its result). "Cannot say" is an exception, never a status:
        :class:`EngineError`, :class:`PartyLookupError`, :class:`ObservationIncomplete` --
        except for ``reach``, whose "cannot say" is None: a read for reputation never fails an
        observation escrow needs.

        Temporal: ``Control.observe`` (describe; the request id, callbacks and links resolved
        in the buyer's namespace; the first run id; the chain walk); ``reach`` from
        ``Control.describe_call`` (the standalone Nexus operation ``job-<id>`` in the buyer's
        namespace, described)."""
        ...

    async def liveness(self, handle: JobHandle, *, pin: Pin) -> Liveness | None:
        """Progress of the pinned lineage only, from metadata; None when it is gone. A run
        outside the pin never feeds a buyer's progress. Raises :class:`EngineError`.

        Temporal: ``Control.history_summary(pinned=pin)``."""
        ...

    async def cancel(self, handle: JobHandle, *, stop: Stop, pin: Pin | None, reason: str) -> None:
        """Stop the job: the pinned lineage only with ``pin``, every run of the job without
        it. ASK asks; END ends for good (:class:`Stop`). Gone or already closed is success,
        and a delivery that completed first stands: escrow learns which by observing
        afterwards (I17). Raises :class:`EngineError`.

        Temporal: ASK is RequestCancelWorkflowExecution, END TerminateWorkflowExecution, each
        with ``first_execution_run_id = pin``. Reset, which would revive a terminated run, is
        denied to every token by the authorizer (I30)."""
        ...

    async def fence(self, handle: JobHandle, *, reason: str) -> Fenced:
        """Close the job to starts before any run exists (a cancel before start, I16). It
        never stops a run: a run that may be the buyer's genuine one must be observed (and
        pinned) before anything stops it. Idempotent. Raises :class:`EngineError` when it
        cannot tell, never a guess.

        Temporal: ``occupy_job`` (the placeholder under ``job-<id>``)."""
        ...

    def stream(
        self,
        handle: JobHandle,
        *,
        pin: Pin,
        topics: Collection[str],
        after: int | Literal["tail"],
        closed: Callable[[], Awaitable[bool]],
        session: str,
    ) -> AsyncIterator[StreamBatch]:
        """Relay the pinned lineage's ``topics`` (the version's streams and
        ``kontra.progress``, I31) from ``after``: the gate's persisted offset (0 on a job's
        first relay), or "tail" for only what comes next. Batches of
        (topic, offset, :class:`Sealed`), until ``closed()`` -- the gate's own observer --
        reports the job closed and nothing is left, or :class:`StreamStopped`. An item of a
        topic not asked for is never yielded, and a buyer's word never moves the offset. The
        credential the relay uses is minted inside the adapter, confined to this job's
        stream and revocable by ``session``, and minted again before it expires: a stream
        lasts as long as its job, not as one credential.

        Temporal: :func:`kontra._relay.relay` with the relay principal
        (``sub = relay/<namespace>/job-<id>``, I30) and ``first_execution_run_id = pin``,
        paced from the run's history size (:func:`kontra._relay.pace_for`)."""
        ...


#: The member set of :class:`Engine`, pinned. Adding one is a decision (I32), not a refactor.
MEMBERS: Final = frozenset(
    {
        "name",
        "isolate",
        "expose",
        "quote",
        "bind",
        "start",
        "observe",
        "liveness",
        "cancel",
        "fence",
        "stream",
    }
)

#: How escrow's stop effects reach an engine: (stop, pinned), where ``pinned`` means "pass
#: ``JobRecord.pin``". The effects' values are persisted (kontra-cloud ``jobs.pending_stop``)
#: and therefore unchanged (owner decision OD9 of #55).
STOPS: Final[Mapping[EffectKind, tuple[Stop, bool]]] = {
    #: No pin yet (a cancel before a run), or a late start after a cancel: ask any run.
    EffectKind.CANCEL_WORKFLOW: (Stop.ASK, False),
    #: Past the deadline: end any open run.
    EffectKind.TERMINATE_WORKFLOW: (Stop.END, False),
    #: A buyer's cancel of an observed run, step one (I17): end the pinned lineage.
    EffectKind.TERMINATE_RUN: (Stop.END, True),
}
