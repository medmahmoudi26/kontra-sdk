"""The Temporal adapter of seam ⑥ (decisions I32): :class:`TemporalEngine`, the only one.

A thin layer over :class:`kontra._control.Control` (the control plane's calls on the internal
frontend, shared by both gates) and :func:`kontra._relay.relay` (the stream relay, I31): it
adds what the Protocol asks of an adapter and nothing of its own about money.

* The handle (:meth:`TemporalEngine.bind`) is ``temporal/1`` and a newline, then the plan's
  five fields as canonical JSON (sorted keys, no spaces). It is parsed only here, strictly
  and before any I/O: the prefix, the size, exactly those keys, each field by its own
  anchored validator, and construct-and-compare (the parsed fields must rebuild the bytes
  exactly). The namespaces, the workflow id, the Nexus service and the task queue are then
  *constructed* from those fields by :mod:`kontra.contract.names`; nothing is read back from
  a Temporal string.
* A delivery leaves only as :class:`~kontra.engine.SealedPayload`: a verified completion
  whose stored result is not the codec's envelope is reported
  :attr:`~kontra.escrow.ObservedStatus.UNSEALED` and its bytes are dropped here.
* Credentials are the gate's ES256 JWTs, signed by its key ring (:class:`Signer`): the worker
  token :meth:`TemporalEngine.isolate` mints, and the relay token (no permission, subject
  :func:`~kontra.contract.names.relay_subject`, I30) minted inside :meth:`TemporalEngine.stream`
  and never handed out -- minted again, and the frontend dialled with it, once half its life
  is spent, so a job's relay outlives any one token.
* How far a job's start got (:attr:`~kontra.engine.Observed.reach`, decisions I40) is
  STARTED for a verified or pinned lineage, and otherwise read from the buyer's standalone
  operation ``job-<id>`` (:meth:`~kontra._control.Control.describe_call`) and mapped by
  :func:`reach_of`, the case analysis kontra-cloud's ``judge_call`` made (FIXUPS #22); a read
  that fails is None, never an error, so reputation never holds up an observation escrow
  needs.
* The internal frontend is reached only through :class:`kontra._control.Control` (the relay's
  pace too: :meth:`~kontra._control.Control.history_size`); the relay reads where
  ``relay_service`` dials (the public frontend, or a dev server's own:
  :func:`kontra._control.internal_relay_service`), and dereferences each item from the job's
  stream blobs through ``stream_blobs`` (the gate's own reader, scoped to the job: decisions
  I39), handing the gate the same :class:`~kontra.engine.Sealed` items as before.

Nexus, namespaces, task queues and run ids stay in this module: the Protocol names none of
them. One read is Temporal by definition and stays outside it, as an extension of this
class: the Workers view (:meth:`TemporalEngine.workers`, I21). The blame read for a job that
never started, kontra-cloud's ``describe_operation`` / ``judge_call`` until #55's OD7, is
behind :meth:`TemporalEngine.observe` now (``reach``, decisions I40).
"""

from __future__ import annotations

import json
from collections.abc import AsyncGenerator, Awaitable, Callable, Collection, Mapping
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from typing import TYPE_CHECKING, Any, Final, Literal, Protocol, cast

from google.protobuf.message import DecodeError
from temporalio.api.common.v1 import Payload
from temporalio.api.workflowservice.v1 import (
    QueryWorkflowRequest,
    QueryWorkflowResponse,
    UpdateWorkflowExecutionRequest,
    UpdateWorkflowExecutionResponse,
)

from kontra._control import (
    EXECUTE_OPERATION,
    HANDLER_BAD_REQUEST,
    HANDLER_INTERNAL,
    HANDLER_UPSTREAM_TIMEOUT,
    HOLD_HEADER,
    BuyerCall,
    BuyerLookupError,
    CallRecord,
    CallStatus,
    EndpointConflict,
    NexusSyncKind,
    Occupation,
    TemporalControl,
    TemporalError,
)
from kontra._control import ObservationIncomplete as _ChainIncomplete
from kontra._relay import MIN_INTERVAL, RelayService, RelayStopped, pace_for, relay
from kontra.contract.names import (
    NameError_,
    OfferingId,
    is_endpoint_name,
    is_handle,
    is_job_id,
    job_workflow_id,
    relay_subject,
)
from kontra.engine import (
    HANDLE_MAX,
    BuyerStart,
    EngineError,
    EngineName,
    Fenced,
    InvalidHandle,
    Isolation,
    JobHandle,
    JobPlan,
    Liveness,
    NotSealed,
    ObservationIncomplete,
    Observed,
    PartyLookupError,
    Pin,
    QuoteAnswer,
    QuoteKind,
    Reach,
    RouteConflict,
    Sealed,
    SealedPayload,
    Stop,
    StopReason,
    StreamBatch,
    StreamItem,
    StreamStopped,
)
from kontra.escrow import Observation, ObservedStatus, Provenance
from kontra.gate_client import models as m

__all__ = [
    "AUD_TEMPORAL",
    "HANDLE_PREFIX",
    "HISTORY_LIMITS",
    "QUOTE_OPERATION",
    "WORKER_TTL",
    "Signer",
    "StreamBlobs",
    "TemporalEngine",
    "reach_of",
    "worker_token",
]

#: The audience of every token a Temporal frontend accepts from the gate.
AUD_TEMPORAL: Final = "kontra:temporal"
#: How long a worker (and a relay) token lives.
WORKER_TTL: Final = timedelta(minutes=15)
#: The synchronous Nexus operation that prices a job (the seller's ``quote``).
QUOTE_OPERATION: Final = "quote"
#: What every handle this adapter mints starts with: its own name and the format's version.
HANDLE_PREFIX: Final = b"temporal/1\n"
#: Kontra's pinned ``limit.historyCount.error`` and ``limit.historySize.error``
#: (``temporal/authorizer/dynamicconfig/kontra.yaml``): what the relay paces against.
HISTORY_LIMITS: Final = (20_480, 20 * 1024 * 1024)

_KEYS = ("buyer", "job", "offering", "route", "seller")
_JSON_PLAIN = b"json/plain"
_QUOTE_KINDS: Final[Mapping[NexusSyncKind, QuoteKind]] = {
    NexusSyncKind.OK: QuoteKind.OK,
    NexusSyncKind.HANDLER_ERROR: QuoteKind.REFUSED,
    NexusSyncKind.TIMEOUT: QuoteKind.TIMEOUT,
    NexusSyncKind.UNAVAILABLE: QuoteKind.UNAVAILABLE,
}
_STOPS: Final[Mapping[str, StopReason]] = {
    "history_budget": "budget",
    "protocol": "protocol",
    "denied": "denied",
}


class Signer(Protocol):
    """The gate's key ring: who it says it is, and an ES256 signature over claims (FakeGate's
    and kontra-cloud's, whose public keys the Temporal frontend fetches from the gate's
    JWKS)."""

    @property
    def issuer(self) -> str: ...

    def sign(self, claims: Mapping[str, Any]) -> str: ...


class StreamBlobs(Protocol):
    """The gate's own reader of a job's stream blobs (decisions I39): the blob named
    ``sha256`` if it is one of ``job_id``'s (created by its seller for its stream) and at most
    ``size`` bytes are read; else None. The relay checks its size, digest and age shape."""

    async def read_stream_blob(self, job_id: str, sha256: str, size: int) -> bytes | None: ...


def _utcnow() -> datetime:
    return datetime.now(UTC)


def _whole(t: datetime) -> datetime:
    return datetime.fromtimestamp(int(t.timestamp()), UTC)


def worker_token(
    signer: Signer, party: str, session: str, *, now: datetime, ttl: timedelta = WORKER_TTL
) -> tuple[str, datetime]:
    """A party's Temporal *worker* JWT (decisions I3, I30) and its expiry: ``sub`` the party,
    ``sid`` the gate session that can revoke it, the one permission ``<party>:worker``. The
    claims of every worker token, whoever mints it (:meth:`TemporalEngine.isolate`, or a gate
    with no Temporal at all handing out a well-formed token)."""
    if not is_handle(party):
        raise ValueError("not a party handle")
    expires = _whole(now + ttl)
    token = signer.sign(
        {
            "iss": signer.issuer,
            "aud": AUD_TEMPORAL,
            "sub": party,
            "sid": session,
            "permissions": [f"{party}:worker"],
            "iat": int(now.timestamp()),
            "exp": int(expires.timestamp()),
        }
    )
    return token, expires


@dataclass(frozen=True, slots=True)
class _Handle:
    """A parsed handle: the plan, every field validated."""

    buyer: str
    job: str
    offering: OfferingId
    route: str
    seller: str

    @property
    def workflow_id(self) -> str:
        return job_workflow_id(self.job)

    def call(self, holds: Collection[str]) -> BuyerCall:
        """The buyer's start as the gate's own records describe it (I15): what a run must
        trace back to. A party's namespace is its handle (I1)."""
        return BuyerCall(
            namespace=self.buyer,
            endpoint=self.route,
            service=self.offering.service_name,
            holds=frozenset(holds),
        )


def _canonical(fields: Mapping[str, str]) -> bytes:
    return HANDLE_PREFIX + json.dumps(
        dict(sorted(fields.items())), separators=(",", ":"), ensure_ascii=True
    ).encode("ascii")


def _invalid(why: str) -> InvalidHandle:
    return InvalidHandle(f"not a temporal/1 handle: {why}")


def _parse(raw: object) -> _Handle:
    """The plan inside a handle this adapter minted, or :class:`InvalidHandle` (before any
    I/O): see the module docstring."""
    if not isinstance(raw, bytes):
        raise _invalid("not bytes")
    if len(raw) > HANDLE_MAX or not raw.startswith(HANDLE_PREFIX):
        raise _invalid("wrong prefix or size")
    try:
        loaded: object = json.loads(raw[len(HANDLE_PREFIX) :])
    except (ValueError, UnicodeDecodeError):
        raise _invalid("not JSON") from None
    if not isinstance(loaded, dict):
        raise _invalid("not an object")
    fields: dict[str, str] = {}
    for key, value in cast(dict[object, object], loaded).items():
        if not isinstance(key, str) or not isinstance(value, str):
            raise _invalid("a field is not a string")
        fields[key] = value
    if tuple(sorted(fields)) != _KEYS or _canonical(fields) != raw:
        raise _invalid("not canonical")
    try:
        offering = OfferingId.parse(fields["offering"])
    except NameError_:
        raise _invalid("bad offering") from None
    if not (
        offering.key == fields["offering"]
        and offering.party == fields["seller"]
        and is_handle(fields["buyer"])
        and is_handle(fields["seller"])
        and is_job_id(fields["job"])
        and is_endpoint_name(fields["route"])
    ):
        raise _invalid("bad field")
    return _Handle(fields["buyer"], fields["job"], offering, fields["route"], fields["seller"])


def reach_of(record: CallRecord | None, call: BuyerCall) -> tuple[Reach, BuyerStart | None]:
    """How far the buyer's start got (decisions I40), from the standalone operation
    ``job-<id>`` in the buyer's namespace as the frontend described it (``record``; None:
    there is none) for a job with no verified run: kontra-cloud's ``judge_call`` (FIXUPS
    #22), case for case, with its window and its wording left to the gate (the
    :class:`~kontra.engine.BuyerStart` of the call is handed over for both). ``call``: the
    job's own record of the buyer's call (its route, service and holds).

    Only an operation that *is* this job's call counts (:meth:`CallRecord.is_call_of`, as
    escrow matches a run's call). Then, in judge_call's order:

    * the handler took the call, or it completed: STARTED (judge_call: reached);
    * the buyer terminated it, or asked to cancel it and it ended canceled: WITHDRAWN
      (unseen; the buyer must not call during a seller's brief outage, withdraw, and leave
      the seller blamed);
    * it ended canceled with no request of the buyer's: REFUSED (the seller's handler failed
      its start with a canceled error);
    * no attempt has finished: NEVER_DISPATCHED (unseen);
    * ``BAD_REQUEST``: REFUSED (the hold, the input or its price; no blame, by design);
    * ``UPSTREAM_TIMEOUT``: UNANSWERED (reached only if it waited through the gate's window,
      from :class:`~kontra.engine.BuyerStart`'s times; else unseen);
    * ``INTERNAL``, declared final: FAILED (the seller's code; reached);
    * any other failure: INCONCLUSIVE (it may be the buyer's doing).

    The call's :class:`~kontra.engine.BuyerStart` goes with every reach of a call of the
    job's (None with no such call): the window reads its times, and the wording of every
    unseen call its ``cancel_requested``."""
    if record is None or not record.is_call_of(call):
        return Reach.NEVER_DISPATCHED, None
    start = BuyerStart(record.scheduled_at, record.expires_at, record.cancel_requested)
    if record.started or record.status is CallStatus.COMPLETED:
        return Reach.STARTED, start
    if record.status is CallStatus.TERMINATED or (
        record.status is CallStatus.CANCELED and record.cancel_requested
    ):
        return Reach.WITHDRAWN, start
    if record.status is CallStatus.CANCELED:
        return Reach.REFUSED, start
    failure = record.failure
    if failure is None:
        return Reach.NEVER_DISPATCHED, start
    if failure.type == HANDLER_BAD_REQUEST:
        return Reach.REFUSED, start
    if failure.type == HANDLER_UPSTREAM_TIMEOUT:
        return Reach.UNANSWERED, start
    if failure.type == HANDLER_INTERNAL and failure.final:
        return Reach.FAILED, start
    return Reach.INCONCLUSIVE, start


def _payload(raw: bytes) -> Payload:
    try:
        return Payload.FromString(raw)
    except DecodeError:
        raise ValueError("not a serialized temporal Payload") from None


def _engine_error(e: Exception) -> EngineError:
    """What a failed control-plane call means on this seam: cannot say, never a fact."""
    if isinstance(e, EngineError):
        return e
    if isinstance(e, BuyerLookupError):
        return PartyLookupError(str(e))
    if isinstance(e, _ChainIncomplete):
        return ObservationIncomplete(str(e))
    if isinstance(e, TemporalError):
        return EngineError(str(e))
    return EngineError(f"{type(e).__name__}: {e}")


class TemporalEngine:
    """:class:`kontra.engine.Engine` on Temporal (see the module docstring).

    ``control``: the control plane on the internal frontend (:class:`kontra._control.Control`,
    or a test double). ``signer``: the gate's key ring. ``address``: the public frontend a
    party's worker dials (``Isolation.address``). ``relay_service``: dials a
    ``WorkflowService`` on the PUBLIC frontend carrying a given relay JWT, for :meth:`stream`
    (called again with a fresh token before the last expires); a dev server's own frontend,
    which judges no token, is :func:`kontra._control.internal_relay_service`.
    ``stream_blobs``: the gate's reader of a job's stream blobs, where the relay dereferences
    each item (``stream_item_max``: none larger is fetched). Without both the adapter relays
    no streams (``StreamStopped("unsupported")``). ``relay_interval``,
    ``relay_rpc_timeout`` and ``history_limits`` tune the relay's pace (the platform's 2 s, a
    poll's deadline, the server's history limits); ``worker_ttl`` is a worker's and a relay
    token's life; ``clock`` is the gate's (tokens, a start's deadline)."""

    def __init__(
        self,
        control: TemporalControl,
        *,
        signer: Signer,
        address: str,
        relay_service: Callable[[str], Awaitable[RelayService]] | None = None,
        stream_blobs: StreamBlobs | None = None,
        stream_item_max: int = m.STREAM_ITEM_MAX,
        worker_ttl: timedelta = WORKER_TTL,
        relay_interval: float = MIN_INTERVAL,
        relay_rpc_timeout: float = 30.0,
        history_limits: tuple[int, int] = HISTORY_LIMITS,
        clock: Callable[[], datetime] = _utcnow,
    ) -> None:
        self._control = control
        self._signer = signer
        self._address = address
        self._relay_service = relay_service
        self._stream_blobs = stream_blobs
        self._stream_item_max = stream_item_max
        self._worker_ttl = worker_ttl
        self._relay_interval = relay_interval
        self._relay_rpc_timeout = relay_rpc_timeout
        self._history_limits = history_limits
        self._clock = clock
        #: Parties whose namespace this process has provisioned (a restart provisions again:
        #: ``ensure_namespace`` is idempotent).
        self._provisioned: set[str] = set()

    @property
    def name(self) -> EngineName:
        return "temporal"

    @property
    def control(self) -> TemporalControl:
        return self._control

    # ------------------------------------------------------------------ parties

    async def isolate(self, party: str, *, session: str | None = None) -> Isolation:
        if not is_handle(party):
            raise ValueError("not a party handle")
        if party not in self._provisioned:
            try:
                await self._control.ensure_namespace(party)
            except Exception as e:
                raise _engine_error(e) from e
            self._provisioned.add(party)
        if session is None:
            return Isolation(home=party, address=self._address)
        token, expires = worker_token(
            self._signer, party, session, now=self._clock(), ttl=self._worker_ttl
        )
        return Isolation(home=party, address=self._address, credential=token, expires_at=expires)

    # ------------------------------------------------------------------ offerings

    async def expose(self, offering: OfferingId, route: str) -> None:
        if not is_endpoint_name(route):
            raise ValueError("not a route this catalog assigns")
        try:
            await self._control.create_endpoint(route, offering.namespace, offering.task_queue)
        except EndpointConflict as e:
            raise RouteConflict(str(e)) from e
        except Exception as e:
            raise _engine_error(e) from e

    async def quote(
        self,
        offering: OfferingId,
        input: bytes,
        *,
        timeout: float,  # noqa: ASYNC109 -- the Nexus request timeout
    ) -> QuoteAnswer:
        payload = _payload(input)
        try:
            r = await self._control.nexus_sync(
                offering.namespace,
                offering.task_queue,
                offering.service_name,
                QUOTE_OPERATION,
                payload,
                {},
                timeout,
            )
        except Exception as e:
            raise _engine_error(e) from e
        document = None
        if r.payload is not None and r.payload.metadata.get("encoding") == _JSON_PLAIN:
            document = r.payload.data  # the one plaintext that crosses: a price (I4)
        return QuoteAnswer(_QUOTE_KINDS[r.kind], document, r.message)

    async def workers(self, offering: OfferingId) -> dict[m.TaskQueueType, m.TaskQueueTypeInfo]:
        """The Workers view of ``offering`` (I21): Temporal's task-queue stats in the seller's
        namespace. A Temporal-only extension, not a Protocol member (owner decision OD8)."""
        try:
            return await self._control.describe_task_queue(offering.namespace, offering.task_queue)
        except Exception as e:
            raise _engine_error(e) from e

    # ------------------------------------------------------------------ jobs

    def bind(self, plan: JobPlan) -> JobHandle:
        if not is_endpoint_name(plan.route):
            raise ValueError("not a route this catalog assigns")
        raw = _canonical(
            {
                "buyer": plan.buyer,
                "job": plan.job_id,
                "offering": plan.offering.key,
                "route": plan.route,
                "seller": plan.seller,
            }
        )
        _parse(raw)  # every field validated, and the size
        return JobHandle(raw)

    async def start(
        self, handle: JobHandle, input: bytes, *, hold: str, deadline: datetime
    ) -> None:
        h = _parse(handle)
        payload = _payload(input)
        try:
            await self._control.start_operation(
                h.buyer,
                h.workflow_id,  # the standalone operation's id: job-<id> (I4)
                h.route,
                h.offering.service_name,
                EXECUTE_OPERATION,
                payload,
                {HOLD_HEADER: hold},
                max(timedelta(seconds=1), deadline - self._clock()),
            )
        except Exception as e:
            raise _engine_error(e) from e

    async def observe(
        self, handle: JobHandle, *, pin: Pin | None, holds: Collection[str]
    ) -> Observed:
        h = _parse(handle)
        call = h.call(holds)
        try:
            obs, result = await self._control.observe(h.seller, h.workflow_id, call, pin)
        except Exception as e:
            raise _engine_error(e) from e
        reach, start = await self._reach(h, call, obs, pin)
        if obs.status is not ObservedStatus.COMPLETED or obs.provenance is not Provenance.VERIFIED:
            # Only a verified completion delivers: no bytes leave for anything else.
            cleared = replace(obs, result_sha256=None, result_size=None)
            return Observed(cleared, reach=reach, buyer_start=start)
        try:
            # A verified completion the control plane gave no bytes for is no delivery
            # either (``Control`` itself answers b"" for a completion with no payload).
            sealed = SealedPayload(result if result is not None else b"")
        except NotSealed:
            # Never hand the gate plaintext: an unsealed completion is not a delivery.
            unsealed = replace(
                obs, status=ObservedStatus.UNSEALED, result_sha256=None, result_size=None
            )
            return Observed(unsealed, reach=reach, buyer_start=start)
        # The digest is of exactly the bytes handed over, computed here, not taken on trust.
        digest = sha256(sealed.data).hexdigest()
        delivered = replace(obs, result_sha256=digest, result_size=len(sealed.data))
        return Observed(delivered, sealed, reach=reach, buyer_start=start)

    async def _reach(
        self, h: _Handle, call: BuyerCall, obs: Observation, pin: Pin | None
    ) -> tuple[Reach | None, BuyerStart | None]:
        """How far the job's start got (decisions I40). A verified lineage, or the pinned one
        (verified when escrow pinned it), was begun by the buyer's start: STARTED, with no
        read. Otherwise the buyer's standalone operation ``job-<id>`` is described and mapped
        (:func:`reach_of`); a read that fails is None ("cannot say"), never raised: reach is
        reputation, and the observation is escrow's."""
        if pin or obs.provenance is Provenance.VERIFIED:
            return Reach.STARTED, None
        try:
            record = await self._control.describe_call(h.buyer, h.workflow_id)
        except Exception:
            return None, None
        return reach_of(record, call)

    async def liveness(self, handle: JobHandle, *, pin: Pin) -> Liveness | None:
        h = _parse(handle)
        try:
            s = await self._control.history_summary(h.seller, h.workflow_id, pin)
        except Exception as e:
            raise _engine_error(e) from e
        if s is None:
            return None
        return Liveness(
            state=s.state,
            last_step=s.last_event,
            steps_done=s.activities_completed,
            steps_running=s.activities_running,
            restarts=s.continue_as_new_count,
        )

    async def cancel(self, handle: JobHandle, *, stop: Stop, pin: Pin | None, reason: str) -> None:
        h = _parse(handle)
        try:
            if stop is Stop.ASK:
                if pin:
                    await self._control.cancel_workflow(
                        h.seller, h.workflow_id, reason, first_run_id=pin
                    )
                else:
                    await self._control.cancel_workflow(h.seller, h.workflow_id, reason)
            elif pin:
                await self._control.terminate_workflow(
                    h.seller, h.workflow_id, reason, first_run_id=pin
                )
            else:
                await self._control.terminate_workflow(h.seller, h.workflow_id, reason)
        except Exception as e:
            raise _engine_error(e) from e

    async def fence(self, handle: JobHandle, *, reason: str) -> Fenced:
        h = _parse(handle)
        try:
            found = await self._control.occupy_job(h.seller, h.workflow_id, reason)
        except Exception as e:
            raise _engine_error(e) from e
        return Fenced.CLOSED if found is Occupation.OCCUPIED else Fenced.STARTED

    async def stream(
        self,
        handle: JobHandle,
        *,
        pin: Pin,
        topics: Collection[str],
        after: int | Literal["tail"],
        closed: Callable[[], Awaitable[bool]],
        session: str,
    ) -> AsyncGenerator[StreamBatch, None]:
        h = _parse(handle)
        if self._relay_service is None or self._stream_blobs is None:
            raise StreamStopped(
                "unsupported", "this adapter has no frontend or bucket to relay from"
            )
        service = _Renewing(lambda: self._relay_token(h, session), self._relay_service, self._clock)
        await service.dial()
        reader = self._stream_blobs

        async def fetch(sha: str, size: int) -> bytes | None:
            return await reader.read_stream_blob(h.job, sha, size)  # scoped to this job

        batches = relay(
            service,
            namespace=h.seller,
            job_id=h.job,
            topics=sorted(topics),
            closed=closed,
            start=after,
            first_execution_run_id=lambda: pin,
            pace=self._pace(h),
            min_interval=self._relay_interval,
            rpc_timeout=self._relay_rpc_timeout,
            blobs=fetch,
            item_max=self._stream_item_max,
        )
        try:
            async for batch in batches:
                try:
                    items = tuple(
                        StreamItem(
                            i.topic,
                            i.offset,
                            None if i.ciphertext is None else Sealed(i.ciphertext),
                            i.size,
                        )
                        for i in batch.items
                    )
                except NotSealed:
                    raise StreamStopped("protocol", "a relayed item is not an age file") from None
                yield StreamBatch(items, batch.next_offset, batch.dropped)
        except RelayStopped as e:
            raise StreamStopped(_STOPS[e.reason], str(e)) from None
        finally:
            await batches.aclose()

    def _relay_token(self, h: _Handle, session: str) -> tuple[str, datetime]:
        """The relay's JWT for one job (decisions I30): no permission, the subject the
        authorizer rebuilds from the request. Never handed to a party."""
        now = self._clock()
        expires = _whole(now + self._worker_ttl)
        token = self._signer.sign(
            {
                "iss": self._signer.issuer,
                "aud": AUD_TEMPORAL,
                "sub": relay_subject(h.seller, h.job),
                "sid": session,
                "iat": int(now.timestamp()),
                "exp": int(expires.timestamp()),
            }
        )
        return token, expires

    def _pace(self, h: _Handle) -> Callable[[], Awaitable[float | None]]:
        """How long the relay waits between polls: :func:`kontra._relay.pace_for` over the
        run's history as the control plane describes it (what the gate's observer reads,
        :meth:`~kontra._control.Control.history_size`); None (stop) from half the server's
        limits on. A describe that fails, or finds no run, keeps the relay's own pace."""
        events_limit, bytes_limit = self._history_limits
        interval = self._relay_interval

        async def pace() -> float | None:
            try:
                size = await self._control.history_size(h.seller, h.workflow_id)
            except Exception:
                return interval
            if size is None:
                return interval
            paced = pace_for(*size, events_limit=events_limit, bytes_limit=bytes_limit)
            if paced is None:
                return None
            return interval if paced == MIN_INTERVAL else paced

        return pace


class _Renewing:
    """The relay's frontend, dialled with a relay token, and dialled again with a fresh one
    once half the current one's life is spent (decisions I30: a token lives
    ``worker_ttl``; a job's stream may last as long as the job). A token only has to be good
    when a call is admitted, so the half left covers any clock skew under it. The new token
    has the same subject and session, so a revoked session still ends the relay."""

    def __init__(
        self,
        mint: Callable[[], tuple[str, datetime]],
        connect: Callable[[str], Awaitable[RelayService]],
        clock: Callable[[], datetime],
    ) -> None:
        self._mint = mint
        self._connect = connect
        self._clock = clock
        self._service: RelayService | None = None
        self._renew_at: datetime | None = None

    async def dial(self) -> RelayService:
        now = self._clock()
        if self._service is None or self._renew_at is None or now >= self._renew_at:
            token, expires = self._mint()
            try:
                self._service = await self._connect(token)
            except Exception as e:
                raise _engine_error(e) from e
            self._renew_at = now + (expires - now) / 2
        return self._service

    async def update_workflow_execution(
        self,
        req: UpdateWorkflowExecutionRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] | None = None,
        timeout: timedelta | None = None,  # noqa: ASYNC109 -- temporalio's signature
    ) -> UpdateWorkflowExecutionResponse:
        service = await self.dial()
        if metadata is None:
            return await service.update_workflow_execution(req, retry=retry, timeout=timeout)
        return await service.update_workflow_execution(
            req, retry=retry, metadata=dict(metadata), timeout=timeout
        )

    async def query_workflow(
        self,
        req: QueryWorkflowRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] | None = None,
        timeout: timedelta | None = None,  # noqa: ASYNC109 -- temporalio's signature
    ) -> QueryWorkflowResponse:
        service = await self.dial()
        if metadata is None:
            return await service.query_workflow(req, retry=retry, timeout=timeout)
        return await service.query_workflow(
            req, retry=retry, metadata=dict(metadata), timeout=timeout
        )


if TYPE_CHECKING:  # pyright proves the structural fit: a TemporalEngine is an Engine
    from kontra.engine import Engine

    _conforms: type[Engine] = TemporalEngine
