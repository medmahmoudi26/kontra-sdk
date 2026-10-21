"""The gate's stream relay: reading a job's workflow stream with the strict wire shape of
decisions I30, written once (decisions I31). The FakeGate relays with it; kontra-cloud's gate
imports it and reads a stream with nothing else.

:func:`relay` makes exactly two calls, on a relay token's client (``names.relay_subject``):

* ``UpdateWorkflowExecution`` of :data:`~kontra.contract.names.STREAM_POLL_UPDATE` on
  ``job-<id>``: run id ``""`` (so it follows a continue-as-new), ``first_execution_run_id``
  once escrow has pinned the chain, wait policy COMPLETED, one fresh update id per logical
  poll. While an answer carries no outcome (the server's long poll returned at ACCEPTED), or
  the RPC timed out, the **same** request -- the same update id -- is sent again: the server
  finds an update by its id. Never ``PollWorkflowExecutionUpdate`` or
  ``DescribeWorkflowExecution``, which the authorizer refuses a relay.
* ``QueryWorkflow`` of :data:`~kontra.contract.names.STREAM_OFFSET_QUERY`, only for a
  ``"tail"`` start; its answer must be the marked plaintext offset. The gate starts a job's
  relay from offset 0 instead (decisions I31, OD2), and ``start`` has no default.

Each answer is a stream envelope (:mod:`kontra.codec.stream`), read without a key: a topic
the buyer may read is named in the clear, any other is ``null`` and dropped here; every item
is a reference to its blob in the claim-check bucket (decisions I39). The loop
**dereferences** the listed ones through ``blobs`` -- the gate's own reader of the job's
stream blobs -- a few at once, and keeps a blob only when it is exactly the size and the
sha256 the envelope names and an age file (:func:`~kontra.codec.stream.verified_item`), so
plaintext never reaches a gate's storage; an item above ``item_max``, a stub, or a blob
missing or not what its reference names, is an item without ciphertext (the gate records a
gap). The loop yields one :class:`RelayBatch` per answer; the caller stores its items, then
its ``next_offset`` (the at-least-once boundary), and passes that offset as ``start`` when it
relays the same job again: a buyer's own resume is served from the gate's store and never
moves the relay.

Failures, by the outcome's ``ApplicationFailureInfo.type``: ``StreamDraining`` (the run
continues-as-new) is polled again after a short cooldown, ``TruncatedOffset`` from offset 0
(at once; at offset 0 itself it is a refusal like any other, since a stream never answers
that), ``AcceptedUpdateCompletedWorkflow`` again unless the job is closed,
``KontraStreamFull`` (the job's run accepts no more polls: its poll budget or its history)
stops with ``history_budget``; ``KontraStreamBusy`` (too many polls in flight) and any other
rejection (no stream registered yet, say) back off while the job is open -- rejections cost
the job no history, so a worker could answer them for free, as fast as the relay asks. RPC
errors: NOT_FOUND / FAILED_PRECONDITION stop when ``closed()`` says so, else back off;
RESOURCE_EXHAUSTED, and PERMISSION_DENIED whose reason is the Authorizer's relay rate
(:data:`RELAY_RATE_REASON`, read from the typed details), back off up to a minute; any other
PERMISSION_DENIED / UNAUTHENTICATED stops with ``denied`` (the gate mints a new token or
gives up). ``closed()`` is the gate's own observer (decisions I6), asked after every failure
and every empty wait. Between answers that were not ``more_ready`` the loop waits
:data:`MIN_INTERVAL`, what ``pace()`` says, or what its own count of the run's polls says
(:func:`own_pace`: the run accepts :data:`~kontra.codec.stream.STREAM_POLLS_MAX`), whichever is
longest; ``pace()`` returning None stops it with ``history_budget`` (:func:`pace_for` is the
shared policy). A ``more_ready`` answer is polled again at once, from its ``next_offset``:
one with no items is a poll the job held back, past the answers one of its workflow tasks
may carry (``kontra.serve.interceptors.STREAM_ANSWER_BUDGET``), and is no failure.

Imports ``temporalio.api`` protos, ``temporalio.service``, :mod:`kontra.contract` and
:mod:`kontra.codec.stream` -- never ``temporalio.contrib``: the gate depends on the wire shape,
not on the Public Preview package.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from collections.abc import AsyncGenerator, Awaitable, Callable, Sequence
from dataclasses import dataclass
from datetime import timedelta
from typing import Literal, Protocol

from temporalio.api.common.v1 import Payload, Payloads, WorkflowExecution
from temporalio.api.enums.v1 import UpdateWorkflowExecutionLifecycleStage
from temporalio.api.errordetails.v1 import PermissionDeniedFailure
from temporalio.api.query.v1 import WorkflowQuery
from temporalio.api.update.v1 import Input as UpdateInput
from temporalio.api.update.v1 import Meta as UpdateMeta
from temporalio.api.update.v1 import Outcome, WaitPolicy
from temporalio.api.update.v1 import Request as UpdateRequest
from temporalio.api.workflowservice.v1 import (
    QueryWorkflowRequest,
    QueryWorkflowResponse,
    UpdateWorkflowExecutionRequest,
    UpdateWorkflowExecutionResponse,
)
from temporalio.service import RPCError, RPCStatusCode

from kontra.codec.stream import (
    STREAM_BUSY,
    STREAM_FULL,
    STREAM_POLLS_MAX,
    EnvelopeError,
    EnvelopeItem,
    parse_envelope,
    parse_offset,
    verified_item,
)
from kontra.contract.names import STREAM_OFFSET_QUERY, STREAM_POLL_UPDATE, job_workflow_id
from kontra.contract.offering import check_topic

__all__ = [
    "MIN_INTERVAL",
    "RELAY_RATE_REASON",
    "STREAM_BUSY",
    "STREAM_FULL",
    "FetchItem",
    "RelayBatch",
    "RelayItem",
    "RelayService",
    "RelayStopped",
    "StopReason",
    "own_pace",
    "pace_for",
    "poll_input_payload",
    "relay",
]

#: The shortest wait between two polls that were not ``more_ready``.
MIN_INTERVAL = 2.0
#: The reason the Authorizer gives a relay request past its rate (``RelayRateReason`` in
#: ``temporal/authorizer``; a test pins the twin): the relay waits, it does not stop.
RELAY_RATE_REASON = "kontra: a stream relay's requests are past their rate"
#: ``fetch(sha256, size)``: a job's stream blob as the gate's reader returns it, or None when
#: it is not there. The relay verifies what comes back (:func:`kontra.codec.stream.verified_item`).
FetchItem = Callable[[str, int], Awaitable[bytes | None]]

#: Why :func:`relay` stopped: the job's history budget, an answer that is not a stream
#: envelope (a worker without #52, a seller that replaced the poll handler), or a token the
#: server refused.
StopReason = Literal["history_budget", "protocol", "denied"]

_COMPLETED = (
    UpdateWorkflowExecutionLifecycleStage.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED
)
_JSON_PLAIN = b"json/plain"
_STREAM_DRAINING = "StreamDraining"
_TRUNCATED_OFFSET = "TruncatedOffset"
_COMPLETED_WORKFLOW = "AcceptedUpdateCompletedWorkflow"
#: Waits (seconds); module-level so a test can shorten them.
_COOLDOWN = 0.2
_BACKOFF_START = 0.5
_BACKOFF_MAX = 30.0
_EXHAUSTED_MAX = 60.0
#: The most topics a poll names: an offering's streams (``offering.STREAMS_MAX``, 8) and
#: ``kontra.progress``. The gate's largest poll then stays far under the Authorizer's 4 KiB
#: cap on a relay request (``RelayRequestMax``, decisions I30).
_TOPICS_MAX = 9
#: Blobs fetched at once for one answer.
_FETCH_CONCURRENCY = 16
_RESEND = frozenset({RPCStatusCode.DEADLINE_EXCEEDED, RPCStatusCode.CANCELLED})
_GONE = frozenset({RPCStatusCode.NOT_FOUND, RPCStatusCode.FAILED_PRECONDITION})
_DENIED = frozenset({RPCStatusCode.PERMISSION_DENIED, RPCStatusCode.UNAUTHENTICATED})

# Indirection so tests can observe the waits without patching asyncio itself.
_sleep = asyncio.sleep


class RelayService(Protocol):
    """The two calls of ``temporalio.service.WorkflowService`` the relay makes."""

    async def update_workflow_execution(
        self,
        req: UpdateWorkflowExecutionRequest,
        retry: bool = False,
        metadata: dict[str, str | bytes] = ...,
        timeout: timedelta | None = None,  # noqa: ASYNC109 -- temporalio's signature
    ) -> UpdateWorkflowExecutionResponse: ...

    async def query_workflow(
        self,
        req: QueryWorkflowRequest,
        retry: bool = False,
        metadata: dict[str, str | bytes] = ...,
        timeout: timedelta | None = None,  # noqa: ASYNC109 -- temporalio's signature
    ) -> QueryWorkflowResponse: ...


@dataclass(frozen=True, slots=True)
class RelayItem:
    """One item to store: a topic the buyer may read (in the clear), its offset, its age
    ciphertext, fetched and verified -- or None for an item the relay could not relay: above
    ``item_max``, not stored by the seller's worker, or its blob missing or not what the
    envelope names (store a gap with ``size``)."""

    topic: str
    offset: int
    ciphertext: bytes | None
    size: int


@dataclass(frozen=True, slots=True)
class RelayBatch:
    """One poll's answer. Store the items, then persist ``next_offset``."""

    items: tuple[RelayItem, ...]
    next_offset: int
    #: The run that answered (for logs).
    run_id: str
    #: Items the envelope held that were not asked for or not readable by the buyer
    #: (``topic`` null): dropped here, as defence in depth.
    dropped: int = 0


class RelayStopped(Exception):
    """The relay cannot go on: ``reason`` says why (:data:`StopReason`)."""

    def __init__(self, reason: StopReason, message: str) -> None:
        super().__init__(f"{reason}: {message}")
        self.reason: StopReason = reason


def poll_input_payload(topics: Sequence[str], from_offset: int) -> Payload:
    """The poll's argument as the contrib's ``PollInput`` encodes it with temporalio's JSON
    converter (``json/plain``, sorted keys, compact), built without the contrib."""
    data = json.dumps(
        {"from_offset": from_offset, "topics": list(topics)}, separators=(",", ":"), sort_keys=True
    )
    return Payload(metadata={"encoding": _JSON_PLAIN}, data=data.encode("utf-8"))


def own_pace(polls: int, min_interval: float = MIN_INTERVAL) -> float:
    """The least wait before a poll that is not ``more_ready``, after ``polls`` answers of the
    job's current run: ``min_interval`` for the first half of the run's poll budget
    (:data:`~kontra.codec.stream.STREAM_POLLS_MAX`), 10 s to three quarters, 60 s after -- so a
    run a buyer listens to all along, an item ready at every poll, lasts about an hour and a
    quarter of polling at the default interval before the budget, and the gate stops with
    ``history_budget`` only then (a longer stream continues as new)."""
    if polls < STREAM_POLLS_MAX // 2:
        return min_interval
    if polls < STREAM_POLLS_MAX * 3 // 4:
        return 10.0
    return 60.0


def pace_for(
    history_length: int, history_bytes: int, *, events_limit: int, bytes_limit: int
) -> float | None:
    """How long to wait before the next poll of a job whose history is this long (events)
    and this large (bytes), against the server's limits: :data:`MIN_INTERVAL` under 25% of
    either, 10 s under 40%, 60 s under 50%, None (stop) from 50% on -- where the job's own
    guard refuses polls anyway."""
    if events_limit <= 0 or bytes_limit <= 0:
        raise ValueError("the limits are positive")
    used = max(history_length / events_limit, history_bytes / bytes_limit)
    if used < 0.25:
        return MIN_INTERVAL
    if used < 0.40:
        return 10.0
    if used < 0.50:
        return 60.0
    return None


def _topics(topics: Sequence[str]) -> list[str]:
    if isinstance(topics, str):
        raise TypeError("topics is a sequence of topic names, not one name")
    chosen = sorted({check_topic(t) for t in topics})
    if not chosen:
        # The contrib reads no topic as every topic: the relay always names its topics.
        raise ValueError("the relay polls named topics (the offering's streams and progress)")
    if len(chosen) > _TOPICS_MAX:
        raise ValueError(f"at most {_TOPICS_MAX} topics")
    return chosen


def _request(
    namespace: str,
    workflow_id: str,
    first_run: str,
    topics: Sequence[str],
    offset: int,
    identity: str,
) -> UpdateWorkflowExecutionRequest:
    return UpdateWorkflowExecutionRequest(
        namespace=namespace,
        workflow_execution=WorkflowExecution(workflow_id=workflow_id),
        first_execution_run_id=first_run,
        wait_policy=WaitPolicy(lifecycle_stage=_COMPLETED),
        request=UpdateRequest(
            meta=UpdateMeta(update_id=uuid.uuid4().hex, identity=identity),
            input=UpdateInput(
                name=STREAM_POLL_UPDATE,
                args=Payloads(payloads=[poll_input_payload(topics, offset)]),
            ),
        ),
    )


class _Backoff:
    def __init__(self, start: float, ceiling: float) -> None:
        self.start, self.ceiling, self.next = start, ceiling, start

    async def wait(self) -> None:
        delay, self.next = self.next, min(self.next * 2, self.ceiling)
        await _sleep(delay)

    def reset(self) -> None:
        self.next = self.start


async def relay(
    service: RelayService,
    *,
    namespace: str,
    job_id: str,
    topics: Sequence[str],
    closed: Callable[[], Awaitable[bool]],
    start: int | Literal["tail"],
    first_execution_run_id: Callable[[], str] = lambda: "",
    pace: Callable[[], Awaitable[float | None]] | None = None,
    min_interval: float = MIN_INTERVAL,
    rpc_timeout: float = 30.0,
    identity: str = "kontra-relay",
    blobs: FetchItem | None = None,
    item_max: int | None = None,
) -> AsyncGenerator[RelayBatch, None]:
    """Read ``job_id``'s stream (see the module docstring) until the job is closed, the
    caller stops iterating, or :class:`RelayStopped`. ``topics``: the offering's streams and
    ``kontra.progress``. ``start``, required: an offset -- ``0`` on a job's first relay
    (decisions I31, owner OD2: from the start of the log, once per job, so a buyer who
    listens late still gets the whole stream from the gate's store), then the gate's
    persisted cursor -- or ``"tail"`` (the stream's current end, read with the offset
    query), for a caller that wants only what comes next. ``blobs``: the gate's reader of
    this job's stream blobs (without it every item comes back without ciphertext);
    ``item_max``: the largest item the gate stores, none above it is fetched."""
    workflow_id = job_workflow_id(job_id)  # constructed, never parsed
    wanted = _topics(topics)
    shown = frozenset(wanted)
    timeout = timedelta(seconds=rpc_timeout)
    loop = asyncio.get_running_loop()
    if start == "tail":
        offset = await _tail(service, namespace, workflow_id, closed, timeout)
        if offset is None:
            return
    elif isinstance(start, bool) or not isinstance(start, int) or start < 0:  # pyright: ignore[reportUnnecessaryIsInstance]
        raise ValueError("start is an offset >= 0 or 'tail'")
    else:
        offset = start
    rejected = _Backoff(_BACKOFF_START, _BACKOFF_MAX)
    #: Answers of the current run (its poll budget is the run's): reset by a new run id.
    run_polls, polls = "", 0
    while True:
        began = loop.time()
        req = _request(namespace, workflow_id, first_execution_run_id(), wanted, offset, identity)
        answered = await _answer(service, req, closed, timeout)
        if answered is None:
            return
        outcome, run_id = answered
        if outcome.HasField("failure"):
            kind = outcome.failure.application_failure_info.type
            if kind == STREAM_FULL:
                raise RelayStopped("history_budget", "the job's run accepts no more polls")
            if kind == _STREAM_DRAINING:
                await _sleep(_COOLDOWN)
            elif kind == _TRUNCATED_OFFSET and offset != 0:
                offset = 0  # the log was truncated past us: read from its base
            elif kind == _COMPLETED_WORKFLOW:
                if await closed():
                    return
                await _sleep(_COOLDOWN)
            else:
                # Not written to the job's history: wait while the job is open. A stream
                # never refuses offset 0 as truncated, so a worker that does is answered as
                # any other refusal, not polled again at once.
                if await closed():
                    return
                await rejected.wait()
            continue
        rejected.reset()
        if run_id != run_polls:
            run_polls, polls = run_id, 0
        polls += 1  # an answered poll: one of the run's budget
        payloads = outcome.success.payloads
        if len(payloads) != 1:
            raise RelayStopped("protocol", "a poll answered other than one payload")
        try:
            envelope = parse_envelope(payloads[0])
        except EnvelopeError as e:
            raise RelayStopped("protocol", str(e)) from None
        if envelope.next_offset < offset and offset != 0:
            raise RelayStopped("protocol", "a poll answered an offset before the one asked")
        listed = [i for i in envelope.items if i.topic is not None and i.topic in shown]
        dropped = len(envelope.items) - len(listed)
        fetched = await _fetch(listed, blobs, item_max)
        items = tuple(
            RelayItem(i.topic, i.offset, fetched.get(i.offset), i.size)
            for i in listed
            if i.topic is not None
        )
        yield RelayBatch(items, envelope.next_offset, run_id, dropped)
        offset = envelope.next_offset
        if envelope.more_ready:
            continue
        wait = max(0.0, own_pace(polls, min_interval) - (loop.time() - began))
        if pace is not None:
            paced = await pace()
            if paced is None:
                raise RelayStopped("history_budget", "the job's history is past half its limits")
            wait = max(wait, paced)
        if wait > 0:
            await _sleep(wait)


async def _fetch(
    items: list[EnvelopeItem], blobs: FetchItem | None, item_max: int | None
) -> dict[int, bytes]:
    """The verified ciphertext of each of ``items`` that ``blobs`` has, by offset: a few at
    once; a stub, an item above ``item_max``, a blob missing, failing to arrive or not what
    the envelope names is left out (a gap)."""
    if blobs is None:
        return {}
    wanted = [i for i in items if i.sha256 is not None and (item_max is None or i.size <= item_max)]
    out: dict[int, bytes] = {}
    gate = asyncio.Semaphore(_FETCH_CONCURRENCY)

    async def one(item: EnvelopeItem) -> None:
        assert item.sha256 is not None
        async with gate:
            try:
                data = await blobs(item.sha256, item.size)
            except Exception:
                return  # the bucket did not answer: a gap, as for a missing blob
        ct = verified_item(data, item)
        if ct is not None:
            out[item.offset] = ct

    await asyncio.gather(*(one(i) for i in wanted))
    return out


def _rate_limited(e: RPCError) -> bool:
    """A PERMISSION_DENIED whose typed reason is the Authorizer's relay rate, compared whole."""
    if e.status != RPCStatusCode.PERMISSION_DENIED:
        return False
    try:
        details = list(e.grpc_status.details)
    except Exception:
        return False
    for detail in details:
        found = PermissionDeniedFailure()
        if detail.Is(PermissionDeniedFailure.DESCRIPTOR) and detail.Unpack(found):
            return found.reason == RELAY_RATE_REASON
    return False


async def _rpc_failed(
    e: RPCError, closed: Callable[[], Awaitable[bool]], exhausted: _Backoff, other: _Backoff
) -> bool:
    """After a failed RPC: True to stop (the job is closed); raises for a refused token;
    otherwise waits as the status says (past the relay's rate as for RESOURCE_EXHAUSTED)."""
    limited = _rate_limited(e)
    if e.status in _DENIED and not limited:
        raise RelayStopped("denied", f"{e.status.name}: {e.message}") from None
    if await closed():
        return True
    if limited or e.status == RPCStatusCode.RESOURCE_EXHAUSTED:
        await exhausted.wait()
    else:
        await other.wait()
    return False


async def _answer(
    service: RelayService,
    req: UpdateWorkflowExecutionRequest,
    closed: Callable[[], Awaitable[bool]],
    rpc_timeout: timedelta,
) -> tuple[Outcome, str] | None:
    """The outcome of one logical poll (re-sending the same request until it has one), or
    None when the job closed meanwhile."""
    exhausted = _Backoff(_BACKOFF_START, _EXHAUSTED_MAX)
    other = _Backoff(_BACKOFF_START, _BACKOFF_MAX)
    while True:
        try:
            resp = await service.update_workflow_execution(req, retry=False, timeout=rpc_timeout)
        except RPCError as e:
            if e.status in _RESEND:
                if await closed():
                    return None
                continue  # the same update id: the server finds the same update
            if await _rpc_failed(e, closed, exhausted, other):
                return None
            if e.status in _GONE:
                # Not admitted: a new logical poll (a fresh id) once the job is back.
                req.request.meta.update_id = uuid.uuid4().hex
            continue
        if resp.HasField("outcome"):
            return resp.outcome, resp.update_ref.workflow_execution.run_id
        if await closed():
            return None


async def _tail(
    service: RelayService,
    namespace: str,
    workflow_id: str,
    closed: Callable[[], Awaitable[bool]],
    rpc_timeout: timedelta,
) -> int | None:
    """The stream's current end (the marked offset answer), or None when the job closed."""
    req = QueryWorkflowRequest(
        namespace=namespace,
        execution=WorkflowExecution(workflow_id=workflow_id),
        query=WorkflowQuery(query_type=STREAM_OFFSET_QUERY),
    )
    exhausted = _Backoff(_BACKOFF_START, _EXHAUSTED_MAX)
    other = _Backoff(_BACKOFF_START, _BACKOFF_MAX)
    while True:
        try:
            resp = await service.query_workflow(req, retry=False, timeout=rpc_timeout)
        except RPCError as e:
            if e.status in _RESEND:
                if await closed():
                    return None
                continue
            if await _rpc_failed(e, closed, exhausted, other):
                return None
            continue
        if resp.HasField("query_rejected") or len(resp.query_result.payloads) != 1:
            raise RelayStopped("protocol", "the offset query answered no offset")
        try:
            return parse_offset(resp.query_result.payloads[0])
        except EnvelopeError as e:
            raise RelayStopped("protocol", str(e)) from None
