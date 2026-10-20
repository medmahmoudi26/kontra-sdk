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
  ``"tail"`` start; its answer must be the marked plaintext offset.

Each answer is a stream envelope (:mod:`kontra.codec.stream`), read without a key: a topic
the buyer may read is named in the clear, any other is ``null`` and dropped here; every
ciphertext is an age file (:func:`~kontra.codec.stream.parse_envelope` refuses anything
else, so plaintext never reaches a gate's storage). The loop yields one :class:`RelayBatch`
per answer; the caller stores its items, then its ``next_offset`` (the at-least-once
boundary), and passes that offset as ``start`` when it relays the same job again: a buyer's
own resume is served from the gate's store and never moves the relay.

Failures, by the outcome's ``ApplicationFailureInfo.type``: ``StreamDraining`` (the run
continues-as-new) is polled again after a short cooldown, ``TruncatedOffset`` from offset 0,
``AcceptedUpdateCompletedWorkflow`` again unless the job is closed, ``KontraStreamFull`` (the
job refuses polls past its history budget) stops with ``history_budget``; any other rejection
(no stream registered yet, say) backs off while the job is open -- rejections cost the job no
history. RPC errors: NOT_FOUND / FAILED_PRECONDITION stop when ``closed()`` says so, else back
off; RESOURCE_EXHAUSTED backs off up to a minute; PERMISSION_DENIED / UNAUTHENTICATED stop
with ``denied`` (the gate mints a new token or gives up). ``closed()`` is the gate's own
observer (decisions I6), asked after every failure and every empty wait. Between answers that
were not ``more_ready`` the loop waits :data:`MIN_INTERVAL` or what ``pace()`` says, whichever
is longer; ``pace()`` returning None stops it with ``history_budget`` (:func:`pace_for` is the
shared policy).

Imports ``temporalio.api`` protos, ``temporalio.service``, :mod:`kontra.contract` and
:mod:`kontra.codec.stream` -- never ``temporalio.contrib``: the gate depends on the wire shape,
not on the Public Preview package.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from dataclasses import dataclass
from datetime import timedelta
from typing import Literal, Protocol

from temporalio.api.common.v1 import Payload, Payloads, WorkflowExecution
from temporalio.api.enums.v1 import UpdateWorkflowExecutionLifecycleStage
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

from kontra.codec.stream import EnvelopeError, parse_envelope, parse_offset
from kontra.contract.names import STREAM_OFFSET_QUERY, STREAM_POLL_UPDATE, job_workflow_id
from kontra.contract.offering import check_topic

__all__ = [
    "MIN_INTERVAL",
    "STREAM_FULL",
    "RelayBatch",
    "RelayItem",
    "RelayService",
    "RelayStopped",
    "StopReason",
    "pace_for",
    "poll_input_payload",
    "relay",
]

#: The shortest wait between two polls that were not ``more_ready``.
MIN_INTERVAL = 2.0
#: The failure type a job root answers a poll with once its history is past the budget
#: (:mod:`kontra.serve.interceptors`).
STREAM_FULL = "KontraStreamFull"

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
_TOPICS_MAX = 64
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
    ciphertext -- or None for an item too large to relay (store a gap with ``size``)."""

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
    start: int | Literal["tail"] = "tail",
    first_execution_run_id: Callable[[], str] = lambda: "",
    pace: Callable[[], Awaitable[float | None]] | None = None,
    min_interval: float = MIN_INTERVAL,
    rpc_timeout: float = 30.0,
    identity: str = "kontra-relay",
) -> AsyncIterator[RelayBatch]:
    """Read ``job_id``'s stream (see the module docstring) until the job is closed, the
    caller stops iterating, or :class:`RelayStopped`. ``topics``: the offering's streams and
    ``kontra.progress``. ``start``: an offset (the gate's persisted cursor), or ``"tail"``
    (the stream's current end, read with the offset query)."""
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
                raise RelayStopped("history_budget", "the job refuses polls past its budget")
            if kind == _STREAM_DRAINING:
                await _sleep(_COOLDOWN)
            elif kind == _TRUNCATED_OFFSET:
                offset = 0
            elif kind == _COMPLETED_WORKFLOW:
                if await closed():
                    return
                await _sleep(_COOLDOWN)
            else:
                # Not written to the job's history: wait while the job is open.
                if await closed():
                    return
                await rejected.wait()
            continue
        rejected.reset()
        payloads = outcome.success.payloads
        if len(payloads) != 1:
            raise RelayStopped("protocol", "a poll answered other than one payload")
        try:
            envelope = parse_envelope(payloads[0])
        except EnvelopeError as e:
            raise RelayStopped("protocol", str(e)) from None
        if envelope.next_offset < offset and offset != 0:
            raise RelayStopped("protocol", "a poll answered an offset before the one asked")
        items: list[RelayItem] = []
        dropped = 0
        for item in envelope.items:
            if item.topic is None or item.topic not in shown:
                dropped += 1
                continue
            items.append(RelayItem(item.topic, item.offset, item.ciphertext, item.size))
        yield RelayBatch(tuple(items), envelope.next_offset, run_id, dropped)
        offset = envelope.next_offset
        if envelope.more_ready:
            continue
        wait = max(0.0, min_interval - (loop.time() - began))
        if pace is not None:
            paced = await pace()
            if paced is None:
                raise RelayStopped("history_budget", "the job's history is past half its limits")
            wait = max(wait, paced)
        if wait > 0:
            await _sleep(wait)


async def _rpc_failed(
    e: RPCError, closed: Callable[[], Awaitable[bool]], exhausted: _Backoff, other: _Backoff
) -> bool:
    """After a failed RPC: True to stop (the job is closed); raises for a refused token;
    otherwise waits as the status says."""
    if e.status in _DENIED:
        raise RelayStopped("denied", f"{e.status.name}: {e.message}") from None
    if await closed():
        return True
    if e.status == RPCStatusCode.RESOURCE_EXHAUSTED:
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
