"""A bound handler's job: one hire is one turn of the seller's agent (decisions I29).

``serve(ReconAgent.sweep)`` sells one ``@agent.accepts`` handler of an agent written with
Temporal's agent harness (``temporal-agent-harness``). An agent workflow cannot be the job
itself: its ``run`` returns nothing and only after a ``close`` signal, and a handler's reply is
not an update result either -- the ``send_agent_message`` update answers with an admission
receipt, and the reply is an event on the agent's workflow stream. So the generated
``execute`` starts Kontra's wrapper workflow, ``kontra.turn.v1``, as the job (``job-<id>``,
exactly as a workflow entry's job is started, so escrow, the codec's buyer rule and the job
header all work unchanged), and the wrapper:

1. starts the agent as a child (``args=[{}]``, an empty ``AgentConfig``) with the id
   :func:`~kontra.contract.names.agent_workflow_id`, which is not job-shaped: everything in
   the agent's history is encrypted to the seller alone;
2. runs one activity, ``kontra.turn.v1.deliver``, that sends the contract-validated input as
   the handler's message (an update whose id is stable across the activity's attempts, so a
   retry never sends it twice) and reads the agent's ``turn_events`` stream until the reply
   to that message;
3. signals ``close`` once the reply is in, waits for the agent to finish (its teardown runs
   before the job delivers), and returns the reply -- the job's result, encrypted to the
   buyer like any job result, and keyed by the output model's aliases, as a workflow
   entry's result is (the harness publishes it by field name).

A worker lost mid-turn -- a deploy, a crash -- costs nothing: the activity is retried on
another worker, its update id gets the first attempt's admission back, and it reads the
stream again from the same offset. temporalio's stream client ends its iteration, rather than
raising, when its poll is cancelled or times out, so the end of the stream is read as final
only by the wrapper, which sees the agent itself end.

The v1 constraint: a job is one turn. A handler that asks for a tool approval or a callback
would wait for a person or a client the buyer does not have, so it fails the job at once
(session jobs, with buyer approvals, are kontra-sdk#60). That includes a gated call an
agent's auto-mode evaluator would have decided: the harness publishes the approval request
*before* it consults the evaluator, and nothing on the stream says then whether a person will
be needed. A call the agent's ``ToolApprovalPolicy`` approves publishes no request at all, so
a handler sold on Kontra allow-lists the tools it uses. A handler that raises fails the job,
non-retryably, with the handler's message -- delivered encrypted to the buyer as the failure.
The agent ending before it replied fails the job too.

Kontra never imports the harness. What it relies on is the harness's wire shape, written out
once below (``tests/serve/test_turn.py`` checks it against the harness, the golden pattern):
the update and signal names, the message envelope and the reply's fields, the stream topic,
the event types and the event envelope's fields, and the attribute names the decorators
stamp. The stream is read with temporalio's own ``workflow_streams`` client.

The activity heartbeats for liveness only: :class:`~kontra.serve.interceptors.KontraInterceptor`
never forwards ``kontra.turn.v1.deliver``'s heartbeats to the buyer as progress.

**Streams** (decisions I31). The gate's relay reads the job's own stream (``job-<id>``,
hosted by Kontra's interceptor on the wrapper), never the agent child's, whose payloads are
sealed to the seller alone. So the activity, which reads the agent's stream anyway, mirrors
the topics the offering lists (``offering.yaml`` ``streams:``) onto the job's stream, under
the same names -- ``streams: [turn_events]`` relays the agent's events to the buyer -- as
the agent published them (each item's ``Payload``, untouched). Unlisted topics are never
copied. It flushes what it mirrored every half second and before it returns the reply, so
the events are in the job's stream before the job can complete, and checkpoints the last
agent offset flushed in its heartbeat details: a retried attempt mirrors from there, so a
lost worker re-publishes at most one flush window.
"""

from __future__ import annotations

import asyncio
import contextlib
import inspect
from collections.abc import AsyncGenerator, AsyncIterator, Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, cast

from pydantic import BaseModel, ValidationError
from temporalio import activity, workflow
from temporalio.api.common.v1 import Payload
from temporalio.client import Client, WorkflowUpdateFailedError
from temporalio.common import RawValue, RetryPolicy
from temporalio.exceptions import ActivityError, ApplicationError, ChildWorkflowError

with workflow.unsafe.imports_passed_through():
    from temporalio.contrib.workflow_streams import WorkflowStreamClient

    from kontra.contract.names import agent_workflow_id, job_id_from_workflow_id

__all__ = [
    "TURN_ACTIVITY",
    "TURN_WORKFLOW",
    "StreamEnded",
    "TurnActivities",
    "TurnJob",
    "TurnRequest",
    "TurnWorkflow",
    "as_output",
    "await_reply",
    "read_reply",
]

#: The wrapper workflow's Temporal name: the job of a bound-handler offering.
TURN_WORKFLOW = "kontra.turn.v1"
#: The activity that delivers the one message and reads its reply.
TURN_ACTIVITY = "kontra.turn.v1.deliver"

# --------------------------------------------------------------------------- the wire shape
#
# temporal-agent-harness's protocol, as Kontra relies on it (0.6.0 and 0.7.0 are identical
# here). Constants only: Kontra must never import the harness.

#: The update that admits one message: ``{type: <handler name>, payload: <input JSON>}``.
SEND_AGENT_MESSAGE_UPDATE = "send_agent_message"
#: The message envelope's fields.
MESSAGE_TYPE = "type"
MESSAGE_PAYLOAD = "payload"
#: The fields of the update's reply (``AgentMessageReply``).
REPLY_FIELDS = ("turn_number", "turn_id", "message_id", "disposition", "accepted_offset")
#: The signal that ends the agent's session; its ``run`` returns once in-flight turns drain.
CLOSE_SIGNAL = "close"
#: The workflow-stream topic the agent publishes its events on.
TURN_EVENTS_TOPIC = "turn_events"
#: Event types (``AgentEventType``) Kontra acts on.
MESSAGE_HANDLER_END = "message_handler_end"
MESSAGE_HANDLER_ERROR = "message_handler_error"
TURN_END = "turn_end"
TOOL_APPROVAL_REQUESTED = "tool_approval_requested"
CALLBACK_REQUESTED = "callback_requested"
#: The event envelope's fields (``AgentEvent``) and the event body's.
EVENT_MESSAGE_ID = "message_id"
EVENT_TURN_ID = "turn_id"
EVENT = "event"
EVENT_TYPE = "type"
EVENT_OUTPUT = "output"
EVENT_MESSAGE = "message"
#: What ``@agent.accepts`` stamps on the handler, and ``@agent.defn`` on the class.
AGENT_ACCEPTS = "__agent_accepts__"
AGENT_HANDLERS = "__agent_handlers__"
AGENT_INIT_DATA = "__agent_init_data__"

# --------------------------------------------------------------------------- the job

#: The activity's heartbeat timeout: a worker that died is noticed this soon.
_HEARTBEAT_TIMEOUT = timedelta(seconds=30)
#: How long the job may take when nothing bounds it (a direct start, in tests).
_UNBOUNDED = timedelta(days=1)
#: The failure type of everything the wrapper refuses or reports.
_FAILURE = "KontraTurn"
#: How often the stream is polled once caught up.
_POLL = timedelta(milliseconds=50)
#: How often mirrored items are flushed to the job's stream (and checkpointed).
_MIRROR_FLUSH = 0.5
_ONE_TURN = (
    "a Kontra job is one turn without buyer approvals or callbacks (v1; session jobs are "
    "kontra-sdk#60)"
)
#: What the handler asked for, by event type, in the failure that refuses it.
_ASKED = {TOOL_APPROVAL_REQUESTED: "a tool approval", CALLBACK_REQUESTED: "a callback"}


@dataclass
class TurnJob:
    """The wrapper's input, built by the generated ``execute`` from the binding (static) and
    the contract-validated input."""

    #: The agent's Temporal workflow type.
    agent: str
    #: The ``@agent.accepts`` handler's name: the message's ``type``.
    handler: str
    #: The input, ``model_dump(mode="json", by_alias=True)``: the message's ``payload``,
    #: keyed as the harness validates it (by alias).
    payload: dict[str, Any]


@dataclass
class TurnRequest:
    """The activity's input."""

    agent_workflow_id: str
    handler: str
    payload: dict[str, Any]


def _failure(message: str) -> ApplicationError:
    return ApplicationError(message, type=_FAILURE, non_retryable=True)


class StreamEnded(Exception):
    """The agent's event stream ended before the reply: the agent ended, or the reading was
    cancelled or timed out (temporalio's stream client says all three the same way)."""


def _remaining() -> timedelta:
    """What is left of the job's execution timeout (the hold's, set by ``execute``)."""
    info = workflow.info()
    if info.execution_timeout is None:
        return _UNBOUNDED
    left = info.workflow_start_time + info.execution_timeout - workflow.now()
    return max(left, timedelta(seconds=1))


@workflow.defn(name=TURN_WORKFLOW)
class TurnWorkflow:
    """``kontra.turn.v1``: one turn of the agent, as a job (see the module docstring)."""

    @workflow.run
    async def run(self, job: TurnJob) -> dict[str, Any]:
        job_id = job_id_from_workflow_id(workflow.info().workflow_id)
        if job_id is None:
            raise _failure(f"{TURN_WORKFLOW} runs only as a Kontra job (job-<id>)")
        child_id = agent_workflow_id(job_id)
        left = _remaining()
        child = await workflow.start_child_workflow(
            job.agent,
            args=[{}],  # an empty AgentConfig: the agent's own defaults apply
            id=child_id,
            task_queue=workflow.info().task_queue,
            execution_timeout=left,
            parent_close_policy=workflow.ParentClosePolicy.TERMINATE,
        )
        turn = workflow.start_activity(
            TURN_ACTIVITY,
            TurnRequest(child_id, job.handler, job.payload),
            result_type=dict[str, Any],
            schedule_to_close_timeout=left,
            heartbeat_timeout=_HEARTBEAT_TIMEOUT,
            # Failures it reports are final (non-retryable); a worker lost mid-turn retries,
            # and the update id makes the retry pick up the same message. The agent ending
            # is noticed here, by the wait below, not by the activity.
            retry_policy=RetryPolicy(maximum_interval=timedelta(seconds=10)),
        )
        await workflow.wait([turn, child], return_when=asyncio.FIRST_COMPLETED)
        if not turn.done():
            turn.cancel()
            with contextlib.suppress(ActivityError, asyncio.CancelledError):
                await turn
            try:
                await child
            except ChildWorkflowError as e:
                raise _failure("the agent ended before replying") from e
            raise _failure("the agent ended before replying")
        try:
            reply = await turn
        except ActivityError as e:
            cause = e.cause
            if isinstance(cause, ApplicationError):
                raise _failure(cause.message) from None
            raise
        await child.signal(CLOSE_SIGNAL)
        with contextlib.suppress(ChildWorkflowError):
            await child  # its teardown, before the job delivers
        return reply


# --------------------------------------------------------------------------- the activity


class TurnActivities:
    """``kontra.turn.v1.deliver``, bound to the worker's own client (it talks to the agent, a
    child of the job: :class:`~kontra.plugin.KontraPlugin` binds it when it builds a
    bound-handler offering's worker), to the handler's output model (the reply is re-keyed
    by its aliases, :func:`as_output`), and to the topics it mirrors onto the job's stream
    (``mirror``: the offering's ``streams``)."""

    def __init__(
        self,
        client: Client,
        output: type[BaseModel] | None = None,
        mirror: Iterable[str] = frozenset(),
    ) -> None:
        self._client = client
        self._output = output
        if isinstance(mirror, str):
            raise TypeError("mirror is a collection of topic names, not one name")
        self._mirror = frozenset(mirror)

    @activity.defn(name=TURN_ACTIVITY)
    async def deliver(self, req: TurnRequest) -> dict[str, Any]:
        """Send the one message, then read the agent's stream until its reply."""
        info = activity.info()
        handle = self._client.get_workflow_handle(req.agent_workflow_id)
        try:
            # Keyed on this activity (its run and id, shared by every attempt): a retry gets
            # the first attempt's admission back instead of sending the message twice.
            reply = await handle.execute_update(
                SEND_AGENT_MESSAGE_UPDATE,
                {MESSAGE_TYPE: req.handler, MESSAGE_PAYLOAD: req.payload},
                id=f"kontra-turn:{info.workflow_run_id}:{info.activity_id}",
                result_type=dict[str, Any],
            )
        except WorkflowUpdateFailedError as e:
            raise _failure(f"the agent refused the message: {e.cause}") from None
        mirror = self._mirror_for(reply) if self._mirror else None
        async with _heartbeating(mirror.details if mirror is not None else tuple):
            return as_output(self._output, await self._reply(req, reply, mirror))

    def _mirror_for(self, reply: Mapping[str, Any]) -> _Mirror | None:
        offset = reply.get("accepted_offset")
        job = activity.info().workflow_id
        if not isinstance(offset, int) or job is None:
            return None  # _reply refuses the admission
        details = activity.info().heartbeat_details
        resumed = details[0] if details else None
        after = offset - 1
        if isinstance(resumed, int) and not isinstance(resumed, bool) and resumed >= after:
            after = resumed  # a retry: what an earlier attempt flushed is in the job's stream
        return _Mirror(WorkflowStreamClient.create(self._client, job), self._mirror, after)

    async def _reply(
        self, req: TurnRequest, reply: Mapping[str, Any], mirror: _Mirror | None = None
    ) -> dict[str, Any]:
        message_id, offset = reply.get("message_id"), reply.get("accepted_offset")
        if not isinstance(message_id, str) or not isinstance(offset, int):
            raise _failure("the agent's admission names no message (not a harness agent?)")
        stream = WorkflowStreamClient.create(self._client, req.agent_workflow_id)
        if mirror is None:
            items: AsyncIterator[Any] = stream.subscribe(
                topics=[TURN_EVENTS_TOPIC],
                from_offset=offset,
                result_type=dict[str, Any],
                poll_cooldown=_POLL,
            )
            return await read_reply(
                (item.data async for item in items), message_id, reply.get("turn_id")
            )
        raw: AsyncIterator[Any] = stream.subscribe(
            topics=sorted({TURN_EVENTS_TOPIC, *self._mirror}),
            from_offset=offset,
            result_type=RawValue,
            poll_cooldown=_POLL,
        )
        converter = self._client.data_converter.payload_converter

        async def events() -> AsyncIterator[object]:
            async for item in raw:
                payload: Payload = item.data.payload
                mirror.offer(item.topic, item.offset, payload)
                if item.topic == TURN_EVENTS_TOPIC:
                    try:
                        yield converter.from_payload(payload, dict[str, Any])
                    except Exception:  # an item is untrusted: one that will not decode is not ours
                        yield None

        async with mirror.flushing():
            out = await read_reply(events(), message_id, reply.get("turn_id"))
            await mirror.flush()  # the events are in the job's stream before the reply
        return out


class _Mirror:
    """The listed topics of the agent's stream, published onto the job's stream under the
    same names, each item's ``Payload`` as the agent wrote it. ``after``: the agent offset
    already in the job's stream (an earlier attempt's checkpoint)."""

    def __init__(self, job: WorkflowStreamClient, topics: frozenset[str], after: int) -> None:
        self._job = job
        self._topics = topics
        self._after = after
        #: The last agent offset published (buffered) and flushed (checkpointed).
        self._published = after
        self.flushed = after
        self._lock = asyncio.Lock()

    def offer(self, topic: str, offset: int, payload: Payload) -> None:
        if topic in self._topics and offset > self._after:
            self._job.topic(topic).publish(payload)
            self._published = max(self._published, offset)

    def details(self) -> tuple[Any, ...]:
        """The heartbeat's details: the checkpoint, so a liveness beat never erases it."""
        return (self.flushed,)

    async def flush(self) -> None:
        async with self._lock:
            target = self._published
            if target <= self.flushed:
                return
            await self._job.flush()
            self.flushed = target
            activity.heartbeat(*self.details())

    @contextlib.asynccontextmanager
    async def flushing(self) -> AsyncGenerator[None]:
        """Flush every :data:`_MIRROR_FLUSH` seconds while the block runs."""

        async def loop() -> None:
            while True:
                await asyncio.sleep(_MIRROR_FLUSH)
                await self.flush()

        task = asyncio.create_task(loop())
        try:
            yield
        finally:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task


async def read_reply(
    events: AsyncIterator[object], message_id: str, turn_id: object
) -> dict[str, Any]:
    """:func:`await_reply`, in the activity: the stream ending first is never final here.
    Cancelled (the wrapper saw the agent end, or the worker is stopping), it is the
    activity's cancellation, which a stopping worker reports as a retryable failure;
    otherwise (a poll that timed out, the agent closing) a retryable failure. Either way the
    wrapper decides: a retry resumes the same message from the same offset, and the agent
    having ended fails the job there."""
    try:
        return await await_reply(events, message_id, turn_id)
    except StreamEnded:
        pass
    if activity.is_cancelled() or activity.is_worker_shutdown():
        raise asyncio.CancelledError
    raise ApplicationError(
        "the agent's event stream ended before its reply; reading it again", type=_FAILURE
    )


class _Reply:
    """:func:`await_reply`'s logic, one event at a time."""

    def __init__(self, message_id: str, turn_id: object) -> None:
        self.message_id, self.turn_id = message_id, turn_id

    def feed(self, event: object) -> dict[str, Any] | None:
        """The reply, if ``event`` is it; raises the failure that ends the job; else None."""
        envelope = _mapping(event)
        body = _mapping(envelope.get(EVENT))
        kind = body.get(EVENT_TYPE)
        if envelope.get(EVENT_MESSAGE_ID) == self.message_id:
            if kind == MESSAGE_HANDLER_END:
                output = body.get(EVENT_OUTPUT)
                if not isinstance(output, dict):
                    raise _failure("the agent's reply is not an object")
                return cast(dict[str, Any], output)
            if kind == MESSAGE_HANDLER_ERROR:
                raise _failure(str(body.get(EVENT_MESSAGE) or "the handler failed"))
            if isinstance(kind, str) and kind in _ASKED:
                raise _failure(f"the handler asked for {_ASKED[kind]}: {_ONE_TURN}")
        elif kind == TURN_END and envelope.get(EVENT_TURN_ID) == self.turn_id:
            raise _failure("the agent's turn ended without a reply")
        return None


async def await_reply(
    events: AsyncIterator[object], message_id: str, turn_id: object
) -> dict[str, Any]:
    """The reply to ``message_id`` among the agent's events (each an ``AgentEvent`` envelope
    as JSON), or the failure that ends the job: the handler's error, a tool approval or a
    callback asked for (a job is one turn), or the turn ending without it. The stream
    ending first is :class:`StreamEnded`, not a failure: :func:`read_reply` says what it
    means. Events of other messages are not ours and are passed over."""
    reply = _Reply(message_id, turn_id)
    async for event in events:
        out = reply.feed(event)
        if out is not None:
            return out
    raise StreamEnded


#: ``model_validate``'s ``by_alias``/``by_name`` (pydantic 2.11; kontra-sdk takes 2.12 up).
_BY_NAME = "by_name" in inspect.signature(BaseModel.model_validate).parameters


def as_output(model: type[BaseModel] | None, reply: dict[str, Any]) -> dict[str, Any]:
    """The handler's reply as the job's result. The harness publishes it as
    ``model_dump(mode="json")``, keyed by field name; a job's result is the output model's
    JSON keyed by alias -- what temporalio's pydantic converter writes for a workflow
    entry's result, and what the contract and the buyer's model read. Validated by name (or
    alias), dumped by alias; a reply that is not the model fails the job."""
    if model is None:
        return reply
    try:
        if _BY_NAME:
            value = model.model_validate(reply, by_alias=True, by_name=True)
        else:  # pragma: no cover - pydantic < 2.11 validates by alias only
            value = model.model_validate(reply)
    except ValidationError as e:
        # Where and what, never the value: the reply is the buyer's output.
        said = "; ".join(f"{'.'.join(map(str, err['loc']))}: {err['msg']}" for err in e.errors())
        raise _failure(f"the handler's reply is not a {model.__name__} ({said})") from None
    return value.model_dump(mode="json", by_alias=True)


def _mapping(value: object) -> Mapping[str, Any]:
    """``value`` if it is a JSON object, else an empty one (a stream item is untrusted)."""
    return cast(Mapping[str, Any], value) if isinstance(value, Mapping) else {}


@contextlib.asynccontextmanager
async def _heartbeating(details: Callable[[], tuple[Any, ...]] = tuple) -> AsyncGenerator[None]:
    """Heartbeat every half heartbeat timeout while the block runs, with ``details()`` (the
    mirror's checkpoint, which a beat without it would erase; none without a mirror)."""
    timeout = activity.info().heartbeat_timeout or _HEARTBEAT_TIMEOUT
    interval = timeout.total_seconds() / 2

    async def beat() -> None:
        while True:
            activity.heartbeat(*details())
            await asyncio.sleep(interval)

    task = asyncio.create_task(beat())
    try:
        yield
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
