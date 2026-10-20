"""Worker interceptors: which job a piece of seller code is working for, its heartbeats, and
the job's workflow stream.

A job is the handler workflow ``job-<id>`` *and everything it starts*: activities, local
activities, child workflows, continue-as-new runs. Only the root knows its job from its
workflow id, so the job id travels down the tree in a Temporal header, ``kontra-job``.
The header is a plain JSON string, deliberately not codec-encoded: a job id is not secret
(the gate assigned it), and a header the codec had to decrypt could not be read before the
payload keys are known.

* Workflow side (:class:`_WorkflowInbound`): the job is the workflow's own (root) or the
  incoming header's (anything below it); the outbound interceptor stamps it on every
  activity, local activity, child workflow and continue-as-new it schedules.
* Activity side (:class:`_ActivityInbound`): reads the header, sets
  :func:`kontra._runtime.job_context` for the activity's duration (so
  ``Artifact.from_path`` uploads for the right job) and wraps the outbound interceptor so
  every ``activity.heartbeat(*details)`` -- before the SDK throttles it -- is offered to the
  progress forwarder, then passed on unchanged -- except Kontra's own turn activity's
  (:data:`~kontra.serve.turn.TURN_ACTIVITY`), which heartbeats for liveness, not for the
  buyer (:mod:`kontra.serve.turn`). When the activity returns, the forwarder
  gets a short, bounded chance to publish the heartbeats that are due
  (:meth:`~kontra.serve.progress.ProgressForwarder.settle`), so they are in the job's stream
  before the job closes; a slow Temporal does not hold activities up.

**The job's workflow stream** (decisions I31). The gate's relay reaches ``job-<id>`` and
nothing else, so the root hosts the stream (``temporalio.contrib.workflow_streams``):

* **Host if absent.** When the root's run starts and its code built no ``WorkflowStream``
  (no publish signal handler), Kontra builds one (:class:`_HostedStream`, the contrib's own
  class). Publish signals that arrived first are buffered by temporalio and delivered then.
* **Early handlers.** In a run's first activation, updates are applied before the workflow
  is initialised; a poll then would be rejected and lost. :meth:`_WorkflowInbound.init`
  registers placeholders (a poll that waits for the stream, an offset of 0), which the
  stream replaces.
* **Marking.** The answer of a poll (and of the offset query) on a root is handed to the
  codec as a ``RawValue`` with Kontra's metadata -- the only channel from here to the codec,
  which runs outside the sandbox -- with the topics the buyer may read: the offering's
  ``streams`` (by task queue) and ``kontra.progress``. :mod:`kontra.codec.stream` seals it
  item by item. Any other answer (a seller who replaced the handler) is left alone and
  encrypted whole: the relay then stops, and nothing leaks. Before marking, the answer is
  cut where its sealed envelope could pass the envelope's limit (the contrib's own
  truncation: ``next_offset`` at the first item left out, ``more_ready``), so the codec
  never trims it and what this run counts as read is what the poller is sent.
* **History guard.** A poll is refused at validation, ``KontraStreamFull``, once the run's
  history is past half of Kontra's pinned limits (:data:`STREAM_HISTORY_EVENTS`,
  :data:`STREAM_HISTORY_BYTES`). A rejected update is not written to history, so past it no
  poll adds to the history: the relay's share stays near half of the limits (a job someone
  listens to must keep its own work under the other half). It bounds history growth only.
  A burst of oversized poll requests would end a run whatever this answers (the SDK echoes
  refused updates too): the Authorizer refuses a relay request over 4 KiB (decisions I30),
  and with both, what a relay token's polls do to the job's history is bounded. The answers
  are the answer budget's.
* **Answer budget.** The polls one workflow task completes answer together, in the worker's
  reply to that task, which must fit gRPC's 4 MiB message or Temporal ends the run. A root
  answers at most :data:`STREAM_ANSWER_BUDGET` (1.5 MiB) in one workflow task; a poll past
  it is answered with no items and ``more_ready``, at the offset it would have started from,
  so its poller polls again and is answered in a later task. The first answer of a task is
  always sent, so the stream makes progress, and one held back is not counted as read (the
  drain below waits for it). The task is told apart by the run's history length, which is
  the same for every answer of one task, and the same on replay.
* **Drain.** A relay sleeps between polls, so a job that publishes and returns would close
  before the next poll. When the run returns or fails and any poll was seen in the chain,
  the root waits (at most :data:`DRAIN_TIMEOUT`) for a poll to have answered up to the
  stream's end. On continue-as-new it drains too, detaches the pollers, and carries the
  stream's end to the next run in a plaintext header (:data:`STREAM_BASE_HEADER`), so
  offsets stay monotonic across the chain; a stream the seller hosts follows the contrib's
  own recipe (``prior_state``) instead.

This module runs inside workflow code: what it calls there is temporalio, the standard
library, :mod:`kontra.contract.names`, :mod:`kontra._runtime` (a context variable; no I/O,
deterministic), the pure marking helpers of :mod:`kontra.codec.stream` and the turn
activity's name, and the workflow interceptor class is handed to the sandbox as an object.

Known gap: a *descendant* workflow learns its job when its run method starts; an activity
scheduled by a signal handler that runs before that (a signal in the very first activation)
carries no job, so its heartbeats are not forwarded. The root is never affected.
"""

from __future__ import annotations

import contextlib
import json
import logging
import re
from collections.abc import Iterable, Mapping
from datetime import timedelta
from typing import Any, NoReturn, Protocol

from temporalio import activity, workflow
from temporalio.api.common.v1 import Payload
from temporalio.common import RawValue
from temporalio.contrib.workflow_streams import (
    PollInput,
    PollResult,
    WorkflowStream,
    WorkflowStreamState,
)
from temporalio.exceptions import ApplicationError, FailureError
from temporalio.worker import (
    ActivityInboundInterceptor,
    ActivityOutboundInterceptor,
    ContinueAsNewInput,
    ExecuteActivityInput,
    ExecuteWorkflowInput,
    HandleQueryInput,
    HandleUpdateInput,
    Interceptor,
    StartActivityInput,
    StartChildWorkflowInput,
    StartLocalActivityInput,
    WorkflowInboundInterceptor,
    WorkflowInterceptorClassInput,
    WorkflowOutboundInterceptor,
)

from kontra import _runtime
from kontra.codec.stream import (
    BUYER_TOPICS_KEY,
    ENVELOPE_MAX,
    POLL_MARK,
    STREAM_MARK,
    buyer_topics,
    envelope_bound,
    items_that_fit,
)
from kontra.codec.stream import offset_payload as _offset_payload
from kontra.contract.names import (
    PROGRESS_TOPIC,
    STREAM_OFFSET_QUERY,
    STREAM_POLL_UPDATE,
    STREAM_PUBLISH_SIGNAL,
    is_job_id,
    job_id_from_workflow_id,
)
from kontra.serve.turn import TURN_ACTIVITY

__all__ = [
    "DRAIN_TIMEOUT",
    "JOB_HEADER",
    "RECIPIENTS_BOUND",
    "STREAM_ANSWER_BUDGET",
    "STREAM_BASE_HEADER",
    "STREAM_FULL",
    "STREAM_HISTORY_BYTES",
    "STREAM_HISTORY_EVENTS",
    "STREAM_POLLED_HEADER",
    "KontraInterceptor",
    "ProgressSink",
    "job_from_headers",
    "job_header",
]

log = logging.getLogger("kontra.serve.interceptors")

#: The Temporal header that carries the job id below the root handler workflow.
JOB_HEADER = "kontra-job"
#: On a root's continue-as-new: the end of the stream Kontra hosts (the next run's base
#: offset), and that a poll was seen in the chain (the next run drains). Plain JSON: a log
#: length is not secret.
STREAM_BASE_HEADER = "kontra-stream-base"
STREAM_POLLED_HEADER = "kontra-stream-polled"
#: The longest a returning (or continuing) root waits for the relay to read its stream's end.
DRAIN_TIMEOUT = timedelta(seconds=5)
#: Half of Kontra's pinned ``limit.historyCount.error`` (20,480) and
#: ``limit.historySize.error`` (20 MiB) (``temporal/authorizer/dynamicconfig/kontra.yaml``):
#: past either, a root refuses polls.
STREAM_HISTORY_EVENTS = 10_240
STREAM_HISTORY_BYTES = 10 * 1024 * 1024
#: The failure type of a poll refused past the history budget (``kontra._relay``).
STREAM_FULL = "KontraStreamFull"
#: The most keys a job's stream item is sealed to: a party's registered keys (the gate
#: keeps at most 16, ``gate_client.models.RECIPIENTS_MAX``) and the job's buyer key. A poll's
#: answer is cut to what surely fits one envelope sealed to that many.
RECIPIENTS_BOUND = 16 + 1
#: The most a job root answers its stream's polls with in one workflow task, counted with
#: :func:`~kontra.codec.stream.envelope_bound` over the answers that carry items: past it, a
#: poll is answered with no items and ``more_ready``, and its poller polls again (decisions
#: I31). The worker's reply to a workflow task carries every answer completed in it, and must
#: fit gRPC's 4 MiB. Three full envelopes (1.5 MiB) leave the rest of that reply 2.5 MiB: the
#: update requests it echoes (a relay's are at most 4 KiB, ``RelayRequestMax``, and at most
#: ``history.maxInFlightUpdates``, 10, are in flight: 40 KiB), the answers with no items (a
#: frame each), and the seller's own commands, one payload at Kontra's pinned
#: ``limit.blobSize.error`` (2 MiB) included.
STREAM_ANSWER_BUDGET = 3 * ENVELOPE_MAX
_JSON_PLAIN = b"json/plain"
_INT = re.compile(rb"0|[1-9][0-9]{0,18}", re.ASCII)


class ProgressSink(Protocol):
    """What the activity interceptor needs of :class:`kontra.serve.progress.ProgressForwarder`."""

    def offer(self, job_id: str, activity: str, details: tuple[Any, ...]) -> None: ...

    async def settle(self, job_id: str) -> None: ...


def job_header(job_id: str) -> Payload:
    """The header payload for ``job_id``: plain JSON, never codec-encoded."""
    return Payload(metadata={"encoding": _JSON_PLAIN}, data=json.dumps(job_id).encode("ascii"))


def job_from_headers(headers: Mapping[str, Payload]) -> str | None:
    """The job id in ``headers``, if it is one; anything else is ignored, never trusted."""
    p = headers.get(JOB_HEADER)
    if p is None or p.metadata.get("encoding") != _JSON_PLAIN or len(p.data) > 64:
        return None
    try:
        value = json.loads(p.data)
    except ValueError:
        return None
    return value if is_job_id(value) else None


def _with_job(headers: Mapping[str, Payload], job_id: str | None) -> Mapping[str, Payload]:
    if job_id is None:
        return headers
    return {**headers, JOB_HEADER: job_header(job_id)}


def _int_header(headers: Mapping[str, Payload], name: str) -> int | None:
    p = headers.get(name)
    if p is None or p.metadata.get("encoding") != _JSON_PLAIN or _INT.fullmatch(p.data) is None:
        return None
    return int(p.data)


def _int_payload(n: int) -> Payload:
    return Payload(metadata={"encoding": _JSON_PLAIN}, data=str(n).encode("ascii"))


# --------------------------------------------------------------------------- workflows


class _HostedStream(WorkflowStream):
    """The stream Kontra hosts on a root whose code hosts none: the contrib's own
    implementation. ``WorkflowStream`` insists on being built from a function named
    ``__init__`` (it reads its caller's frame); this is one."""

    def __init__(self, prior_state: WorkflowStreamState | None = None) -> None:
        super().__init__(prior_state)


class _WorkflowInbound(WorkflowInboundInterceptor):
    """Learns the workflow's job: its own id (the root ``job-<id>``) or the parent's header;
    on a root, hosts, marks, guards and drains the job's stream (module docstring)."""

    #: The topics a buyer may read, by task queue (one queue per offering); set on the
    #: subclass :class:`KontraInterceptor` hands the worker.
    _listed: Mapping[str, frozenset[str]] = {}

    _header_job: str | None = None
    _root: bool = False
    #: The stream Kontra hosts on this run, if it hosts one.
    _hosted: _HostedStream | None = None
    #: The run's stream exists (hosted, or the seller's): early polls may go on.
    _ready: bool = False
    #: A poll was seen in this chain (this run, or a run before it: the polled header).
    _polled: bool = False
    _in_flight: int = 0
    #: The largest ``next_offset`` of a poll answered with ``more_ready`` false.
    _answered_to: int = 0
    #: The workflow task whose answers :attr:`_task_answered` counts (its history length, see
    #: :meth:`_budget`), and the bytes answered in it.
    _task: int = -1
    _task_answered: int = 0
    #: The headers of the continue-as-new this run raised (the base is rewritten there).
    _can_headers: dict[str, Payload] | None = None

    def init(self, outbound: WorkflowOutboundInterceptor) -> None:
        super().init(_WorkflowOutbound(outbound, self))
        # workflow.info() needs the instance's outbound, set only once init() returns; the
        # outbound handed to us answers it.
        self._root = job_id_from_workflow_id(outbound.info().workflow_id) is not None
        if self._root:
            # Before the workflow object exists: a poll in the run's first activation
            # (applied before the workflow is initialised) finds a handler, waits for the
            # stream, and is answered by it. The stream replaces both.
            workflow.set_update_handler(STREAM_POLL_UPDATE, self._early_poll)
            workflow.set_query_handler(STREAM_OFFSET_QUERY, self._early_offset)

    async def _early_poll(self, payload: PollInput) -> PollResult:
        await workflow.wait_condition(lambda: self._ready)
        real = workflow.get_update_handler(STREAM_POLL_UPDATE)
        if real is None or real == self._early_poll:  # the seller unset it: no stream
            raise ApplicationError("this job hosts no workflow stream", non_retryable=True)
        return await real(payload)

    def _early_offset(self) -> int:
        return 0

    @property
    def job_id(self) -> str | None:
        # The root is known from its id at any time (a signal handler may schedule work
        # before the run method starts); a descendant only once its input headers are read.
        return job_id_from_workflow_id(workflow.info().workflow_id) or self._header_job

    async def execute_workflow(self, input: ExecuteWorkflowInput) -> Any:
        self._header_job = job_from_headers(input.headers)
        if not self._root:
            return await super().execute_workflow(input)
        if workflow.get_signal_handler(STREAM_PUBLISH_SIGNAL) is None:
            # The seller's __init__ has run: a stream it built is registered by now.
            base = _int_header(input.headers, STREAM_BASE_HEADER) or 0
            self._hosted = _HostedStream(WorkflowStreamState(base_offset=base) if base else None)
        if _int_header(input.headers, STREAM_POLLED_HEADER) == 1:
            self._polled = True
        self._ready = True
        try:
            result = await super().execute_workflow(input)
        except workflow.ContinueAsNewError:
            await self._before_continue_as_new()
            raise
        except FailureError:
            await self._drain()
            raise
        await self._drain()
        return result

    def _end(self) -> int | None:
        end_of = workflow.get_query_handler(STREAM_OFFSET_QUERY)
        if end_of is None or end_of == self._early_offset:
            return None
        end = end_of()
        return end if isinstance(end, int) and not isinstance(end, bool) else None

    async def _drain(self) -> None:
        """Wait (bounded) for a poll to have answered up to the stream's end, when anyone
        polled in this chain: what was published last reaches the relay before the job
        closes. Nobody listening: no wait. No drain on cancellation."""
        if not self._polled:
            return
        end = self._end()
        if end is None:
            return
        try:
            await workflow.wait_condition(lambda: self._answered_to >= end, timeout=DRAIN_TIMEOUT)
        except TimeoutError:
            log.debug("the job's stream was not read to its end within %s", DRAIN_TIMEOUT)
        await self._release_pollers()

    async def _release_pollers(self) -> None:
        """Answer the polls still waiting on the job's stream (they get what there is,
        nothing), so the run does not end under an update handler that is still running:
        no "unfinished update handlers" warning on every job a buyer listened to. The relay
        polls again and is told the stream is draining until the job is closed. The run is
        ending: detaching a stream the seller hosts changes nothing it could still read."""
        if self._in_flight == 0:
            return
        stream: WorkflowStream | None = self._hosted
        if stream is None:
            owner = getattr(workflow.get_update_handler(STREAM_POLL_UPDATE), "__self__", None)
            stream = owner if isinstance(owner, WorkflowStream) else None
        if stream is None:
            return
        stream.detach_pollers()
        with contextlib.suppress(TimeoutError):
            await workflow.wait_condition(lambda: self._in_flight == 0, timeout=DRAIN_TIMEOUT)

    async def _before_continue_as_new(self) -> None:
        """The seller continues-as-new: what the relay has not read of the stream Kontra
        hosts would be lost with this run, so drain, detach the pollers (they follow the
        chain), and carry the stream's end, as it is now, in the same headers the
        continue-as-new command reads when the error reaches the top."""
        hosted = self._hosted
        if hosted is None:
            return
        if self._polled:
            await self._drain()  # and the pollers released: they follow the chain
            hosted.detach_pollers()
        headers = self._can_headers
        end = self._end()
        if headers is not None and end is not None:
            headers[STREAM_BASE_HEADER] = _int_payload(end)

    def stamp_continue_as_new(self, headers: Mapping[str, Payload]) -> Mapping[str, Payload]:
        """The headers of this root's continue-as-new: the stream's end and whether anyone
        polled, kept so :meth:`_before_continue_as_new` can rewrite the end."""
        if self._hosted is None:
            return headers
        stamped = dict(headers)
        end = self._end()
        if end is not None:
            stamped[STREAM_BASE_HEADER] = _int_payload(end)
        if self._polled:
            stamped[STREAM_POLLED_HEADER] = _int_payload(1)
        self._can_headers = stamped
        return stamped

    def handle_update_validator(self, input: HandleUpdateInput) -> None:
        if self._root and input.update == STREAM_POLL_UPDATE:
            info = workflow.info()
            if (
                info.get_current_history_length() >= STREAM_HISTORY_EVENTS
                or info.get_current_history_size() >= STREAM_HISTORY_BYTES
            ):
                # Rejected at validation: not written to history.
                raise ApplicationError(
                    "the job's history is past its stream budget: no more polls",
                    type=STREAM_FULL,
                    non_retryable=True,
                )
        super().handle_update_validator(input)

    async def handle_update_handler(self, input: HandleUpdateInput) -> Any:
        if not (self._root and input.update == STREAM_POLL_UPDATE):
            return await super().handle_update_handler(input)
        self._polled = True
        self._in_flight += 1
        try:
            result = await super().handle_update_handler(input)
        finally:
            self._in_flight -= 1
        if not isinstance(result, PollResult):
            return result  # not the stream's answer: encrypted whole, the relay stops
        listed = self._listed.get(workflow.info().task_queue, frozenset[str]())
        result = self._budget(self._trim(result, listed), listed)
        if not result.more_ready:
            self._answered_to = max(self._answered_to, result.next_offset)
        [p] = workflow.payload_converter().to_payloads([result])
        p.metadata[STREAM_MARK] = POLL_MARK
        p.metadata[BUYER_TOPICS_KEY] = buyer_topics(sorted(listed))
        return RawValue(p)

    def _trim(self, result: PollResult, listed: frozenset[str]) -> PollResult:
        """The answer as the codec will send it: cut, with the contrib's own truncation
        protocol, where the sealed envelope could pass ``ENVELOPE_MAX``, so the codec never
        trims it and the drain counts only what the poller is sent. Deterministic, and the
        same on every worker (the drain's wait replays on it): the bound is the most keys
        any party's item is sealed to, a constant, never this worker's own key count."""
        items = result.items
        fit = items_that_fit(
            _sizes(items), buyer_topics=listed | {PROGRESS_TOPIC}, recipients=RECIPIENTS_BOUND
        )
        if fit >= len(items):
            return result
        return PollResult(items=items[:fit], next_offset=items[fit].offset, more_ready=True)

    def _budget(self, result: PollResult, listed: frozenset[str]) -> PollResult:
        """The (cut) answer, or none yet: when it would take what this workflow task answers
        past :data:`STREAM_ANSWER_BUDGET`, the poll is answered with no items and
        ``more_ready``, ``next_offset`` at the first item it held back (the contrib's own
        truncation protocol: the poller polls again at once, and is answered in a later
        task). Only answers that carry items are counted, and the first of a task is always
        sent, so the stream makes progress; one held back is never counted as read, so the
        drain waits for the poll that carries it.

        The task is the run's current history length, as Temporal gave it to this
        activation: the same for every answer completed in one workflow task (the activations
        that resolve its local activities included), larger for the next, and the same on
        replay -- so the cut, and the drain's count of it, replay the same."""
        if not result.items:
            return result  # a frame: part of the rest of the reply (STREAM_ANSWER_BUDGET)
        task = workflow.info().get_current_history_length()
        if task != self._task:
            self._task, self._task_answered = task, 0
        cost = envelope_bound(
            _sizes(result.items),
            buyer_topics=listed | {PROGRESS_TOPIC},
            recipients=RECIPIENTS_BOUND,
        )
        if self._task_answered and self._task_answered + cost > STREAM_ANSWER_BUDGET:
            return PollResult(items=[], next_offset=result.items[0].offset, more_ready=True)
        self._task_answered += cost
        return result

    async def handle_query(self, input: HandleQueryInput) -> Any:
        result = await super().handle_query(input)
        if (
            self._root
            and input.query == STREAM_OFFSET_QUERY
            and isinstance(result, int)
            and not isinstance(result, bool)
            and result >= 0
        ):
            return RawValue(_offset_payload(result))
        return result


def _sizes(items: Iterable[Any]) -> Iterable[tuple[str, int, int]]:
    """``(topic, offset, size)`` of a poll's wire items, for the codec's arithmetic: a wire
    item's data is base64 of the serialized Payload, so 3/4 of it is at least its size."""
    return ((i.topic, i.offset, len(i.data) * 3 // 4) for i in items)


class _WorkflowOutbound(WorkflowOutboundInterceptor):
    """Stamps the job on everything the workflow schedules."""

    def __init__(self, next: WorkflowOutboundInterceptor, inbound: _WorkflowInbound) -> None:
        super().__init__(next)
        self._inbound = inbound

    def start_activity(self, input: StartActivityInput) -> workflow.ActivityHandle[Any]:
        input.headers = _with_job(input.headers, self._inbound.job_id)
        return super().start_activity(input)

    def start_local_activity(self, input: StartLocalActivityInput) -> workflow.ActivityHandle[Any]:
        input.headers = _with_job(input.headers, self._inbound.job_id)
        return super().start_local_activity(input)

    async def start_child_workflow(
        self, input: StartChildWorkflowInput
    ) -> workflow.ChildWorkflowHandle[Any, Any]:
        input.headers = _with_job(input.headers, self._inbound.job_id)
        return await super().start_child_workflow(input)

    def continue_as_new(self, input: ContinueAsNewInput) -> NoReturn:
        headers = _with_job(input.headers, self._inbound.job_id)
        input.headers = self._inbound.stamp_continue_as_new(headers)
        super().continue_as_new(input)


# --------------------------------------------------------------------------- activities


class _ActivityInbound(ActivityInboundInterceptor):
    def __init__(self, next: ActivityInboundInterceptor, progress: ProgressSink | None) -> None:
        super().__init__(next)
        self._progress = progress
        self.job_id: str | None = None

    def init(self, outbound: ActivityOutboundInterceptor) -> None:
        super().init(_ActivityOutbound(outbound, self))

    async def execute_activity(self, input: ExecuteActivityInput) -> Any:
        self.job_id = job_from_headers(input.headers)
        if self.job_id is None:
            return await super().execute_activity(input)
        ctx = _runtime.JobContext(self.job_id, activity.info().activity_type)
        try:
            with _runtime.job_context(ctx):
                return await super().execute_activity(input)
        finally:
            if self._progress is not None:
                await self._progress.settle(self.job_id)

    def offer(self, details: tuple[Any, ...]) -> None:
        job_id, sink = self.job_id, self._progress
        if job_id is None or sink is None:
            return
        try:
            kind = activity.info().activity_type
        except RuntimeError:
            kind = "activity"
        if kind == TURN_ACTIVITY:
            return  # Kontra's own liveness beat, not the seller's progress
        sink.offer(job_id, kind, details)


class _ActivityOutbound(ActivityOutboundInterceptor):
    def __init__(self, next: ActivityOutboundInterceptor, inbound: _ActivityInbound) -> None:
        super().__init__(next)
        self._inbound = inbound

    def heartbeat(self, *details: Any) -> None:
        # Offered first, so the forwarder sees every call; the SDK throttles what follows.
        self._inbound.offer(details)
        super().heartbeat(*details)


# --------------------------------------------------------------------------- the interceptor


class KontraInterceptor(Interceptor):
    """Registered first on every inner worker of :class:`kontra.serve.Worker`.

    ``streams``: the topics a buyer may read of each offering's job stream, by the
    offering's task queue (``offering.yaml`` ``streams:``). A job root on a queue not named
    here shows its buyer ``kontra.progress`` alone."""

    def __init__(
        self,
        progress: ProgressSink | None = None,
        *,
        streams: Mapping[str, Iterable[str]] | None = None,
    ) -> None:
        self.progress = progress
        listed = {queue: frozenset(topics) for queue, topics in (streams or {}).items()}
        self.streams: Mapping[str, frozenset[str]] = listed

        class _Inbound(_WorkflowInbound):
            _listed = listed

        self._inbound: type[_WorkflowInbound] = _Inbound

    def intercept_activity(self, next: ActivityInboundInterceptor) -> ActivityInboundInterceptor:
        return _ActivityInbound(next, self.progress)

    def workflow_interceptor_class(
        self, input: WorkflowInterceptorClassInput
    ) -> type[WorkflowInboundInterceptor] | None:
        return self._inbound
