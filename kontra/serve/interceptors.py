"""Worker interceptors: which job a piece of seller code is working for, and its heartbeats.

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
  progress forwarder, then passed on unchanged. When the activity returns, the forwarder
  gets a short, bounded chance to deliver the heartbeats that are due
  (:meth:`~kontra.serve.progress.ProgressForwarder.settle`), so the buyer sees them before
  the job closes; a slow or unreachable gate does not hold activities up.

This module runs inside workflow code: it imports only temporalio, the standard library,
:mod:`kontra.contract.names` and :mod:`kontra._runtime` (a context variable; no I/O,
deterministic), and the workflow interceptor class is handed to the sandbox as an object.

Known gap: a *descendant* workflow learns its job when its run method starts; an activity
scheduled by a signal handler that runs before that (a signal in the very first activation)
carries no job, so its heartbeats are not forwarded. The root is never affected.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Mapping
from typing import Any, NoReturn, Protocol

from temporalio import activity, workflow
from temporalio.api.common.v1 import Payload
from temporalio.worker import (
    ActivityInboundInterceptor,
    ActivityOutboundInterceptor,
    ContinueAsNewInput,
    ExecuteActivityInput,
    ExecuteWorkflowInput,
    Interceptor,
    StartActivityInput,
    StartChildWorkflowInput,
    StartLocalActivityInput,
    WorkflowInboundInterceptor,
    WorkflowInterceptorClassInput,
    WorkflowOutboundInterceptor,
)

from kontra import _runtime
from kontra.contract.names import is_job_id, job_id_from_workflow_id

__all__ = ["JOB_HEADER", "KontraInterceptor", "ProgressSink", "job_from_headers", "job_header"]

log = logging.getLogger("kontra.serve.interceptors")

#: The Temporal header that carries the job id below the root handler workflow.
JOB_HEADER = "kontra-job"
_JSON_PLAIN = b"json/plain"


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


# --------------------------------------------------------------------------- workflows


class _WorkflowInbound(WorkflowInboundInterceptor):
    """Learns the workflow's job: its own id (the root ``job-<id>``) or the parent's header."""

    _header_job: str | None = None

    def init(self, outbound: WorkflowOutboundInterceptor) -> None:
        super().init(_WorkflowOutbound(outbound, self))

    @property
    def job_id(self) -> str | None:
        # The root is known from its id at any time (a signal handler may schedule work
        # before the run method starts); a descendant only once its input headers are read.
        return job_id_from_workflow_id(workflow.info().workflow_id) or self._header_job

    async def execute_workflow(self, input: ExecuteWorkflowInput) -> Any:
        self._header_job = job_from_headers(input.headers)
        return await super().execute_workflow(input)


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
        input.headers = _with_job(input.headers, self._inbound.job_id)
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
    """Registered first on every inner worker of :class:`kontra.serve.Worker`."""

    def __init__(self, progress: ProgressSink | None = None) -> None:
        self.progress = progress

    def intercept_activity(self, next: ActivityInboundInterceptor) -> ActivityInboundInterceptor:
        return _ActivityInbound(next, self.progress)

    def workflow_interceptor_class(
        self, input: WorkflowInterceptorClassInput
    ) -> type[WorkflowInboundInterceptor] | None:
        return _WorkflowInbound
