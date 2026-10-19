"""The spans of ``kontra serve --otlp``: Temporal's ``TracingInterceptor`` on a tracer provider
of its own (the ``opentelemetry`` extra; :mod:`kontra.serve.telemetry` imports this module
only when ``--otlp`` is given).

The provider is the interceptor's alone, never OpenTelemetry's global one: the spans a seller
makes with OpenTelemetry's API are theirs to export where they choose, and only what this
provider holds is held to the rule below.

The provider's resource is OpenTelemetry's default (``OTEL_SERVICE_NAME`` and
``OTEL_RESOURCE_ATTRIBUTES`` apply), except that a service they leave unnamed -- which
OpenTelemetry would call ``unknown_service`` -- is :data:`SERVICE_NAME`; the worker's logs
(:mod:`kontra.serve._logs`) have the same resource.

Each span carries ``temporalTaskQueue``, the task queue of the worker it was started in,
beside the interceptor's own ``temporalWorkflowID``, ``temporalRunID`` and
``temporalActivityID`` (the name follows theirs; Temporal has none for it). It is read from
the Temporal context the span starts in: an activity's, a workflow's, a Nexus operation's.

Nothing a buyer wrote leaves in a span (seam ⑤). The interceptor's attributes are names
and IDs, none of them a buyer's, but two things from a job would reach a span through it:

* The trace context of a Nexus call. A Nexus operation's headers are the caller's, and
  the interceptor makes their W3C ``traceparent`` the parent of the handler's span, keeps
  their ``tracestate`` in its span context (exported with the span), honours their
  sampling flag (``00`` and nothing under the handler is recorded), and carries their
  ``baggage`` on into the ``_tracer-data`` header of the workflow the handler starts,
  which Temporal stores unencoded in history. So :class:`SellerTracingInterceptor` removes
  every header the interceptor's propagator reads (``traceparent``, ``tracestate``,
  ``baggage``) from a Nexus call before the interceptor sees it: a handler's span starts a
  trace of its own, sampled by the seller's provider, with no link to the buyer's (a link
  would export the IDs the buyer chose).
* An exception's message, as the description of a failed span's status and in the
  ``exception`` event with its stack trace. A failure message travels encrypted everywhere
  else (the codec encodes failures), so every span is exported through :class:`Scrubbed`:
  an error keeps its status code and its exception's type, and loses the description, the
  message and the stack trace.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Mapping, Sequence
from typing import Any

import nexusrpc.handler
import opentelemetry.context
import temporalio.activity
import temporalio.nexus
import temporalio.worker
import temporalio.workflow
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import Event, ReadableSpan, Span, SpanProcessor, TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, SpanExporter, SpanExportResult
from opentelemetry.trace import Status
from temporalio.contrib.opentelemetry import TracingInterceptor

__all__ = [
    "SERVICE_NAME",
    "TASK_QUEUE_ATTRIBUTE",
    "Scrubbed",
    "SellerTracingInterceptor",
    "Tracing",
    "resource",
    "scrub",
    "tracing",
]

#: ``service.name`` when the environment names none: what the graph page of
#: ``kontra dev up --ui`` selects a worker's logs by in Loki.
SERVICE_NAME = "kontra-worker"

#: The span attribute naming the task queue (spelled like ``temporalWorkflowID``).
TASK_QUEUE_ATTRIBUTE = "temporalTaskQueue"
#: What an ``exception`` event keeps: the type, and whether it escaped the span.
_EXCEPTION_KEPT = frozenset({"exception.type", "exception.escaped"})


def resource() -> Resource:
    """OpenTelemetry's default resource, with ``service.name`` :data:`SERVICE_NAME` unless
    ``OTEL_SERVICE_NAME`` or ``OTEL_RESOURCE_ATTRIBUTES`` names the service."""
    detected = Resource.create()
    name = str(detected.attributes.get("service.name", ""))
    if name == "unknown_service" or name.startswith("unknown_service:"):
        return detected.merge(Resource({"service.name": SERVICE_NAME}))
    return detected


@dataclasses.dataclass(frozen=True)
class Tracing:
    """The interceptor to connect the worker's client with, and the provider behind it."""

    interceptor: TracingInterceptor
    provider: TracerProvider

    def shutdown(self) -> None:
        """Export what is still batched and stop: ``kontra`` leaves through ``os._exit``,
        so the provider's own exit hook never runs."""
        self.provider.shutdown()


def tracing(exporter: SpanExporter) -> Tracing:
    """:class:`SellerTracingInterceptor` on a provider that stamps the task queue and
    exports through :class:`Scrubbed` (batched) to ``exporter``, with :func:`resource`."""
    provider = TracerProvider(resource=resource())
    provider.add_span_processor(_TaskQueue())
    provider.add_span_processor(BatchSpanProcessor(Scrubbed(exporter)))
    # The interceptor's own instrumentation scope, as when it builds its tracer itself.
    tracer = provider.get_tracer(TracingInterceptor.__module__)
    # Workflow spans are created even without a client span above them: a job's workflow
    # is started by the Nexus handler, from a buyer's call, not by a traced client of ours.
    interceptor = SellerTracingInterceptor(tracer, always_create_workflow_spans=True)
    return Tracing(interceptor, provider)


class SellerTracingInterceptor(TracingInterceptor):
    """``TracingInterceptor`` that never reads a trace context from a Nexus call: the
    headers its propagator reads are removed from the call first (see the module
    docstring). Everything else is the interceptor's own."""

    def intercept_nexus_operation(
        self, next: temporalio.worker.NexusOperationInboundInterceptor
    ) -> temporalio.worker.NexusOperationInboundInterceptor:
        fields = frozenset(f.lower() for f in self.text_map_propagator.fields)
        return _CallerContextRemoved(super().intercept_nexus_operation(next), fields)


def _without(headers: Mapping[str, str], fields: frozenset[str]) -> dict[str, str]:
    """``headers`` less ``fields``, in any ASCII case (Nexus header names arrive
    lowercased; a name with a non-ASCII letter is never one of them)."""
    return {k: v for k, v in headers.items() if not (k.isascii() and k.lower() in fields)}


class _CallerContextRemoved(temporalio.worker.NexusOperationInboundInterceptor):
    """Hands ``next`` (the tracing interceptor's) each Nexus call without ``fields``."""

    def __init__(
        self, next: temporalio.worker.NexusOperationInboundInterceptor, fields: frozenset[str]
    ) -> None:
        super().__init__(next)
        self._fields = fields

    async def execute_nexus_operation_start(
        self, input: temporalio.worker.ExecuteNexusOperationStartInput
    ) -> (
        nexusrpc.handler.StartOperationResultSync[Any] | nexusrpc.handler.StartOperationResultAsync
    ):
        input.ctx = dataclasses.replace(
            input.ctx, headers=_without(input.ctx.headers, self._fields)
        )
        return await self.next.execute_nexus_operation_start(input)

    async def execute_nexus_operation_cancel(
        self, input: temporalio.worker.ExecuteNexusOperationCancelInput
    ) -> None:
        input.ctx = dataclasses.replace(
            input.ctx, headers=_without(input.ctx.headers, self._fields)
        )
        await self.next.execute_nexus_operation_cancel(input)


def _task_queue() -> str | None:
    """The task queue of the Temporal context running now, if any."""
    if temporalio.activity.in_activity():
        return temporalio.activity.info().task_queue
    if temporalio.workflow.in_workflow():
        return temporalio.workflow.info().task_queue
    if temporalio.nexus.in_operation():
        return temporalio.nexus.info().task_queue
    return None


class _TaskQueue(SpanProcessor):
    """Sets :data:`TASK_QUEUE_ATTRIBUTE` on each span as it starts."""

    def on_start(
        self, span: Span, parent_context: opentelemetry.context.Context | None = None
    ) -> None:
        queue = _task_queue()
        if queue is not None:
            span.set_attribute(TASK_QUEUE_ATTRIBUTE, queue)


def _scrub_event(event: Event) -> Event:
    if event.name != "exception":
        return event
    kept = {k: v for k, v in (event.attributes or {}).items() if k in _EXCEPTION_KEPT}
    return Event(event.name, kept, event.timestamp)


def scrub(span: ReadableSpan) -> ReadableSpan:
    """``span`` without exception messages: see the module docstring."""
    status = span.status
    if status.description is not None:
        status = Status(status.status_code)
    return ReadableSpan(
        name=span.name,
        context=span.context,
        parent=span.parent,
        resource=span.resource,
        attributes=span.attributes,
        events=[_scrub_event(e) for e in span.events],
        links=span.links,
        kind=span.kind,
        status=status,
        start_time=span.start_time,
        end_time=span.end_time,
        instrumentation_scope=span.instrumentation_scope,
    )


class Scrubbed(SpanExporter):
    """An exporter that hands ``inner`` each span :func:`scrub`-bed."""

    def __init__(self, inner: SpanExporter) -> None:
        self.inner = inner

    def export(self, spans: Sequence[ReadableSpan]) -> SpanExportResult:
        return self.inner.export([scrub(s) for s in spans])

    def shutdown(self) -> None:
        self.inner.shutdown()

    def force_flush(self, timeout_millis: int = 30000) -> bool:
        return self.inner.force_flush(timeout_millis)
