"""The spans of ``kontra serve --otlp``: Temporal's ``TracingInterceptor`` on a tracer provider
of its own (the ``opentelemetry`` extra; :mod:`kontra.serve.telemetry` imports this module
only when ``--otlp`` is given).

The provider is the interceptor's alone, never OpenTelemetry's global one: the spans a seller
makes with OpenTelemetry's API are theirs to export where they choose, and only what this
provider holds is held to the rule below.

Each span carries ``temporalTaskQueue``, the task queue of the worker it was started in,
beside the interceptor's own ``temporalWorkflowID``, ``temporalRunID`` and
``temporalActivityID`` (the name follows theirs; Temporal has none for it). It is read from
the Temporal context the span starts in: an activity's, a workflow's, a Nexus operation's.

Nothing a buyer wrote leaves in a span (seam ⑤). The interceptor's attributes are names
and IDs, none of them a buyer's, but it does write one thing from the job: an exception's
message, as the description of a failed span's status and in the ``exception`` event with
its stack trace. A failure message travels encrypted everywhere else (the codec encodes
failures), so every span is exported through :class:`Scrubbed`: an error keeps its status
code and its exception's type, and loses the description, the message and the stack trace.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

import opentelemetry.context
import temporalio.activity
import temporalio.nexus
import temporalio.workflow
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import Event, ReadableSpan, Span, SpanProcessor, TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, SpanExporter, SpanExportResult
from opentelemetry.trace import Status
from temporalio.contrib.opentelemetry import TracingInterceptor

__all__ = ["TASK_QUEUE_ATTRIBUTE", "Scrubbed", "Tracing", "scrub", "tracing"]

#: The span attribute naming the task queue (spelled like ``temporalWorkflowID``).
TASK_QUEUE_ATTRIBUTE = "temporalTaskQueue"
#: What an ``exception`` event keeps: the type, and whether it escaped the span.
_EXCEPTION_KEPT = frozenset({"exception.type", "exception.escaped"})


@dataclass(frozen=True)
class Tracing:
    """The interceptor to connect the worker's client with, and the provider behind it."""

    interceptor: TracingInterceptor
    provider: TracerProvider

    def shutdown(self) -> None:
        """Export what is still batched and stop: ``kontra`` leaves through ``os._exit``,
        so the provider's own exit hook never runs."""
        self.provider.shutdown()


def tracing(exporter: SpanExporter) -> Tracing:
    """``TracingInterceptor`` on a provider that stamps the task queue and exports through
    :class:`Scrubbed` (batched) to ``exporter``. The resource is OpenTelemetry's default, so
    ``OTEL_SERVICE_NAME`` and ``OTEL_RESOURCE_ATTRIBUTES`` apply."""
    provider = TracerProvider(resource=Resource.create())
    provider.add_span_processor(_TaskQueue())
    provider.add_span_processor(BatchSpanProcessor(Scrubbed(exporter)))
    # The interceptor's own instrumentation scope, as when it builds its tracer itself.
    tracer = provider.get_tracer(TracingInterceptor.__module__)
    # Workflow spans are created even without a client span above them: a job's workflow
    # is started by the Nexus handler, from a buyer's call, not by a traced client of ours.
    interceptor = TracingInterceptor(tracer, always_create_workflow_spans=True)
    return Tracing(interceptor, provider)


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
