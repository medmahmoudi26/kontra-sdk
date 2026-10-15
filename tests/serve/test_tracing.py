"""A Nexus call's trace context is the caller's, so ``kontra serve --otlp`` never reads it
(:class:`kontra.serve._tracing.SellerTracingInterceptor`): a buyer's ``traceparent`` is not the
parent of the seller's handler span and its sampling flag does not stop the span being
recorded, and its ``tracestate`` and ``baggage`` reach neither the exported spans nor what
the handler hands on. A control runs the same calls through Temporal's own
``TracingInterceptor``, which reads all four. Over the wire, with a buyer's real hire, it is
seam ⑤'s (``tests/codec/test_codec_telemetry.py``)."""

from __future__ import annotations

from datetime import timedelta
from typing import Any

import nexusrpc.handler
import opentelemetry.baggage
import opentelemetry.trace
from opentelemetry.exporter.otlp.proto.common.trace_encoder import encode_spans
from opentelemetry.sdk.trace import ReadableSpan
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from temporalio.contrib.opentelemetry import TracingInterceptor
from temporalio.worker import (
    ExecuteNexusOperationCancelInput,
    ExecuteNexusOperationStartInput,
    NexusOperationInboundInterceptor,
)

from kontra.serve._tracing import SellerTracingInterceptor, Tracing, tracing
from kontra.serve.hold import HOLD_HEADER

MARKER = "kontra-trace-marker-5d1e"
BUYER_TRACE = "0af7651916cd43dd8448eb211c80319c"
BUYER_SPAN = "b7ad6b7169203331"


def _headers(flags: str) -> dict[str, str]:
    """What a buyer may put in its Nexus call, beside the hold token the handler reads."""
    return {
        "traceparent": f"00-{BUYER_TRACE}-{BUYER_SPAN}-{flags}",
        "tracestate": f"buyer={MARKER}",
        "baggage": f"buyer={MARKER}",
        HOLD_HEADER: "the-hold-token",
    }


class _NeverCancelled(nexusrpc.handler.OperationTaskCancellation):
    def is_cancelled(self) -> bool:
        return False

    def cancellation_reason(self) -> str | None:
        return None

    def wait_until_cancelled_sync(self, timeout: float | None = None) -> bool:
        return False

    async def wait_until_cancelled(self) -> None:
        return None


class Handler(NexusOperationInboundInterceptor):
    """The innermost interceptor: records what the handler would see."""

    def __init__(self) -> None:
        self.headers: dict[str, str] = {}
        self.baggage: dict[str, object] = {}
        self.trace_id = 0

    def _saw(self, headers: Any) -> None:
        self.headers = dict(headers)
        self.baggage = dict(opentelemetry.baggage.get_all())
        self.trace_id = opentelemetry.trace.get_current_span().get_span_context().trace_id

    async def execute_nexus_operation_start(
        self, input: ExecuteNexusOperationStartInput
    ) -> nexusrpc.handler.StartOperationResultSync[Any]:
        self._saw(input.ctx.headers)
        return nexusrpc.handler.StartOperationResultSync("started")

    async def execute_nexus_operation_cancel(self, input: ExecuteNexusOperationCancelInput) -> None:
        self._saw(input.ctx.headers)


async def _start(interceptor: TracingInterceptor, headers: dict[str, str]) -> Handler:
    handler = Handler()
    ctx = nexusrpc.handler.StartOperationContext(
        service="acme-enrich",
        operation="execute",
        headers=headers,
        task_cancellation=_NeverCancelled(),
        request_id="request-1",
    )
    inbound = interceptor.intercept_nexus_operation(handler)
    await inbound.execute_nexus_operation_start(ExecuteNexusOperationStartInput(ctx, None))
    return handler


async def _cancel(interceptor: TracingInterceptor, headers: dict[str, str]) -> Handler:
    handler = Handler()
    ctx = nexusrpc.handler.CancelOperationContext(
        service="acme-enrich",
        operation="execute",
        headers=headers,
        task_cancellation=_NeverCancelled(),
        request_deadline=None,
    )
    inbound = interceptor.intercept_nexus_operation(handler)
    await inbound.execute_nexus_operation_cancel(ExecuteNexusOperationCancelInput(ctx, "token"))
    return handler


def _exported(traced: Tracing, exporter: InMemorySpanExporter) -> list[ReadableSpan]:
    assert traced.provider.force_flush(timedelta(seconds=10) // timedelta(milliseconds=1))
    return list(exporter.get_finished_spans())


def test_kontra_serve_traces_with_it() -> None:
    assert isinstance(tracing(InMemorySpanExporter()).interceptor, SellerTracingInterceptor)


async def test_a_handler_span_starts_a_trace_of_its_own() -> None:
    exporter = InMemorySpanExporter()
    traced = tracing(exporter)
    # Not sampled by the buyer: the seller's provider decides, and records the span.
    start = await _start(traced.interceptor, _headers("00"))
    cancel = await _cancel(traced.interceptor, _headers("01"))
    spans = _exported(traced, exporter)
    assert [s.name for s in spans] == [
        "RunStartNexusOperationHandler:acme-enrich/execute",
        "RunCancelNexusOperationHandler:acme-enrich/execute",
    ]
    for span in spans:
        assert span.parent is None  # a root: no buyer span above it, and no link to one
        assert not span.links
        assert span.context is not None
        assert format(span.context.trace_id, "032x") != BUYER_TRACE
        assert not span.context.trace_state
    assert MARKER.encode() not in encode_spans(spans).SerializeToString()
    for seen, span in zip((start, cancel), spans, strict=True):
        # The handler runs under the seller's span, with no baggage of the buyer's; the
        # hold token is still there, and the trace context it would pass on is the seller's.
        assert span.context is not None
        assert seen.trace_id == span.context.trace_id
        assert seen.baggage == {}
        assert seen.headers[HOLD_HEADER] == "the-hold-token"
        assert BUYER_TRACE not in seen.headers.get("traceparent", "")
        assert "tracestate" not in seen.headers
        assert "baggage" not in seen.headers


async def test_any_ascii_case_of_the_names_is_removed() -> None:
    exporter = InMemorySpanExporter()
    traced = tracing(exporter)
    shouted = {k.upper() if k != HOLD_HEADER else k: v for k, v in _headers("01").items()}
    seen = await _start(traced.interceptor, shouted)
    assert {k.lower() for k in seen.headers} == {HOLD_HEADER, "traceparent"}
    assert BUYER_TRACE not in seen.headers["traceparent"]
    [span] = _exported(traced, exporter)
    assert span.parent is None


async def test_control_temporal_s_interceptor_reads_the_buyer_s_context() -> None:
    """Without the removal, each of the four is the buyer's: what the test above rules out
    is what Temporal's interceptor does with a Nexus call's headers."""
    exporter = InMemorySpanExporter()
    traced = tracing(exporter)
    stock = TracingInterceptor(traced.interceptor.tracer, always_create_workflow_spans=True)
    unsampled = await _start(stock, _headers("00"))
    assert _exported(traced, exporter) == []  # the buyer's flag: nothing recorded
    assert unsampled.baggage == {"buyer": MARKER}
    exporter.clear()
    sampled = await _start(stock, _headers("01"))
    [span] = _exported(traced, exporter)
    assert span.parent is not None
    assert format(span.parent.span_id, "016x") == BUYER_SPAN
    assert format(span.parent.trace_id, "032x") == BUYER_TRACE
    assert MARKER.encode() in encode_spans([span]).SerializeToString()  # its tracestate
    assert MARKER in sampled.headers["baggage"]  # handed on to what the handler starts
