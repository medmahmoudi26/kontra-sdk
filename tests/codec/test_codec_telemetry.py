"""The marker-string scan, extended to a worker's telemetry (seam ⑤): what ``kontra serve
--metrics`` serves and what ``--otlp`` exports carry no payload, input, result, failure
message or heartbeat detail.

A seller's ``kontra.serve`` Worker runs on a real dev server with the fake gate, connected
with the runtime and the tracing interceptor those two flags build
(:mod:`kontra.serve.telemetry`), spans going to an in-memory exporter instead of a
collector. A buyer hires two jobs whose input carries a marker: one delivers it upper-cased
after an activity attempt that heartbeats it and fails with it in the message, the other
fails its workflow with it in the message. A third is hired by a buyer whose Nexus call
carries a trace context of its choosing: unsampled, with the marker in its ``tracestate``
and ``baggage``. Then the Prometheus text and the spans, encoded as OTLP would send them, are
searched for the marker in every case, and so is the third job's history, where the trace
context the handler passes on is stored unencoded (``_tracer-data``). A control proves the
search would find it: the same spans before :class:`~kontra.serve._tracing.Scrubbed` carry
it (the interceptor writes exception messages), so the scrub is what keeps them out; the
buyer's trace context is ruled out before any span starts, and its control is
``tests/serve/test_tracing.py``.
"""

from __future__ import annotations

import asyncio
import json
import os
import socket
import uuid
from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

import httpx
import pytest
import pytest_asyncio
from opentelemetry.exporter.otlp.proto.common.trace_encoder import encode_spans
from opentelemetry.sdk.trace import ReadableSpan
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import StatusCode
from temporalio.client import (
    Client,
    Interceptor,
    NexusOperationHandle,
    OutboundInterceptor,
    StartNexusOperationInput,
)

from kontra.contract.names import job_workflow_id
from kontra.hire import JobFailed
from kontra.serve import Quote, Worker, offering
from kontra.serve._tracing import TASK_QUEUE_ATTRIBUTE, Tracing, tracing
from kontra.serve.telemetry import metrics_runtime
from kontra.testing import Marketplace, TestParty
from kontra.testing.devserver import (
    CLI_ENV,
    DevServer,
    acquire_shared_dev_server,
    release_shared_dev_server,
)

from .serve_flows import Doc, Shout, shout

pytestmark = [pytest.mark.temporal, pytest.mark.asyncio(loop_scope="module")]

MARKER = "kontra-telemetry-marker-3c8a"
BUYER_TRACE = "0af7651916cd43dd8448eb211c80319c"
#: What the third job's buyer puts in its Nexus call: a trace context of its own choosing.
BUYER_TRACE_HEADERS = {
    "traceparent": f"00-{BUYER_TRACE}-b7ad6b7169203331-00",  # not sampled
    "tracestate": f"buyer={MARKER}",
    "baggage": f"buyer={MARKER}",
}


@pytest.fixture(scope="module")
def dev_server() -> Iterator[DevServer]:
    try:
        server = acquire_shared_dev_server()
    except Exception as e:
        if os.environ.get(CLI_ENV):
            raise  # an explicit binary must work
        pytest.skip(f"no Temporal CLI 1.9.1 dev server: {e}")
    try:
        yield server
    finally:
        release_shared_dev_server()


async def quote(doc: Doc) -> Quote:
    return Quote(price=Decimal(len(doc.text)) / 100)


def _offering(directory: Path) -> Path:
    spec = {
        "name": "acme/shout",
        "version": f"1.{os.getpid() % 100_000}.{uuid.uuid4().int % 10_000}",
        "description": "Shouts a document",
        "max_duration": "10m",
        "review_window": "1h",
        "max_output_bytes": "1MB",
        "artifact_retention": "1d",
    }
    path = directory / "offering.yaml"
    path.write_text("".join(f"{k}: {json.dumps(v)}\n" for k, v in spec.items()))
    return path


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


class BuyerTraceContext(Interceptor):
    """A buyer's client that adds :data:`BUYER_TRACE_HEADERS` to every Nexus call it starts."""

    def __init__(self) -> None:
        self.calls = 0

    def intercept_client(self, next: OutboundInterceptor) -> OutboundInterceptor:
        root = self

        class Outbound(OutboundInterceptor):
            async def start_nexus_operation(
                self, input: StartNexusOperationInput
            ) -> NexusOperationHandle[object]:
                input.headers = {**input.headers, **BUYER_TRACE_HEADERS}
                root.calls += 1
                return await super().start_nexus_operation(input)

        return Outbound(next)


@dataclass
class Telemetry:
    metrics_url: str
    tracing: Tracing
    exported: InMemorySpanExporter
    #: The same spans as they leave the interceptor, before the scrub: the control.
    raw: InMemorySpanExporter
    task_queue: str
    #: What each job was: "delivered" / "failed" / "traced" (hired with a buyer's trace
    #: context) -> job id.
    jobs: dict[str, str]
    seller: Client

    def spans(self) -> tuple[list[ReadableSpan], list[ReadableSpan]]:
        assert self.tracing.provider.force_flush(10_000)
        return list(self.exported.get_finished_spans()), list(self.raw.get_finished_spans())


async def _serve(worker: Worker, party: TestParty, **connect: object) -> asyncio.Task[None]:
    """``worker`` run as ``party`` with ``connect`` (``runtime``, ``interceptors``) as
    ``kontra serve --metrics/--otlp`` passes them, once it polls."""
    run = asyncio.create_task(
        worker.run_async(session=party.session, handle_signals=False, **connect)  # type: ignore[arg-type]
    )
    ready = asyncio.create_task(worker.wait_ready())
    await asyncio.wait([run, ready], return_when=asyncio.FIRST_COMPLETED)
    if run.done() or not worker.serving:
        ready.cancel()
        await run
        raise RuntimeError("the worker stopped before it was ready")
    return run


@pytest_asyncio.fixture(loop_scope="module", scope="module")
async def telemetry(
    dev_server: DevServer, tmp_path_factory: pytest.TempPathFactory
) -> AsyncIterator[Telemetry]:
    port = _free_port()
    runtime = metrics_runtime(f"127.0.0.1:{port}")
    exported, raw = InMemorySpanExporter(), InMemorySpanExporter()
    traced = tracing(exported)
    traced.provider.add_span_processor(SimpleSpanProcessor(raw))
    async with Marketplace(dev_server=dev_server) as mp:
        acme = await mp.party("acme")
        bob = await mp.party(f"bob-{uuid.uuid4().hex[:6]}", balance="1000.00")
        bound = offering(_offering(tmp_path_factory.mktemp("shout")), workflow=Shout, quote=quote)
        worker = Worker(offerings=[bound], activities=[shout])
        run = await _serve(worker, acme, runtime=runtime, interceptors=[traced.interceptor])
        jobs: dict[str, str] = {}
        try:
            buyer = bob.kontra(client=mp.client(bob.namespace))
            delivered = await buyer.hire(bound.key, text=f"hello {MARKER}")
            result = await delivered.result(timeout=60)
            assert result.text == f"HELLO {MARKER}".upper()
            jobs["delivered"] = delivered.job_id
            failing = await buyer.hire(bound.key, text=f"fail {MARKER}")
            with pytest.raises(JobFailed):
                await failing.result(timeout=60)
            jobs["failed"] = failing.job_id
            context = BuyerTraceContext()
            config = mp.client(bob.namespace).config()
            config["interceptors"] = [context]
            tracing_buyer = bob.kontra(client=Client(**config))
            traced_job = await tracing_buyer.hire(bound.key, text=f"traced {MARKER}")
            assert (await traced_job.result(timeout=60)).text == f"TRACED {MARKER}".upper()
            assert context.calls == 1  # the call carried the buyer's trace context
            jobs["traced"] = traced_job.job_id
            yield Telemetry(
                f"http://127.0.0.1:{port}/metrics",
                traced,
                exported,
                raw,
                bound.offering_id.task_queue,
                jobs,
                mp.client(acme.namespace),
            )
        finally:
            worker.shutdown()
            await run
            traced.shutdown()


def _absent(where: str, data: bytes) -> None:
    for needle in (MARKER, MARKER.upper()):
        assert needle.encode() not in data, f"{needle} in {where}"


async def test_the_metrics_endpoint_carries_no_job_data(telemetry: Telemetry) -> None:
    async with httpx.AsyncClient() as http:
        text = (await http.get(telemetry.metrics_url, timeout=10)).text
    # The SDK's own metrics under Temporal's names, labelled with the worker's task queue:
    # the endpoint is the real one, after both jobs ran through it.
    for name in (
        "temporal_worker_task_slots_available",
        "temporal_activity_execution_latency",
        "temporal_activity_schedule_to_start_latency",
        "temporal_activity_execution_failed",
        "temporal_workflow_completed",
        "temporal_workflow_failed",
    ):
        assert name in text, name
    assert f'task_queue="{telemetry.task_queue}"' in text
    assert "kontra" not in text.replace(telemetry.task_queue, "")  # no Kontra metric names
    _absent("the metrics endpoint", text.encode())


async def test_exported_spans_carry_no_job_data(telemetry: Telemetry) -> None:
    exported, raw = telemetry.spans()
    _absent("the exported spans", encode_spans(exported).SerializeToString())
    # Control: the interceptor wrote the failure messages into the spans it made; the scrub
    # took them out. Without it, the search above would have found the marker.
    assert MARKER.encode() in encode_spans(raw).SerializeToString()
    assert [s.name for s in exported] == [s.name for s in raw]


async def test_spans_carry_the_task_queue_and_the_workflow_id(telemetry: Telemetry) -> None:
    exported, _ = telemetry.spans()
    delivered = job_workflow_id(telemetry.jobs["delivered"])
    attempts = [
        s
        for s in exported
        if s.name == "RunActivity:shout"
        and (s.attributes or {}).get("temporalWorkflowID") == delivered
    ]
    assert len(attempts) == 2  # the failed first attempt, then the one that delivered
    for span in attempts:
        assert (span.attributes or {})[TASK_QUEUE_ATTRIBUTE] == telemetry.task_queue
    failed_attempt, delivering = attempts
    # A failure keeps its status code and its exception's type, not its message.
    assert failed_attempt.status.status_code is StatusCode.ERROR
    assert failed_attempt.status.description is None
    [event] = [e for e in failed_attempt.events if e.name == "exception"]
    assert dict(event.attributes or {}).keys() <= {"exception.type", "exception.escaped"}
    assert (event.attributes or {})["exception.type"].endswith("ApplicationError")
    assert delivering.status.status_code is not StatusCode.ERROR
    workflow_ids = {
        (s.attributes or {}).get("temporalWorkflowID")
        for s in exported
        if s.name.startswith("RunWorkflow:")
    }
    assert {delivered, job_workflow_id(telemetry.jobs["failed"])} <= workflow_ids
    assert all(
        (s.attributes or {}).get(TASK_QUEUE_ATTRIBUTE) == telemetry.task_queue
        for s in exported
        if s.name.startswith(("RunWorkflow:", "RunActivity:", "StartActivity:"))
    )


async def test_a_buyer_s_trace_context_stays_out(telemetry: Telemetry) -> None:
    """The third job's buyer sent an unsampled ``traceparent`` and the marker in
    ``tracestate`` and ``baggage``: the seller's spans are recorded anyway, under a trace of
    the seller's, and the history of the workflow the handler started holds no marker."""
    exported, _ = telemetry.spans()
    workflow_id = job_workflow_id(telemetry.jobs["traced"])
    [run] = [
        s
        for s in exported
        if s.name.startswith("RunWorkflow:")
        and (s.attributes or {}).get("temporalWorkflowID") == workflow_id
    ]
    assert run.context is not None
    trace = [
        s for s in exported if s.context is not None and s.context.trace_id == run.context.trace_id
    ]
    [handler] = [s for s in trace if s.name.startswith("RunStartNexusOperationHandler:")]
    assert handler.parent is None  # a root: the buyer's span is not above it
    assert format(run.context.trace_id, "032x") != BUYER_TRACE
    assert all(not s.context.trace_state for s in trace if s.context is not None)
    _absent("the traced job's spans", encode_spans(trace).SerializeToString())
    history = await telemetry.seller.get_workflow_handle(workflow_id).fetch_history()
    started = history.events[0].workflow_execution_started_event_attributes
    # The trace context the handler handed on is the seller's, and there is one to check.
    carried = started.header.fields["_tracer-data"].data
    assert format(run.context.trace_id, "032x").encode() in carried
    _absent("the traced job's history", history.to_json().encode())
