"""``kontra serve --otlp``'s logs (:mod:`kontra.serve._logs`): the worker's Python logging,
exported beside its spans, each record carrying the Temporal context it was emitted in and
its span's ids.

A workflow logs through ``workflow.logger`` and its activity through ``activity.logger``,
through a module logger of its own, and with an exception, on a real dev server under
Kontra's sandbox and the tracing interceptor ``--otlp`` builds; the records go to an
in-memory exporter instead of a collector. Each is found by the attributes the graph
page's Logs link selects in Loki (``workflow_id``, ``run_id``, ``activity_type``, ...), with
the trace id of the span it was emitted in. The spans themselves are seam ⑤'s
(``tests/codec/test_codec_telemetry.py``); logs are the seller's own output and are not
scrubbed (``docs/sellers.md``, "Scaling and observability")."""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass

import pytest
from opentelemetry._logs import SeverityNumber
from opentelemetry.sdk._logs import ReadableLogRecord
from opentelemetry.sdk._logs.export import InMemoryLogRecordExporter
from opentelemetry.sdk.trace import ReadableSpan
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from temporalio.worker import Worker

from kontra.serve._logs import TEMPORAL_ATTRIBUTES, OtlpHandler, logs, temporal_attributes
from kontra.serve._tracing import SERVICE_NAME, resource, tracing
from kontra.serve.worker import workflow_runner
from kontra.testing.devserver import DevServer

from . import logflows
from .logflows import Chatty, chatty

pytestmark = pytest.mark.temporal

MARKER = "kontra-logs-marker-7e21"


@dataclass
class Run:
    workflow_id: str
    run_id: str
    task_queue: str
    records: list[ReadableLogRecord]
    spans: list[ReadableSpan]
    #: The records as Python's logging handed them to every handler (caplog's).
    seen: list[logging.LogRecord]

    def record(self, starts: str) -> ReadableLogRecord:
        """The one record whose message starts with ``starts``."""
        [found] = [r for r in self.records if str(r.log_record.body).startswith(starts)]
        return found

    def span(self, name: str) -> ReadableSpan:
        [found] = [s for s in self.spans if s.name == name]
        return found


@pytest.fixture
def clean_service_name(monkeypatch: pytest.MonkeyPatch) -> None:
    """No service named by the environment: the resource's default applies."""
    monkeypatch.delenv("OTEL_SERVICE_NAME", raising=False)
    monkeypatch.delenv("OTEL_RESOURCE_ATTRIBUTES", raising=False)


@pytest.fixture
async def run(
    dev_server: DevServer, caplog: pytest.LogCaptureFixture, clean_service_name: None
) -> Run:
    caplog.set_level(logging.INFO)  # what `kontra serve` sets when nothing configured logging
    spans, records = InMemorySpanExporter(), InMemoryLogRecordExporter()
    traced = tracing(spans)
    logged = logs(records)
    task_queue = f"logs-{uuid.uuid4().hex[:8]}"
    workflow_id = f"chatty-{uuid.uuid4().hex[:8]}"
    try:
        client = await dev_server.connect(interceptors=[traced.interceptor])
        async with Worker(
            client,
            task_queue=task_queue,
            workflows=[Chatty],
            activities=[chatty],
            workflow_runner=workflow_runner(),
        ):
            handle = await client.start_workflow(
                Chatty.run, MARKER, id=workflow_id, task_queue=task_queue
            )
            assert await handle.result() == MARKER
    finally:
        logged.shutdown()
        traced.shutdown()
    assert handle.result_run_id is not None
    ours = [r for r in records.get_finished_logs() if MARKER in str(r.log_record.body)]
    ours += [
        r
        for r in records.get_finished_logs()
        if MARKER in str((r.log_record.attributes or {}).get("exception.message", ""))
    ]
    return Run(
        workflow_id,
        handle.result_run_id,
        task_queue,
        ours,
        list(spans.get_finished_spans()),
        list(caplog.records),
    )


async def test_an_activity_s_records_carry_its_context_and_its_span(run: Run) -> None:
    """``activity.logger``, a module logger and an exception inside the same activity: each
    record names the activity and its run, and the span it was emitted in."""
    attempt = run.span("RunActivity:chatty")
    assert attempt.context is not None
    expected = {
        "workflow_id": run.workflow_id,
        "run_id": run.run_id,
        "workflow_type": "Chatty",
        "activity_id": "1",
        "activity_type": "chatty",
        "attempt": 1,
        "task_queue": run.task_queue,
    }
    adapter = run.record("through activity.logger:")
    plain = run.record("through a module logger:")
    caught = run.record("an exception, logged")
    for record in (adapter, plain, caught):
        attributes = dict(record.log_record.attributes or {})
        assert {k: attributes.get(k) for k in TEMPORAL_ATTRIBUTES} == expected
        assert record.log_record.trace_id == attempt.context.trace_id
        assert record.log_record.span_id == attempt.context.span_id
        assert record.resource.attributes["service.name"] == SERVICE_NAME
    assert adapter.instrumentation_scope is not None
    assert adapter.instrumentation_scope.name == "temporalio.activity"
    assert plain.instrumentation_scope is not None
    assert plain.instrumentation_scope.name == logflows.log.name
    assert (plain.log_record.severity_number, plain.log_record.severity_text) == (
        SeverityNumber.INFO,
        "INFO",
    )
    # The seller's exception, as written: logs are not scrubbed.
    attributes = dict(caught.log_record.attributes or {})
    assert caught.log_record.severity_number is SeverityNumber.ERROR
    assert attributes["exception.type"] == "ValueError"
    assert attributes["exception.message"] == f"caught: {MARKER}"
    assert f"ValueError: caught: {MARKER}" in str(attributes["exception.stacktrace"])
    assert attributes["code.function.name"] == "chatty"


async def test_a_workflow_s_records_carry_its_context_and_its_trace(run: Run) -> None:
    started = run.span("StartWorkflow:Chatty")
    assert started.context is not None
    record = run.record("through workflow.logger:")
    attributes = dict(record.log_record.attributes or {})
    assert {k: attributes.get(k) for k in TEMPORAL_ATTRIBUTES} == {
        "workflow_id": run.workflow_id,
        "run_id": run.run_id,
        "workflow_type": "Chatty",
        "activity_id": None,
        "activity_type": None,
        "attempt": 1,
        "task_queue": run.task_queue,
    }
    # The context the workflow runs in: the trace of the client call that started it.
    assert record.log_record.trace_id == started.context.trace_id


async def test_the_records_other_handlers_see_are_left_as_they_are(run: Run) -> None:
    """temporalio's extras stay on the record (another handler may read them); the flat
    attributes are the exporter's alone."""
    [adapter] = [r for r in run.seen if r.getMessage().startswith("through activity.logger:")]
    assert isinstance(adapter.__dict__["temporal_activity"], dict)
    assert "activity_type" not in adapter.__dict__
    assert temporal_attributes(adapter)["activity_type"] == "chatty"


def test_the_service_is_named_by_the_environment_else_kontra_worker(
    monkeypatch: pytest.MonkeyPatch, clean_service_name: None
) -> None:
    assert resource().attributes["service.name"] == SERVICE_NAME == "kontra-worker"
    monkeypatch.setenv("OTEL_RESOURCE_ATTRIBUTES", "service.name=from-attributes")
    assert resource().attributes["service.name"] == "from-attributes"
    monkeypatch.setenv("OTEL_SERVICE_NAME", "acme-sweeper")
    assert resource().attributes["service.name"] == "acme-sweeper"


def test_opentelemetry_s_own_records_are_not_exported(clean_service_name: None) -> None:
    """A failed export logs a warning: exported, it would go back through that exporter."""
    records = InMemoryLogRecordExporter()
    logged = logs(records)
    try:
        assert [h for h in logging.getLogger().handlers if isinstance(h, OtlpHandler)] == [
            logged.handler
        ]
        logging.getLogger("opentelemetry.exporter.otlp").warning("export failed: %s", MARKER)
        logging.getLogger("acme").warning("a seller's line: %s", MARKER)
        logging.getLogger("acme").debug("below INFO: %s", MARKER)
    finally:
        logged.shutdown()
    assert [str(r.log_record.body) for r in records.get_finished_logs()] == [
        f"a seller's line: {MARKER}"
    ]
    assert logged.handler not in logging.getLogger().handlers
