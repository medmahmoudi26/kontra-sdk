"""The logs of ``kontra serve --otlp``: the worker's Python logging, exported over OTLP beside
its spans (the ``opentelemetry`` extra; :mod:`kontra.serve.telemetry` imports this module
only when ``--otlp`` is given).

:class:`OtlpHandler`, on the root logger, hands each record at INFO and above to a log
provider of the worker's own -- never OpenTelemetry's global one, as for the spans: what a
seller logs through OpenTelemetry's API is theirs to send where they choose. The provider's
resource is the tracer provider's (:func:`kontra.serve._tracing.resource`), so a worker's
logs and spans are one ``service.name``: ``OTEL_SERVICE_NAME``, else
:data:`~kontra.serve._tracing.SERVICE_NAME`, which the graph page of ``kontra dev up --ui``
selects in Loki.

Each record carries, as flat attributes, the Temporal context it was emitted in
(:data:`TEMPORAL_ATTRIBUTES`): from what ``activity.logger`` and ``workflow.logger`` put in a
record (temporalio's ``temporal_activity`` and ``temporal_workflow`` extras, one dict each,
which OTLP cannot carry as they are), or, for any other logger inside an activity, from
``activity.info()``. Inside a span it also carries that span's trace and span ids (the
record's context is the current one), so a line leads to its trace, and a run's lines are
found by its workflow and run ids.

Logs are the seller's own output going to the seller's own collector, so unlike spans they
are not scrubbed: a record's message, an exception's message and its stack trace leave as
they are, and so does whatever a seller's code logs of a job (``docs/sellers.md``, "Scaling
and observability"). Nothing here logs a job's payloads by itself.
"""

from __future__ import annotations

import dataclasses
import logging
import time
import traceback
from collections.abc import Mapping

import opentelemetry.context
import temporalio.activity
from opentelemetry._logs import SeverityNumber
from opentelemetry.sdk._logs import LoggerProvider
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor, LogRecordExporter
from opentelemetry.util.types import AnyValue

from kontra.serve._tracing import resource

__all__ = ["TEMPORAL_ATTRIBUTES", "Logs", "OtlpHandler", "logs", "temporal_attributes"]

#: The attributes naming where a record was emitted, in a workflow or an activity.
TEMPORAL_ATTRIBUTES = (
    "workflow_id",
    "run_id",
    "workflow_type",
    "activity_id",
    "activity_type",
    "attempt",
    "task_queue",
)
#: temporalio's extras, which hold the context as one dict each.
_EXTRAS = ("temporal_activity", "temporal_workflow")
#: An activity's name for what a workflow calls its run id.
_RENAMED = {"workflow_run_id": "run_id"}
#: Python's level names that OpenTelemetry spells otherwise.
_SEVERITY_TEXT = {"WARNING": "WARN", "CRITICAL": "FATAL"}
_SEVERITY = (
    (logging.CRITICAL, SeverityNumber.FATAL),
    (logging.ERROR, SeverityNumber.ERROR),
    (logging.WARNING, SeverityNumber.WARN),
    (logging.INFO, SeverityNumber.INFO),
    (logging.DEBUG, SeverityNumber.DEBUG),
)


def _value(value: object) -> str | int:
    return value if isinstance(value, int) and not isinstance(value, bool) else str(value)


def temporal_attributes(record: logging.LogRecord) -> dict[str, str | int]:
    """The :data:`TEMPORAL_ATTRIBUTES` of the context ``record`` was emitted in (none
    outside one): see the module docstring."""
    found: dict[str, str | int] = {}
    for extra in _EXTRAS:
        details: object = record.__dict__.get(extra)
        if isinstance(details, Mapping):
            for key, value in details.items():
                name = _RENAMED.get(str(key), str(key))
                if name in TEMPORAL_ATTRIBUTES and value is not None:
                    found.setdefault(name, _value(value))
    if not found and temporalio.activity.in_activity():
        info = temporalio.activity.info()
        for name, value in (
            ("workflow_id", info.workflow_id),
            ("run_id", info.workflow_run_id),
            ("workflow_type", info.workflow_type),
            ("activity_id", info.activity_id),
            ("activity_type", info.activity_type),
            ("attempt", info.attempt),
            ("task_queue", info.task_queue),
        ):
            if value is not None:
                found[name] = _value(value)
    return found


def _severity(levelno: int) -> SeverityNumber:
    for floor, number in _SEVERITY:
        if levelno >= floor:
            return number
    return SeverityNumber.TRACE


def _attributes(record: logging.LogRecord) -> dict[str, AnyValue]:
    attributes: dict[str, AnyValue] = {
        "code.file.path": record.pathname,
        "code.function.name": record.funcName,
        "code.line.number": record.lineno,
    }
    if record.exc_info is not None:
        kind, error, tb = record.exc_info
        if kind is not None:
            attributes["exception.type"] = kind.__name__
        if error is not None:
            attributes["exception.message"] = str(error)
        if tb is not None:
            attributes["exception.stacktrace"] = "".join(
                traceback.format_exception(kind, error, tb)
            )
    attributes.update(temporal_attributes(record))
    return attributes


class OtlpHandler(logging.Handler):
    """Emits each record to ``provider``, under a logger named like the record's, with the
    current context (its span's ids), the record's message (formatted, when the handler has
    a formatter), its code location and exception, and :func:`temporal_attributes`.

    OpenTelemetry's own records (``opentelemetry.*``, e.g. an export that failed) are not
    emitted: they would be exported through the exporter they are about, and a collector
    that is down would feed its failures back to itself."""

    def __init__(self, provider: LoggerProvider, level: int = logging.INFO) -> None:
        super().__init__(level)
        self.provider = provider

    def emit(self, record: logging.LogRecord) -> None:
        if record.name == "opentelemetry" or record.name.startswith("opentelemetry."):
            return
        try:
            self.provider.get_logger(record.name).emit(
                timestamp=int(record.created * 1e9),
                observed_timestamp=time.time_ns(),
                context=opentelemetry.context.get_current(),
                severity_number=_severity(record.levelno),
                severity_text=_SEVERITY_TEXT.get(record.levelname, record.levelname),
                body=self.format(record) if self.formatter is not None else record.getMessage(),
                attributes=_attributes(record),
            )
        except Exception:
            self.handleError(record)


@dataclasses.dataclass(frozen=True)
class Logs:
    """The handler on the root logger, and the provider behind it."""

    handler: OtlpHandler
    provider: LoggerProvider

    def shutdown(self) -> None:
        """Take the handler off the root logger, export what is still batched and stop:
        ``kontra`` leaves through ``os._exit``, so the provider's own exit hook never runs."""
        logging.getLogger().removeHandler(self.handler)
        self.provider.shutdown()


def logs(exporter: LogRecordExporter, level: int = logging.INFO) -> Logs:
    """:class:`OtlpHandler` at ``level`` on the root logger, emitting to a provider of its own
    that exports (batched) to ``exporter``. The root logger's own level is left as it is
    (``kontra serve`` sets INFO when nothing has configured logging)."""
    provider = LoggerProvider(resource=resource())
    provider.add_log_record_processor(BatchLogRecordProcessor(exporter))
    handler = OtlpHandler(provider, level)
    logging.getLogger().addHandler(handler)
    return Logs(handler, provider)
