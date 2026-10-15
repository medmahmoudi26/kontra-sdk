"""``kontra serve --metrics`` and ``--otlp``: Temporal's own telemetry on the worker's client.

* ``--metrics IP:PORT`` is :func:`metrics_runtime`: a ``temporalio.runtime.Runtime`` whose
  telemetry serves Prometheus text at ``http://IP:PORT/metrics`` (``PrometheusConfig``).
  The worker's client connects with it, so what is served is the SDK's own metrics --
  ``temporal_worker_task_slots_available``, ``temporal_activity_schedule_to_start_latency``,
  the poll counters -- under Temporal's names and defaults (no ``_total`` suffix, durations
  in milliseconds), which is what Temporal's published SDK dashboard reads. Kontra adds no
  metric and renames none.
* ``--otlp URL`` is :func:`otlp_tracing`: ``temporalio.contrib.opentelemetry.TracingInterceptor``
  exporting spans over OTLP/gRPC to ``URL`` (:mod:`kontra.serve._tracing`). It needs the
  ``opentelemetry`` extra (``pip install 'kontra-sdk[opentelemetry]'``); without it,
  :class:`TelemetryError` says so.

Neither carries a payload, an input, a result, a failure message or a heartbeat detail out
of the worker: ``tests/codec/test_codec_telemetry.py`` scans the metrics text and the
exported spans of a worker serving a job for its marker (seam ⑤).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from temporalio.runtime import PrometheusConfig, Runtime, TelemetryConfig

if TYPE_CHECKING:
    from kontra.serve._tracing import Tracing

__all__ = ["EXTRA", "TelemetryError", "metrics_runtime", "otlp_tracing"]

#: The extra ``--otlp`` needs: temporalio's own of that name, and the OTLP exporter.
EXTRA = "opentelemetry"


class TelemetryError(ValueError):
    """A ``--metrics`` or ``--otlp`` the worker cannot honour: an address that does not
    parse or is taken, a URL that is not http(s), the extra not installed."""


def metrics_runtime(bind_address: str) -> Runtime:
    """A Temporal runtime serving Prometheus at ``bind_address`` (``IP:PORT``; the port is
    bound here, before any connection). Everything else is ``TelemetryConfig``'s default,
    as in the runtime temporalio makes when none is given."""
    try:
        return Runtime(telemetry=TelemetryConfig(metrics=PrometheusConfig(bind_address)))
    except ValueError as e:  # temporalio: "Invalid Prometheus address: ..." / "... in use"
        raise TelemetryError(
            f"cannot serve metrics on {bind_address!r} ({e}); give an IP address and a "
            "port, e.g. 127.0.0.1:9464"
        ) from None


def otlp_tracing(url: str) -> Tracing:
    """``TracingInterceptor`` exporting to the OTLP/gRPC collector at ``url``
    (``http://`` for plaintext, ``https://`` for TLS). Shut the returned :class:`Tracing`
    down when the worker stops: spans are exported in batches."""
    from urllib.parse import urlsplit

    try:
        parts = urlsplit(url)
        valid = parts.scheme in ("http", "https") and bool(parts.hostname) and bool(parts.port)
    except ValueError:  # a port that is not a number
        valid = False
    if not valid:
        raise TelemetryError(
            f"--otlp takes the collector's URL, e.g. http://localhost:4317, not {url!r}"
        )
    try:
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

        from kontra.serve._tracing import tracing
    except ImportError as e:
        raise TelemetryError(
            f"--otlp needs OpenTelemetry, which is not installed ({e.name or e}): "
            f"pip install 'kontra-sdk[{EXTRA}]'"
        ) from None
    return tracing(OTLPSpanExporter(endpoint=url))
