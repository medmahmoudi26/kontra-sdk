"""``kontra serve --metrics IP:PORT`` and ``--otlp URL``: off unless asked, handed to the
worker as the ``runtime`` and client ``interceptors`` of its connection, refused with one
line when they cannot be honoured. What they carry is seam ⑤'s
(``tests/codec/test_codec_telemetry.py``)."""

from __future__ import annotations

import contextlib
import socket
import sys
from collections.abc import Generator
from pathlib import Path
from typing import Any

import httpx
import pytest
from temporalio.contrib.opentelemetry import TracingInterceptor
from temporalio.runtime import Runtime

import kontra.cli._seller as seller_module
from kontra.serve.telemetry import EXTRA

from .support import Cli


class FakeWorker:
    """What ``load_worker`` yields: records how it was run, and what was served meanwhile."""

    def __init__(self, metrics_url: str | None = None) -> None:
        self.metrics_url = metrics_url
        self.runs: list[dict[str, Any]] = []
        self.served: bytes | None = None

    def run(self, **kwargs: Any) -> None:
        self.runs.append(kwargs)
        if self.metrics_url is not None:
            with httpx.Client(trust_env=False) as http:
                self.served = http.get(self.metrics_url, timeout=10).content


def _loading(monkeypatch: pytest.MonkeyPatch, worker: FakeWorker) -> None:
    @contextlib.contextmanager
    def load(directory: str | Path) -> Generator[FakeWorker]:
        yield worker

    monkeypatch.setattr(seller_module, "load_worker", load)


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def test_off_unless_asked(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    worker = FakeWorker()
    _loading(monkeypatch, worker)
    result = Cli(tmp_path)("serve")
    assert result.exit_code == 0, result.output
    assert worker.runs == [{"runtime": None, "interceptors": []}]


def test_metrics_and_otlp_reach_the_connection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    port = _free_port()
    worker = FakeWorker(f"http://127.0.0.1:{port}/metrics")
    _loading(monkeypatch, worker)
    result = Cli(tmp_path)(
        "serve", "--metrics", f"127.0.0.1:{port}", "--otlp", "http://127.0.0.1:4317"
    )
    assert result.exit_code == 0, result.output
    [run] = worker.runs
    assert isinstance(run["runtime"], Runtime)
    [interceptor] = run["interceptors"]
    assert isinstance(interceptor, TracingInterceptor)
    assert worker.served is not None  # the endpoint answered while the worker ran


def test_what_cannot_be_honoured_is_one_line(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    worker = FakeWorker()
    _loading(monkeypatch, worker)
    cli = Cli(tmp_path)
    for bad in ("localhost:9464", "9464", "127.0.0.1:99999"):
        result = cli("serve", "--metrics", bad)
        assert result.exit_code == 1, bad
        assert "give an IP address and a port" in result.stderr
    with socket.socket() as taken:
        taken.bind(("127.0.0.1", 0))
        taken.listen()
        busy = f"127.0.0.1:{taken.getsockname()[1]}"
        result = cli("serve", "--metrics", busy)
        assert result.exit_code == 1
        assert busy in result.stderr
    for bad in ("collector:4317", "ftp://collector:4317", "http://collector", "http://:4317"):
        result = cli("serve", "--otlp", bad)
        assert result.exit_code == 1, bad
        assert "--otlp takes the collector's URL" in result.stderr
    assert worker.runs == []  # nothing ran


def test_otlp_without_the_extra_names_it(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    worker = FakeWorker()
    _loading(monkeypatch, worker)
    # As if `pip install kontra-sdk` without the extra: the exporter does not import.
    monkeypatch.setitem(sys.modules, "opentelemetry.exporter.otlp.proto.grpc.trace_exporter", None)
    result = Cli(tmp_path)("serve", "--otlp", "http://127.0.0.1:4317")
    assert result.exit_code == 1
    assert f"pip install 'kontra-sdk[{EXTRA}]'" in result.stderr
    assert worker.runs == []
