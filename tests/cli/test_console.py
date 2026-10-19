"""The graph page's routes in the dev codec server (``kontra dev up --ui``; decisions I28):
the page and its policy, the run's graph painted on ``kontra graph``'s static graph for its
task queue, the links, the live stream, the list of runs, and how each route refuses. On a
real dev server, the codec server on a thread; the seam ⑤ scan of the same routes is
``tests/codec/test_codec_console.py``."""

from __future__ import annotations

import asyncio
import copy
import json
import os
import re
import threading
import time
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlsplit

import httpx
import pytest
from temporalio.client import Client
from temporalio.worker import Worker

import kontra.cli._console as console_module
from kontra import graph
from kontra.cli._codec_server import CodecServer
from kontra.cli._console import MAX_STREAMS, Connected, Console
from kontra.cli._graph import save_static
from kontra.codec import KontraCodec
from kontra.codec.age import generate_identity
from kontra.serve.worker import workflow_runner
from kontra.testing.devserver import (
    CLI_ENV,
    DevServer,
    acquire_shared_dev_server,
    release_shared_dev_server,
)

from .console_flows import QuickFlow, RetryFlow, WaitFlow, flaky, quick

pytestmark = pytest.mark.temporal

SECRET, PUBLIC = generate_identity()
UI = "http://localhost:8233"
GRAFANA = "http://localhost:3000"
PAGE = Path(console_module.__file__).with_name("_console.html")


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


def _codec() -> KontraCodec:
    return KontraCodec(SECRET, [PUBLIC], role="seller")


Serve = Callable[..., CodecServer]


@pytest.fixture
def serve(dev_server: DevServer, tmp_path: Path) -> Iterator[Serve]:
    """A codec server with a console on ``tmp_path``, reading the dev server unless told
    to ``connect`` otherwise."""
    started: list[tuple[CodecServer, Console, threading.Thread]] = []

    async def to_dev() -> Connected:
        return Connected(await dev_server.connect("default"), "default")

    def make(
        connect: Callable[[], Awaitable[Connected]] | None = None, **links: str | None
    ) -> CodecServer:
        console = Console(
            tmp_path,
            _codec,
            connect=connect or to_dev,
            **{"temporal_ui": UI, "grafana": GRAFANA, **links},  # pyright: ignore[reportArgumentType]
        )
        srv = CodecServer(0, codec=_codec, origins=[UI], token="t", console=console)
        thread = threading.Thread(target=srv.serve_forever, daemon=True)
        thread.start()
        started.append((srv, console, thread))
        return srv

    yield make
    for srv, console, thread in started:
        console.close()
        srv.shutdown()
        srv.server_close()
        thread.join(5)


@pytest.fixture
async def worker(dev_server: DevServer) -> AsyncIterator[tuple[Client, str]]:
    client = await dev_server.connect("default")
    queue = f"console-{uuid.uuid4().hex[:8]}"
    async with Worker(
        client,
        task_queue=queue,
        workflows=[RetryFlow, QuickFlow, WaitFlow],
        activities=[flaky, quick],
        workflow_runner=workflow_runner(),
    ):
        yield client, queue


def _get(srv: CodecServer, path: str, **headers: str) -> httpx.Response:
    with httpx.Client(trust_env=False, timeout=60) as http:
        return http.get(f"http://127.0.0.1:{srv.port}{path}", headers=headers)


def _events(raw: bytes) -> list[tuple[str, Any]]:
    """An event stream's ``(event, data)`` pairs, comments and the retry field left out."""
    out: list[tuple[str, Any]] = []
    for block in raw.decode().split("\n\n"):
        fields = dict(
            line.split(": ", 1) for line in block.splitlines() if line and not line.startswith(":")
        )
        if "event" in fields:
            out.append((fields["event"], json.loads(fields["data"])))
    return out


def _stream(srv: CodecServer, path: str, opened: threading.Event | None = None) -> bytes:
    """The whole event stream (it ends after the closed run's graph); ``opened`` is set
    once it answered."""
    out = b""
    with (
        httpx.Client(trust_env=False, timeout=120) as http,
        http.stream("GET", f"http://127.0.0.1:{srv.port}{path}") as r,
    ):
        assert r.status_code == 200, r.read()
        if opened is not None:
            opened.set()
        for chunk in r.iter_bytes():
            out += chunk
    return out


def _node(g: dict[str, Any], node_id: str) -> dict[str, Any]:
    [found] = [n for n in g["nodes"] if n["id"] == node_id]
    return found


# --------------------------------------------------------------------------- the page


def test_the_page_runs_its_own_script_and_style_alone(serve: Serve) -> None:
    srv = serve()
    first, second = _get(srv, "/graph"), _get(srv, f"/graph?w=x&r={uuid.uuid4()}")
    for r in (first, second):
        assert r.status_code == 200
        assert r.headers["Content-Type"] == "text/html; charset=utf-8"
        assert r.headers["Cache-Control"] == "no-store"
        assert r.headers["X-Content-Type-Options"] == "nosniff"
        assert r.headers["Referrer-Policy"] == "no-referrer"
        assert r.headers["X-Frame-Options"] == "DENY"
        policy = r.headers["Content-Security-Policy"]
        [nonce] = set(re.findall(r"'nonce-([A-Za-z0-9_-]+)'", policy))
        assert len(nonce) >= 22
        assert policy == (
            f"default-src 'none'; script-src 'nonce-{nonce}'; style-src 'nonce-{nonce}'; "
            "connect-src 'self'; img-src 'self' data:; base-uri 'none'; form-action 'none'; "
            "frame-ancestors 'none'"
        )
        tags = re.findall(r"<(script|style)\b([^>]*)>", r.text)
        assert len(tags) == 2
        assert all(attrs == f' nonce="{nonce}"' for _, attrs in tags)
        assert "__NONCE__" not in r.text
    assert first.headers["Content-Security-Policy"] != second.headers["Content-Security-Policy"]


def test_the_page_needs_nothing_its_policy_refuses() -> None:
    """No inline style or handler attribute (``'unsafe-inline'`` would be needed), nothing
    loaded from elsewhere, and data from the server never parsed as HTML."""
    text = PAGE.read_text()
    assert text.startswith("<!doctype html>")
    markup = re.sub(r"<(script|style)\b[^>]*>.*?</\1>", "", text, flags=re.S)
    assert not re.search(r"\sstyle\s*=", markup)
    assert not re.search(r"\son[a-z]+\s*=", markup)
    script = re.search(r"<script\b[^>]*>(.*?)</script>", text, re.S)
    assert script is not None
    for unsafe in (
        "innerHTML",
        "outerHTML",
        "insertAdjacentHTML",
        "document.write",
        "eval(",
        "javascript:",
    ):
        assert unsafe not in script.group(1), unsafe
    assert 'setAttribute("style"' not in script.group(1)
    assert not re.search(r"https?://", markup)  # no external resource
    for route in (
        "/api/workflows",
        "/api/graph",
        "/api/graph/events",
        "/api/io",
        "X-Kontra-Console",
    ):
        assert route in script.group(1), route


# --------------------------------------------------------------------------- the graph


async def _finished(client: Client, queue: str, flow: Any = QuickFlow) -> tuple[str, str]:
    wf = f"console-{uuid.uuid4().hex}"
    handle = await client.start_workflow(flow.run, "hi", id=wf, task_queue=queue)
    await handle.result()
    return wf, handle.result_run_id or ""


async def test_a_run_is_painted_on_the_static_graph_of_its_task_queue(
    serve: Serve, worker: tuple[Client, str], tmp_path: Path
) -> None:
    client, queue = worker
    srv = serve()
    wf, run = await _finished(client, queue)
    alone = (await asyncio.to_thread(_get, srv, f"/api/graph?w={wf}&r={run}")).json()
    assert alone["static_graph"] is False
    assert [n["id"] for n in alone["nodes"]] == ["quick"]
    assert all("reached" not in n for n in alone["nodes"])

    # kontra graph's mock runs took a branch this run did not.
    static = copy.deepcopy(alone)
    static["nodes"].append({"id": "never", "kind": "activity", "name": "never", "label": "never"})
    static["edges"].append({"from": "quick", "to": "never"})
    save_static(tmp_path, queue + " ", offering="acme/x@1.0.0", runs=8, seed=0, graph=static)
    other = (await asyncio.to_thread(_get, srv, f"/api/graph?w={wf}")).json()
    assert other["static_graph"] is False  # saved for another task queue: not this run's
    save_static(tmp_path, queue, offering="acme/x@1.0.0", runs=8, seed=0, graph=static)
    painted = (await asyncio.to_thread(_get, srv, f"/api/graph?w={wf}")).json()
    assert painted["static_graph"] is True
    assert painted["workflow"]["run_id"] == run  # the latest run, without r=
    assert (_node(painted, "quick")["reached"], _node(painted, "never")["reached"]) == (True, False)
    assert _node(painted, "never")["links"] == {}  # nothing to link: the run never got there
    reached = {(e["from"], e["to"]): e["reached"] for e in painted["edges"]}
    assert reached[("quick", "never")] is False
    assert reached[("start", "quick")] is True


async def test_the_links_open_the_event_and_the_logs_of_its_span(
    serve: Serve, worker: tuple[Client, str]
) -> None:
    client, queue = worker
    wf, run = await _finished(client, queue)
    g = (await asyncio.to_thread(_get, serve(), f"/api/graph?w={wf}&r={run}")).json()
    node = _node(g, "quick")
    links = node["links"]
    assert links["temporal"] == (
        f"{UI}/namespaces/default/workflows/{wf}/{run}/history/events/{node['ref']['event_id']}"
    )
    assert (
        g["workflow"]["links"]["temporal"]
        == f"{UI}/namespaces/default/workflows/{wf}/{run}/history"
    )
    assert "trace" not in links  # this worker sends no trace context
    explore = urlsplit(links["logs"])
    assert f"{explore.scheme}://{explore.netloc}{explore.path}" == f"{GRAFANA}/explore"
    [pane] = json.loads(parse_qs(explore.query)["panes"][0]).values()
    assert pane["datasource"] == "loki"
    [query] = pane["queries"]
    assert query["datasource"] == {"type": "loki", "uid": "loki"}
    assert query["expr"] == (
        f'{{service_name="kontra-worker"}} | workflow_id="{wf}" | run_id="{run}"'
        ' | activity_type="quick"'
    )
    # Explore takes a range of strings (it drops numbers): a second either side of the span.
    span = node["span"]
    assert pane["range"] == {
        "from": str(int(graph._nanos(span["from"]) // 10**6) - 1000),  # pyright: ignore[reportOptionalOperand]
        "to": str(-(-graph._nanos(span["to"]) // 10**6) + 1000),  # pyright: ignore[reportOptionalOperand]
    }

    bare = (
        await asyncio.to_thread(_get, serve(grafana=None, temporal_ui=None), f"/api/graph?w={wf}")
    ).json()
    assert _node(bare, "quick")["links"] == {}
    assert bare["workflow"]["links"] == {}


async def test_the_log_selector_is_the_service_name_the_worker_logs_under(
    serve: Serve, worker: tuple[Client, str]
) -> None:
    from kontra.serve._tracing import SERVICE_NAME

    assert console_module.LOG_SERVICE == SERVICE_NAME
    client, queue = worker
    wf, _ = await _finished(client, queue)
    g = (await asyncio.to_thread(_get, serve(log_service='odd"name'), f"/api/graph?w={wf}")).json()
    pane = json.loads(parse_qs(urlsplit(_node(g, "quick")["links"]["logs"]).query)["panes"][0])
    assert pane["kon"]["queries"][0]["expr"].startswith('{service_name="odd\\"name"} | ')


async def test_the_stream_shows_a_retry_before_history_does_then_ends(
    serve: Serve, worker: tuple[Client, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """``flaky`` fails twice, a second each, then succeeds: DescribeWorkflowExecution says
    its attempt while it runs, which history says only once it closes."""
    monkeypatch.setattr(console_module, "_POLL", 0.2)
    client, queue = worker
    srv = serve()
    wf = f"console-{uuid.uuid4().hex}"
    handle = await client.start_workflow(RetryFlow.run, "hi", id=wf, task_queue=queue)
    raw = await asyncio.to_thread(_stream, srv, f"/api/graph/events?w={wf}")
    assert raw.startswith(b"retry: 3000\n\n")
    events = _events(raw)
    assert [name for name, _ in events][-2:] == ["graph", "end"]
    graphs = [g for name, g in events if name == "graph"]
    states = [
        (_node(g, "flaky").get("status"), _node(g, "flaky").get("attempt"))
        for g in graphs
        if g["nodes"]
    ]
    assert ("running", 1) in states
    third = next(i for i, (_, attempt) in enumerate(states) if attempt == 3)
    assert states[third][0] in ("running", "scheduled")  # from describe: history has no close
    assert states[-1] == ("completed", 3)
    assert graphs[-1]["workflow"]["status"] == "completed"
    assert await handle.result() == "hi"
    # A closed run: its graph at once, then the end.
    closed = _events(await asyncio.to_thread(_stream, srv, f"/api/graph/events?w={wf}"))
    assert [name for name, _ in closed] == ["graph", "end"]


def test_an_unnamed_batch_is_read_again_until_its_child_names_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A batch child is named from its own history, which grows while the run's does not:
    the stream reads the graph again at every poll while one is unnamed (``last`` None),
    sends it when it differs, and goes back to asking whether the run changed."""
    monkeypatch.setattr(console_module, "_POLL", 0.01)

    def snap(name: str, *, closed: bool = False, unnamed: bool = False) -> Any:
        g = {
            "workflow": {"run_id": "r", "status": "completed" if closed else "running"},
            "nodes": [{"id": name, "name": name, "kind": "child", "status": "running"}],
            "edges": [],
        }
        return console_module._Snapshot(g, {}, ("k",), closed, unnamed)

    script = [
        snap("kontra.batch.v1", unnamed=True),
        snap("kontra.batch.v1", unnamed=True),  # the same graph: not sent again
        snap("kontra.batch.v1[item]"),
        None,  # unchanged
        snap("kontra.batch.v1[item]", closed=True),
    ]
    asked: list[Any] = []

    class Scripted(Console):
        async def _snapshot(self, *args: Any, **kwargs: Any) -> Any:
            return script.pop(0)

        async def _changed(self, workflow_id: str, run_id: str, last: Any) -> Any:
            asked.append(last)
            return script.pop(0)

    console = Scripted(tmp_path, _codec)
    try:
        events = _events(b"".join(console.stream("w", None)))
    finally:
        console.close()
    assert asked == [None, None, ("k",), ("k",)]
    assert [
        (name, data["nodes"][0]["id"] if name == "graph" else None) for name, data in events
    ] == [
        ("graph", "kontra.batch.v1"),
        ("graph", "kontra.batch.v1[item]"),
        ("graph", "kontra.batch.v1[item]"),
        ("end", None),
    ]
    assert console_module._unnamed_batch(
        {"nodes": [{"kind": "child", "name": "kontra.batch.v1", "status": "running"}]}
    )
    assert not console_module._unnamed_batch(
        {"nodes": [{"kind": "child", "name": "kontra.batch.v1", "status": "completed"}]}
    )


async def test_streams_are_bounded(
    serve: Serve, worker: tuple[Client, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    assert MAX_STREAMS == 16
    monkeypatch.setattr(console_module, "_POLL", 0.2)
    client, queue = worker
    srv = serve()
    assert srv.console is not None
    srv.console.streams = threading.BoundedSemaphore(1)
    wf = f"console-{uuid.uuid4().hex}"
    handle = await client.start_workflow(WaitFlow.run, "hi", id=wf, task_queue=queue)
    opened = threading.Event()
    first = asyncio.create_task(
        asyncio.to_thread(_stream, srv, f"/api/graph/events?w={wf}", opened)
    )
    assert await asyncio.to_thread(opened.wait, 20)
    r = await asyncio.to_thread(_get, srv, f"/api/graph/events?w={wf}")
    assert r.status_code == 503
    assert r.json() == {"error": "16 live graphs are open already"}
    assert r.headers["Cache-Control"] == "no-store"
    await handle.terminate()
    events = _events(await first)
    assert events[-1][0] == "end"
    assert events[-2][1]["workflow"]["status"] == "terminated"
    # The slot is free again (the run is closed: its graph, then the end).
    again = await asyncio.to_thread(_get, srv, f"/api/graph/events?w={wf}")
    assert again.status_code == 200


async def test_the_runs_list_is_metadata(serve: Serve, worker: tuple[Client, str]) -> None:
    client, queue = worker
    wf, run = await _finished(client, queue)
    srv = serve()
    deadline = time.monotonic() + 30
    while True:  # visibility catches up a moment later
        listed = (await asyncio.to_thread(_get, srv, "/api/workflows")).json()
        mine = [x for x in listed if x["workflow_id"] == wf]
        if mine or time.monotonic() > deadline:
            break
        await asyncio.sleep(0.3)
    assert len(listed) <= console_module.WORKFLOWS_LIMIT
    [entry] = mine
    assert set(entry) == {
        "workflow_id", "run_id", "type", "status", "start_time", "close_time", "task_queue",
    }  # fmt: skip
    assert (entry["run_id"], entry["type"], entry["status"], entry["task_queue"]) == (
        run,
        "QuickFlow",
        "completed",
        queue,
    )
    assert all(x["type"] != "kontra.batch.v1" for x in listed)


# --------------------------------------------------------------------------- refusals


def test_bad_requests_are_one_json_line(serve: Serve, dev_server: DevServer) -> None:
    srv = serve()
    for path, status in (
        ("/api/graph", 400),
        ("/api/graph?w=", 400),
        ("/api/graph?w=a&w=b", 400),
        ("/api/graph?w=a&r=not%20a%20run", 400),
        ("/api/graph?w=" + "x" * 1001, 400),
        (f"/api/graph?w=nothing-{uuid.uuid4().hex}", 404),
        (f"/api/graph/events?w=nothing-{uuid.uuid4().hex}", 404),
    ):
        r = _get(srv, path)
        assert r.status_code == status, (path, r.text)
        assert r.headers["Content-Type"] == "application/json"
        assert set(r.json()) == {"error"}
        assert "\n" not in r.json()["error"]
        assert "Traceback" not in r.text
    io = _get(srv, f"/api/io?w=nothing-{uuid.uuid4().hex}&node=")
    assert (io.status_code, io.json()) == (403, {"error": "X-Kontra-Console: 1 is required"})
    assert _get(srv, "/api/nope").status_code == 404


def test_without_temporal_the_routes_say_so_and_decode_still_answers(serve: Serve) -> None:
    from google.protobuf import json_format
    from temporalio.api.common.v1 import Payload, Payloads

    async def unreachable() -> Connected:
        raise RuntimeError("Failed client connect: Connection refused\nmore detail")

    srv = serve(connect=unreachable)
    r = _get(srv, "/api/workflows")
    assert (r.status_code, r.json()) == (
        502,
        {"error": "RuntimeError: Failed client connect: Connection refused"},
    )
    plain = Payload(metadata={"encoding": b"json/plain"}, data=b'"x"')
    with httpx.Client(trust_env=False) as http:
        decoded = http.post(
            f"http://127.0.0.1:{srv.port}/decode",
            content=json_format.MessageToJson(Payloads(payloads=[plain])),
            headers={"Content-Type": "application/json", "Origin": UI},
        )
    assert decoded.status_code == 200
    # An error of our own words says only what it is: never a payload's text.
    assert console_module._said(ValueError("cannot read {'data': 'secret'}")) == (
        "ValueError (the codec server's log has no more)"
    )


def test_the_console_routes_take_the_page_s_origin_alone(serve: Serve) -> None:
    srv = serve()
    for origin in (UI, "http://127.0.0.1:8233", "https://evil.example", "null"):
        r = _get(srv, "/api/workflows", Origin=origin)
        assert r.status_code == 403, origin
        assert "Access-Control-Allow-Origin" not in r.headers
    for host in ("evil.example", f"evil.example:{srv.port}", "127.0.0.1:1"):
        assert _get(srv, "/graph", Host=host).status_code == 403, host
    assert _get(srv, "/graph", Host=f"localhost:{srv.port}").status_code == 200
    # A navigation from another site gets the page, which holds no data; its data routes
    # answer same-origin requests only.
    assert _get(srv, "/graph", **{"Sec-Fetch-Site": "cross-site"}).status_code == 200
    r = _get(srv, "/api/workflows", **{"Sec-Fetch-Site": "cross-site"})
    assert (r.status_code, r.json()) == (403, {"error": "same-origin requests only"})


def test_a_codec_server_without_a_console_serves_no_page() -> None:
    srv = CodecServer(0, codec=_codec, origins=[UI], token="t")
    thread = threading.Thread(target=srv.serve_forever, daemon=True)
    thread.start()
    try:
        assert _get(srv, "/graph").status_code == 404
        assert _get(srv, "/api/workflows").status_code == 404
    finally:
        srv.shutdown()
        srv.server_close()
        thread.join(5)


def test_link_bases_must_be_http_urls(tmp_path: Path) -> None:
    for bad in ("ftp://grafana", "http://grafana?x=1", "localhost:3000"):
        with pytest.raises(ValueError, match="grafana"):
            Console(tmp_path, _codec, grafana=bad)
