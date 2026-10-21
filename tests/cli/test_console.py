"""The graph page's routes in the dev codec server (``kontra dev up --ui``; decisions I28,
I36): the page and its policy, the run's graph painted on the static graph ``kontra serve``
drew for its task queue (I35), what the page says without one, the links, the live stream of
change signals and the one fetch each sends the page to, the list of runs, and how each route
refuses. On a
real dev server, the codec server on a thread; the seam ⑤ scan of the same routes is
``tests/codec/test_codec_console.py``."""

from __future__ import annotations

import asyncio
import copy
import json
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
from kontra.testing.devserver import DevServer

from .console_flows import (
    AgentLike,
    BackoffFlow,
    QuickFlow,
    RetryFlow,
    TurnLike,
    WaitFlow,
    fails_once,
    flaky,
    quick,
)

pytestmark = pytest.mark.temporal

SECRET, PUBLIC = generate_identity()
UI = "http://localhost:8233"
GRAFANA = "http://localhost:3000"
PAGE = Path(console_module.__file__).with_name("_console.html")


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
        workflows=[RetryFlow, QuickFlow, WaitFlow, BackoffFlow, TurnLike, AgentLike],
        activities=[flaky, quick, fails_once],
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


def _watch(srv: CodecServer, w: str) -> tuple[list[tuple[str, Any]], list[dict[str, Any]]]:
    """What the page does: the event stream, and for each ``session-stat`` the one
    ``/api/session`` fetch it sends the page to. The frames, and the graphs fetched."""
    frames: list[tuple[str, Any]] = []
    graphs: list[dict[str, Any]] = []
    buf = b""
    with (
        httpx.Client(trust_env=False, timeout=120) as http,
        http.stream("GET", f"http://127.0.0.1:{srv.port}/api/events?w={w}") as r,
    ):
        assert r.status_code == 200, r.read()
        for chunk in r.iter_bytes():
            buf += chunk
            *done, buf = buf.split(b"\n\n")
            for name, data in _events(b"\n\n".join(done) + b"\n\n"):
                frames.append((name, data))
                if name == "session-stat":
                    got = _get(srv, f"/api/session?w={w}", **{"X-Kontra-Console": "1"})
                    assert got.status_code == 200, got.text
                    graphs.append(got.json()["graph"])
    return frames, graphs


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
        assert "__SESSION__" not in r.text
    assert first.headers["Content-Security-Policy"] != second.headers["Content-Security-Policy"]
    # The POST routes' token (I36): this console's, the same in every page it serves, and
    # another console's differs.
    token = re.compile(r'const SESSION_TOKEN = "([A-Za-z0-9_-]{32,})"')
    [mine], [again] = token.findall(first.text), token.findall(second.text)
    [theirs] = token.findall(_get(serve(), "/graph").text)
    assert mine == again != theirs


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
        "/api/session",
        "/api/events",
        "/api/io",
        "/api/approve",
        "/api/callback",
        "/api/pull",
        "X-Kontra-Console",
        "X-Kontra-Session",
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

    assert alone["handler"] is False  # a workflow offering's run: one is drawn for it

    # The mock runs `kontra serve` drew the graph from took a branch this run did not.
    static = copy.deepcopy(alone)
    static["nodes"].append({"id": "never", "kind": "activity", "name": "never", "label": "never"})
    static["edges"].append({"from": "quick", "to": "never"})

    def drawn(task_queue: str) -> None:
        save_static(
            tmp_path,
            task_queue,
            offering="acme/x@1.0.0",
            contract_hash="sha256:x",
            code_hash="sha256:code",
            runs=8,
            seed=0,
            graph=static,
        )

    drawn(queue + " ")
    other = (await asyncio.to_thread(_get, srv, f"/api/graph?w={wf}")).json()
    assert other["static_graph"] is False  # saved for another task queue: not this run's
    drawn(queue)
    painted = (await asyncio.to_thread(_get, srv, f"/api/graph?w={wf}")).json()
    assert painted["static_graph"] is True
    assert painted["handler"] is False
    assert painted["workflow"]["run_id"] == run  # the latest run, without r=
    assert (_node(painted, "quick")["reached"], _node(painted, "never")["reached"]) == (True, False)
    assert _node(painted, "never")["links"] == {}  # nothing to link: the run never got there
    reached = {(e["from"], e["to"]): e["reached"] for e in painted["edges"]}
    assert reached[("quick", "never")] is False
    assert reached[("start", "quick")] is True


async def test_a_handler_offering_s_runs_say_they_have_no_static_graph(
    serve: Serve, worker: tuple[Client, str]
) -> None:
    """A handler offering's job (``kontra.turn.v1``) and the agent it starts have no static
    graph (decisions I35): told by the types Temporal reports, the job's and its root's. It is
    the one classification OD7 reads (I36 §7): the session pane says the same runs are a
    hired turn's, never the console's to answer, and a workflow offering's run is neither."""
    client, queue = worker
    srv = serve()
    wf = f"console-{uuid.uuid4().hex}"
    await (await client.start_workflow(TurnLike.run, "hi", id=wf, task_queue=queue)).result()
    plain, _ = await _finished(client, queue)
    job = (await asyncio.to_thread(_get, srv, f"/api/graph?w={wf}")).json()
    agent = (await asyncio.to_thread(_get, srv, f"/api/graph?w={wf}-agent")).json()
    for g in (job, agent):
        assert (g["static_graph"], g["handler"]) == (False, True)
    assert agent["workflow"]["type"] == "AgentLike"
    for w, turn in ((wf, True), (f"{wf}-agent", True), (plain, False)):
        both = (
            await asyncio.to_thread(_get, srv, f"/api/session?w={w}", **{"X-Kontra-Console": "1"})
        ).json()
        said = both["session"]["agent"]
        assert (both["graph"]["handler"], said["hired"]) == (turn, turn), w
        assert said["interactive"] is False


def test_the_page_tells_no_one_to_run_anything_but_kontra_serve() -> None:
    """Without a static graph the page says ``kontra serve`` draws one, or, for a handler
    offering, that only the run is drawn; ``kontra graph`` is hidden (I35) and named in no
    string of the page or of its server but code comments and docstrings."""
    import ast

    page = PAGE.read_text("utf-8")
    assert "kontra graph" not in page
    assert (
        '"No static graph for this offering yet: start ", el("code", {}, "kontra serve"), ' in page
    )
    assert '" once and it is drawn in the background."' in page
    assert "This offering's entry is a handler, so only what this run did is drawn." in page
    assert page.count('the mock runs ", el("code", {}, "kontra serve"), " drew the graph from') == 2
    tree = ast.parse(Path(console_module.__file__).read_text("utf-8"))
    docstrings = {
        id(node.body[0].value)  # pyright: ignore[reportAttributeAccessIssue]
        for node in ast.walk(tree)
        if isinstance(node, ast.Module | ast.ClassDef | ast.FunctionDef | ast.AsyncFunctionDef)
        and node.body
        and isinstance(node.body[0], ast.Expr)
        and isinstance(node.body[0].value, ast.Constant)
    }
    said = [
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant)
        and isinstance(node.value, str)
        and id(node) not in docstrings
    ]
    assert not [s for s in said if "kontra graph" in s]


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
    its attempt while it runs, which history says only once it closes. The stream says that
    something changed (``session-stat``, a change signal alone, OD2); the page's
    ``/api/session`` after each is what shows it."""
    monkeypatch.setattr(console_module, "_POLL", 0.2)
    client, queue = worker
    srv = serve()
    wf = f"console-{uuid.uuid4().hex}"
    handle = await client.start_workflow(RetryFlow.run, "hi", id=wf, task_queue=queue)
    raw = await asyncio.to_thread(_stream, srv, f"/api/events?w={wf}")
    assert raw.startswith(b"retry: 3000\n\n")
    frames = _events(raw)
    assert [name for name, _ in frames][-2:] == ["session-stat", "end"]
    assert {name for name, _ in frames} == {"session-stat", "end"}  # never a graph, nor a row
    stats = [data for name, data in frames if name == "session-stat"]
    assert [s["rev"] for s in stats] == list(range(len(stats)))
    assert set(stats[0]) == {"history_length", "count", "last_offset", "status", "turn", "rev"}
    assert (stats[-1]["status"], stats[-1]["count"], stats[-1]["turn"]) == ("completed", 0, None)
    assert await handle.result() == "hi"
    # Again, fetching on each signal as the page does.
    wf = f"console-{uuid.uuid4().hex}"
    handle = await client.start_workflow(RetryFlow.run, "hi", id=wf, task_queue=queue)
    frames, graphs = await asyncio.to_thread(_watch, srv, wf)
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
    # A closed run: its signal at once, then the end.
    closed = _events(await asyncio.to_thread(_stream, srv, f"/api/events?w={wf}"))
    assert [name for name, _ in closed] == ["session-stat", "end"]


async def test_a_step_backing_off_shows_its_failure_before_history_has_it(
    serve: Serve, worker: tuple[Client, str]
) -> None:
    """``fails_once`` failed its first attempt and waits a minute for its second: history
    has nothing of the attempt (its Started event, with the last failure, is written when
    the activity closes), so the graph's attempt, its wait and ``/api/io``'s failure message
    come from ``DescribeWorkflowExecution``."""
    client, queue = worker
    srv = serve()
    wf = f"console-{uuid.uuid4().hex}"
    handle = await client.start_workflow(BackoffFlow.run, "hi", id=wf, task_queue=queue)
    try:
        deadline = time.monotonic() + 30
        while True:
            g = (await asyncio.to_thread(_get, srv, f"/api/graph?w={wf}")).json()
            found = [n for n in g["nodes"] if n["id"] == "fails_once"]
            if (found and found[0]["attempt"] == 2) or time.monotonic() > deadline:
                break
            await asyncio.sleep(0.2)
        [step] = found
        assert (step["status"], step["attempt"]) == ("scheduled", 2)
        assert step["failure"]["kind"] == "application"
        t = step["t"]
        assert (t["started"], t["closed"]) == (None, None)
        # The wait counts from the failure, not from when the step was first scheduled.
        assert graph._nanos(t["scheduled"]) < graph._nanos(t["last_failed"])  # pyright: ignore[reportOperatorIssue]
        history = json.dumps((await handle.fetch_history()).to_json_dict())
        assert "attempt 1 failed" not in history
        io = await asyncio.to_thread(
            _get, srv, f"/api/io?w={wf}&node=fails_once", **{"X-Kontra-Console": "1"}
        )
        assert io.status_code == 200, io.text
        assert io.json()["failure"]["message"] == "attempt 1 failed on hi"
        assert io.json()["input"] == ["hi"]
    finally:
        await handle.terminate()


def _described(*pending: tuple[int, int, int | None, bool, bytes], length: int = 7) -> Any:
    """A ``WorkflowExecutionDescription`` as :func:`_change_key` reads it: pending
    activities of ``(state, attempt, last started second, has a last failure, heartbeat)``."""
    from types import SimpleNamespace

    from google.protobuf.timestamp_pb2 import Timestamp
    from temporalio.api.common.v1 import Payload, Payloads
    from temporalio.api.failure.v1 import Failure
    from temporalio.api.workflow.v1 import PendingActivityInfo, WorkflowExecutionInfo
    from temporalio.api.workflowservice.v1 import DescribeWorkflowExecutionResponse

    raw = DescribeWorkflowExecutionResponse(
        workflow_execution_info=WorkflowExecutionInfo(history_length=length, status=1)
    )
    for state, attempt, started, failed, beat in pending:
        info = PendingActivityInfo(activity_id="1", state=state, attempt=attempt)  # pyright: ignore[reportArgumentType]
        if started is not None:
            info.last_started_time.CopyFrom(Timestamp(seconds=started))
        if failed:
            info.last_failure.CopyFrom(Failure(message="m"))
        info.heartbeat_details.CopyFrom(Payloads(payloads=[Payload(data=beat)]))
        info.last_heartbeat_time.CopyFrom(Timestamp(seconds=len(beat)))
        raw.pending_activities.append(info)
    return SimpleNamespace(raw_description=raw)


def test_the_change_key_holds_what_a_retry_changes_and_not_a_heartbeat() -> None:
    """A retry writes no history until the activity closes: the stream sends the graph
    again when a pending activity's state, attempt, start or last failure changed, though
    a poll may miss the backoff between two attempts (same state, a new attempt and start).
    A heartbeat is no change."""
    key = console_module._change_key
    started, scheduled = 2, 1  # PENDING_ACTIVITY_STATE_STARTED, _SCHEDULED
    base = key(_described((started, 2, 100, True, b"a")))
    for changed in (
        _described((started, 3, 102, True, b"a")),  # the next attempt, already started
        _described((started, 3, 100, True, b"a")),
        _described((started, 2, 102, True, b"a")),
        _described((scheduled, 2, 100, True, b"a")),
        _described((started, 2, 100, False, b"a")),
        _described((started, 2, None, True, b"a")),
        _described((started, 2, 100, True, b"a"), length=8),
        _described(),
    ):
        assert key(changed) != base
    assert key(_described((started, 2, 100, True, b"another heartbeat"))) == base


async def test_a_batch_child_s_history_is_kept_once_it_closed(tmp_path: Path) -> None:
    """An open child's history grows (it is what names its batch): read again each time,
    and remembered only once it closed."""
    child = ("parent/batch/item/1", "run-1")
    parent = {
        "events": [
            {
                "eventType": "EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED",
                "childWorkflowExecutionStartedEventAttributes": {
                    "workflowType": {"name": "kontra.batch.v1"},
                    "workflowExecution": {"workflowId": child[0], "runId": child[1]},
                },
            }
        ]
    }
    opened = {"events": [{"eventType": "EVENT_TYPE_WORKFLOW_EXECUTION_STARTED"}]}
    scheduled = {"events": [*opened["events"], {"eventType": "EVENT_TYPE_ACTIVITY_TASK_SCHEDULED"}]}
    closed = {
        "events": [*scheduled["events"], {"eventType": "EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED"}]
    }
    script = [opened, scheduled, closed]
    fetched: list[tuple[str, str | None]] = []

    class History:
        def __init__(self, value: dict[str, Any]) -> None:
            self.value = value

        def to_json_dict(self) -> dict[str, Any]:
            return copy.deepcopy(self.value)

    class Handle:
        def __init__(self, workflow_id: str, run_id: str | None) -> None:
            self.at = (workflow_id, run_id)

        async def fetch_history(self) -> History:
            fetched.append(self.at)
            return History(script.pop(0))

    class FakeClient:
        def get_workflow_handle(self, workflow_id: str, *, run_id: str | None = None) -> Handle:
            return Handle(workflow_id, run_id)

    console = Console(tmp_path, _codec)
    client: Any = FakeClient()
    try:
        seen = [await console._child_histories(client, parent) for _ in range(5)]
    finally:
        console.close()
    assert [h[child[0]] for h in seen] == [opened, scheduled, closed, closed, closed]
    assert fetched == [child] * 3
    assert list(console._children) == [child]


async def test_a_run_of_a_hired_turn_opened_by_its_own_id_is_never_answered(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """OD7 by Temporal's record of the run, not by the viewed run's type: a turn job's agent
    child (its root is the job) or a subagent of it (on a server that records no root, its
    parents lead to the job), opened by its own id, is read-only, and ``POST /api/approve`` is
    refused (403) with no Update sent. An agent that is its own root is answered."""
    from types import SimpleNamespace

    from temporalio.api.common.v1 import WorkflowExecution
    from temporalio.api.workflow.v1 import WorkflowExecutionInfo
    from temporalio.client import WorkflowExecutionStatus

    from kontra.contract import names

    job = names.new_job_id()
    root, child = names.job_workflow_id(job), names.agent_workflow_id(job)
    sub, alone = "cert-checker-subagent-1", "recon-entry-1"
    run = "5f0c6a7e-0000-4000-8000-000000000001"

    def described(
        wid: str, kind: str, *, root_of: str | None = None, parent: str | None = None
    ) -> Any:
        info = WorkflowExecutionInfo()
        if root_of is not None:
            info.root_execution.CopyFrom(WorkflowExecution(workflow_id=root_of, run_id=run))
        if parent is not None:
            info.parent_execution.CopyFrom(WorkflowExecution(workflow_id=parent, run_id=run))
        raw = SimpleNamespace(workflow_execution_info=info, pending_activities=[])
        return SimpleNamespace(
            id=wid, run_id=run, workflow_type=kind, task_queue="q",
            status=WorkflowExecutionStatus.RUNNING, raw_description=raw,
        )  # fmt: skip

    runs = {
        root: described(root, "kontra.turn.v1", root_of=root),
        child: described(child, "ReconHarnessAgent", root_of=root, parent=root),
        sub: described(sub, "CertChecker", parent=child),  # no root recorded: walk up
        alone: described(alone, "ReconHarnessAgent", root_of=alone),
    }
    sent: list[tuple[str, str]] = []

    class Handle:
        def __init__(self, wid: str) -> None:
            self.wid = wid

        async def describe(self) -> Any:
            return runs[self.wid]

        async def execute_update(self, name: str, arg: Any, **kwargs: Any) -> Any:
            sent.append((self.wid, name))
            return {"accepted": True}

    class FakeClient:
        def get_workflow_handle(self, wid: str, *, run_id: str | None = None) -> Handle:
            return Handle(wid)

    client: Any = FakeClient()
    items = [
        (0, {"event": {"type": "turn_started"}, "turn_id": "t", "turn_number": 1}),
        (
            1,
            {
                "event": {
                    "type": "tool_approval_requested",
                    "tool_id": "call_1",
                    "tool_name": "post",
                },
                "turn_id": "t",
            },
        ),
    ]

    async def given(*args: Any) -> Any:
        return items

    async def nothing(*args: Any, **kwargs: Any) -> Any:
        return ({}, {}) if "watched" in kwargs else {}

    console = Console(tmp_path, _codec)
    monkeypatch.setattr(console._reader, "items", given)
    for name in ("tool_links", "subagents", "input_types", "answered"):
        monkeypatch.setattr(console._reader, name, nothing)
    answered: dict[str, Any] = {}
    try:
        for target in (child, sub, alone):
            agent = await console._reader.agent_of(client, runs[target], {"events": []})
            session, _ = await console._session_of(client, runs[target], agent, {}, [])
            snap = console_module._Snapshot(
                {"workflow": {"workflow_id": target, "run_id": run}, "nodes": []},
                {}, ("k",), False, False, session=session,
            )  # fmt: skip

            async def view(*args: Any, snap: Any = snap) -> Any:
                return snap

            async def write() -> Any:
                return client

            monkeypatch.setattr(console, "_view", view)
            monkeypatch.setattr(console, "_write_client", write)
            request = console_module._request(
                "/api/approve", {"offset": 1, "tool_id": "call_1", "approved": True}
            )
            try:
                answered[target] = await console._answer(target, None, request)
            except console_module.ConsoleError as e:
                answered[target] = (e.status, e.message, session["agent"]["hired"])
    finally:
        console.close()
    for target in (child, sub):
        assert answered[target] == (
            403,
            "this run's requests are not the console's to answer (it is part of a hired turn)",
            True,
        )
    assert answered[alone] == {"accepted": True}
    assert sent == [(alone, "tool_approval")]  # nothing went into the hired job's tree


def test_a_cut_tool_id_is_never_pending_for_an_answer() -> None:
    row = {"offset": 1, "tool_id": "x" * 999 + "…", "label": "approval.requested", "pending": True}
    session = {"turns": [{"rows": [row]}]}
    assert console_module._pending(session, 1, row["tool_id"], "approval.requested")
    row["id_capped"] = True
    assert not console_module._pending(session, 1, row["tool_id"], "approval.requested")


async def test_a_session_the_renderer_cannot_take_is_an_empty_one_not_a_failed_graph(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """One item the renderer raises on must not take the run's graph (and its stream, and the
    POST routes) down with it: the pane says ``undecodable`` and the log says why."""

    build = console_module._session.build

    def boom(items: Any, **kwargs: Any) -> Any:
        if items:  # the empty session the pane falls back to still renders
            raise OverflowError("cannot convert float infinity to integer")
        return build(items, **kwargs)

    async def items(*args: Any) -> list[tuple[int, dict[str, Any]]]:
        return [(0, {"event": {"type": "turn_started"}, "turn_id": "t"})]

    async def nothing(*args: Any, **kwargs: Any) -> Any:
        return ({}, {}) if "watched" in kwargs else {}

    console = Console(tmp_path, _codec)
    monkeypatch.setattr(console._reader, "items", items)
    for name in ("tool_links", "subagents", "input_types"):
        monkeypatch.setattr(console._reader, name, nothing)
    monkeypatch.setattr(console_module._session, "build", boom)
    agent = console_module._agent.AgentRun("w", "r", "b", {"events": []})
    desc: Any = type("Desc", (), {"task_queue": "q"})()
    try:
        session, shown = await console._session_of(Any, desc, agent, {}, [])  # type: ignore[arg-type]
    finally:
        console.close()
    assert (session["empty"], session["reason"], shown) == (True, "undecodable", [])
    assert "the session of w: OverflowError" in capsys.readouterr().err


def test_a_quiet_stream_says_it_is_alive(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Nothing changed for :data:`_KEEPALIVE` seconds: a comment line, so that a proxy or
    the browser does not drop the stream; a frame resets the count."""
    monkeypatch.setattr(console_module, "_POLL", 0.01)
    monkeypatch.setattr(console_module, "_KEEPALIVE", 0.05)

    def snap(*, closed: bool) -> Any:
        g = {
            "workflow": {"run_id": "r", "status": "completed" if closed else "running"},
            "nodes": [],
            "edges": [],
        }
        return console_module._Snapshot(g, {}, ("k",), closed, False)

    quiet = 30  # polls with nothing new: about 0.3 s, six keepalives' worth
    script: list[Any] = [snap(closed=False), *[None] * quiet, snap(closed=True)]

    class Scripted(Console):
        async def _snapshot(self, *args: Any, **kwargs: Any) -> Any:
            return script.pop(0)

        async def _changed(self, workflow_id: str, run_id: str, last: Any) -> Any:
            return script.pop(0)

    console = Scripted(tmp_path, _codec)
    try:
        frames = list(console.stream("w", None))
    finally:
        console.close()
    beats = frames.count(b": keepalive\n\n")
    assert 2 <= beats < quiet, frames
    assert frames[-1].startswith(b"event: end")
    assert [f for f in frames if f.startswith(b": ")] == [b": keepalive\n\n"] * beats


async def test_the_page_reads_temporal_with_a_dev_login_alone(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Whoever reaches the codec server's port reads through it: a real ``kontra login`` in
    the same ``KONTRA_HOME`` (made after ``up --ui`` started it, which checked the profile
    then) is refused before any token is asked for, and its session closed."""
    from datetime import UTC, datetime

    from kontra.auth import Session
    from kontra.cli._console import ConsoleError, connect_dev
    from kontra.profile import Profile

    asked: list[str] = []
    closed: list[str] = []
    real_close = Session.aclose
    client = object()

    async def connect_temporal(self: Session, role: str = "worker", **_: Any) -> Any:
        asked.append(role)
        return client

    async def aclose(self: Session) -> None:
        closed.append(self.profile.namespace)
        await real_close(self)

    monkeypatch.setattr(Session, "connect_temporal", connect_temporal)
    monkeypatch.setattr(Session, "aclose", aclose)
    for dev, home in ((False, tmp_path / "real"), (True, tmp_path / "dev")):
        Profile(
            gate_url="https://gate.example" if not dev else "http://127.0.0.1:1",
            temporal_address="temporal.example:7233",
            party="alice",
            namespace="alice" if not dev else "devparty",
            refresh_token="secret-refresh",
            auth_time=datetime.now(UTC),
            dev=dev,
        ).save(home)
    with pytest.raises(ConsoleError) as refused:
        await connect_dev(tmp_path / "real")
    assert refused.value.status == 403
    assert "not a dev one" in refused.value.message
    assert (asked, closed) == ([], ["alice"])

    connected = await connect_dev(tmp_path / "dev")
    assert (connected.client, connected.namespace, asked) == (client, "devparty", ["worker"])
    assert connected.close is not None
    await connected.close()
    assert closed == ["alice", "devparty"]


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
    # A signal for each graph the page would be shown differently, none for the same one.
    assert [(name, data.get("rev"), data.get("status")) for name, data in events] == [
        ("session-stat", 0, "running"),
        ("session-stat", 1, "running"),
        ("session-stat", 2, "completed"),
        ("end", None, None),
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
    first = asyncio.create_task(asyncio.to_thread(_stream, srv, f"/api/events?w={wf}", opened))
    assert await asyncio.to_thread(opened.wait, 20)
    r = await asyncio.to_thread(_get, srv, f"/api/events?w={wf}")
    assert r.status_code == 503
    assert r.json() == {"error": "16 live graphs are open already"}
    assert r.headers["Cache-Control"] == "no-store"
    await handle.terminate()
    events = _events(await first)
    assert events[-1][0] == "end"
    assert events[-2][1]["status"] == "terminated"
    # The slot is free again (the run is closed: its signal, then the end).
    again = await asyncio.to_thread(_get, srv, f"/api/events?w={wf}")
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
        (f"/api/events?w=nothing-{uuid.uuid4().hex}", 404),
    ):
        r = _get(srv, path)
        assert r.status_code == status, (path, r.text)
        assert r.headers["Content-Type"] == "application/json"
        assert set(r.json()) == {"error"}
        assert "\n" not in r.json()["error"]
        assert "Traceback" not in r.text
    for gated in (f"/api/io?w=nothing-{uuid.uuid4().hex}&node=", "/api/session?w=x"):
        r = _get(srv, gated)
        assert (r.status_code, r.json()) == (403, {"error": "X-Kontra-Console: 1 is required"})
    headed = {"X-Kontra-Console": "1"}
    for path, status in (
        ("/api/session", 400),
        (f"/api/session?w=nothing-{uuid.uuid4().hex}", 404),
        ("/api/io?w=x&event=", 400),
        ("/api/io?w=x&event=-1", 400),
        ("/api/io?w=x&event=01", 400),
        ("/api/io?w=x&event=1e3", 400),
        (f"/api/io?w=nothing-{uuid.uuid4().hex}&event=0", 404),
    ):
        r = _get(srv, path, **headed)
        assert r.status_code == status, (path, r.text)
        assert set(r.json()) == {"error"}
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
