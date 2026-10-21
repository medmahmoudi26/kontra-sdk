"""The agent session pane end to end (decisions I36), on a real Temporal dev server under
``kontra.testing.Marketplace`` (the dev gate, ``kontra.devgate.DevGate``): ``ReconAgent``, the
agent of ``examples/recon-harness`` (its code copied beside an offering.yaml of this test's),
hired as a bound-handler job through ``kontra.turn.v1`` (case ``a``), and run as an agent
workflow of its own that hosts its stream (case ``b``), both read back by the dev codec
server's console the way ``kontra dev up --ui`` wires it (``tests/cli/session_console.py``),
over HTTP.

Asserted: ``/api/session``'s graph -- the agent child drawn nested, its tool activities real
nodes -- and its rows; each tool row's link (the node, the activity's scheduled event id) and
the handoff's (the subagent's child node, its own rows nested); ``session-stat`` on
``/api/events``; a nested node's payloads and an event's verbatim body on ``/api/io`` alone;
and seam ⑤: the stream and the session carry no ciphertext, and the stream not even the
plaintext a body holds.

Memory is tight: one Temporal server for the module (the shared session fixture), the seller's
worker in process, and the console's threads."""

from __future__ import annotations

import asyncio
import base64
import json
import os
import re
import shutil
import threading
import uuid
from collections.abc import Callable, Iterator
from datetime import timedelta
from pathlib import Path
from typing import Any

import httpx
import pytest
from temporalio.client import Client
from temporalio.contrib.workflow_streams import WorkflowStreamClient

from kontra import graph
from kontra.cli._codec_server import CodecServer
from kontra.cli._console import Connected, Console
from kontra.cli._project import load_worker
from kontra.codec import KontraCodec
from kontra.contract.names import agent_workflow_id, job_workflow_id
from kontra.serve import _agent_events as ae
from kontra.serve.worker import seller_data_converter
from kontra.testing import Marketplace, TestParty
from kontra.testing.devserver import DevServer
from tests.cli.session_console import PAGE, consoles
from tests.cli.session_console import codec_for as _codec
from tests.cli.session_console import get as _get
from tests.cli.session_console import row as _row
from tests.cli.session_console import rows as _rows

pytestmark = pytest.mark.temporal

pytest.importorskip("temporal_agent_harness", reason="the dev group's temporal-agent-harness")

#: Planted in the domains the agent sweeps: the bodies hold it, the stream must not.
MARKER = "kontra-session-marker-5c1e"
#: The agent the pane is shown with: the example's own code, not a copy of it.
EXAMPLE = Path(__file__).resolve().parents[2] / "examples" / "recon-harness"


def _project(root: Path) -> Path:
    """examples/recon-harness's code (worker.py, quote.py and the agent's modules), under an
    offering of this test's own: a version no other run of the shared dev server serves."""
    shutil.copytree(
        EXAMPLE, root, ignore=shutil.ignore_patterns("*.yaml", "*.md", "*.json", "__pycache__")
    )
    spec = {
        "name": "acme/recon",
        "version": f"1.{os.getpid() % 100_000}.{uuid.uuid4().int % 10_000}",
        "description": "Sweeps domains, one turn of an agent",
        "max_duration": "10m",
        "review_window": "1h",
        "max_output_bytes": "1MB",
        "artifact_retention": "1d",
        "streams": ["turn_events"],
    }
    (root / "offering.yaml").write_text("".join(f"{k}: {json.dumps(v)}\n" for k, v in spec.items()))
    return root


@pytest.fixture
def console_for() -> Iterator[Callable[[Marketplace, TestParty], CodecServer]]:
    with consoles() as opened:
        yield opened.open


def _stream(srv: CodecServer, path: str) -> bytes:
    out = b""
    with (
        httpx.Client(trust_env=False, timeout=120) as http,
        http.stream("GET", f"http://127.0.0.1:{srv.port}{path}") as r,
    ):
        assert r.status_code == 200, r.read()
        for chunk in r.iter_bytes():
            out += chunk
    return out


def _frames(raw: bytes) -> list[tuple[str, Any]]:
    out: list[tuple[str, Any]] = []
    for block in raw.decode().split("\n\n"):
        fields = dict(
            line.split(": ", 1) for line in block.splitlines() if line and not line.startswith(":")
        )
        if "event" in fields:
            out.append((fields["event"], json.loads(fields["data"])))
    return out


def _node(g: dict[str, Any], node_id: str) -> dict[str, Any]:
    [found] = [n for n in g["nodes"] if n["id"] == node_id]
    return found


def _sealed(histories: list[dict[str, Any]]) -> set[str]:
    """Each payload of these histories as stored (base64, URL-safe and hex), and a
    32-character start of each: none may reach a route but ``/api/io``."""

    def data(value: object) -> Iterator[str]:
        if isinstance(value, dict):
            if isinstance(value.get("metadata"), dict) and isinstance(value.get("data"), str):
                yield value["data"]
            for v in value.values():
                yield from data(v)
        elif isinstance(value, list):
            for v in value:
                yield from data(v)

    forms: set[str] = set()
    for item in [d for h in histories for d in data(h)]:
        raw = base64.b64decode(item)
        assert MARKER.encode() not in raw  # the codec ran: history holds no plaintext
        for text in (item, base64.urlsafe_b64encode(raw).decode(), raw.hex()):
            if len(text) >= 24:
                forms |= {text, text[:32]}
    return forms


def _clean(where: str, body: str, sealed: set[str], *, marker: bool = True) -> None:
    if marker:
        assert MARKER not in body, where
    for form in sealed:
        assert form not in body, (where, form[:16])


async def _histories(client: Client, *workflow_ids: str) -> list[dict[str, Any]]:
    return [
        (await client.get_workflow_handle(w).fetch_history()).to_json_dict() for w in workflow_ids
    ]


# --------------------------------------------------------------------------- case (a)


async def test_a_hired_turn_is_read_from_its_agent_child_drawn_nested(
    dev_server: DevServer,
    tmp_path: Path,
    console_for: Callable[[Marketplace, TestParty], CodecServer],
) -> None:
    domains = [f"{MARKER}-{i}.example" for i in range(4)]
    with load_worker(_project(tmp_path / "p")) as worker:
        async with Marketplace(dev_server=dev_server) as mp:
            acme = await mp.party("acme")
            bob = await mp.party(f"bob-{uuid.uuid4().hex[:6]}", balance="100.00")
            async with mp.serve(worker, acme):
                k = bob.kontra(client=mp.client(bob.namespace))
                try:
                    quote = await k.quote("acme/recon", domains=domains)
                    job = await quote.hire(max_price="1.00")
                    result = await job.result(timeout=120)
                finally:
                    await k.aclose()
                assert result.to_dict()["targets"] == 4
                srv = console_for(mp, acme)
                root = job_workflow_id(job.job_id)
                got = await asyncio.to_thread(_get, srv, f"/api/session?w={root}", **PAGE)
                assert got.status_code == 200, got.text
                view = got.json()
                frames = _frames(await asyncio.to_thread(_stream, srv, f"/api/events?w={root}"))
                plan = _row(view["session"], "tool.call", "plan")
                refused = await asyncio.to_thread(
                    _post, srv, "/api/approve",
                    {"w": root, "offset": plan["offset"], "tool_id": plan["tool_id"], "approved": True},
                    **{"X-Kontra-Session": _token(srv)},
                )  # fmt: skip
                g, session = view["graph"], view["session"]
                seller = mp.client(acme.namespace)
                agent_wf = agent_workflow_id(job.job_id)
                [agent_history] = await _histories(seller, agent_wf)

    # The graph: the job's wrapper, its agent child drawn nested with its tools inside it.
    host = next(
        n for n in g["nodes"] if n["kind"] == "child" and n["ref"]["workflow_id"] == agent_wf
    )
    assert host["id"] == "ReconAgent"
    inside = {n["name"]: n for n in g["nodes"] if n.get("parent") == host["id"]}
    assert {"plan", "probe", "CertChecker", "run_subagent_turn"} <= set(inside)
    assert all(n["within"]["workflow_id"] == agent_wf for n in inside.values())
    assert all(n["status"] == "completed" for n in inside.values())
    assert {e["parent"] for e in g["edges"] if "parent" in e} == {host["id"]}
    assert "kontra.turn.v1.deliver" in {n["name"] for n in g["nodes"] if "parent" not in n}

    # The session: case a, read-only, its rows in the wireframe's vocabulary.
    agent = session["agent"]
    assert (agent["case"], agent["interactive"], agent["workflow_id"]) == ("a", False, agent_wf)
    assert (agent["name"], agent["status"], agent["offering"]) == (
        "ReconAgent.sweep",
        "completed",
        g["workflow"]["task_queue"],
    )
    labels = [r["label"] for r in _rows(session)]
    assert labels[0] == "turn.started"
    assert {
        "model.call",
        "model.response",
        "tool.call",
        "tool.result",
        "handoff",
        "response",
    } <= set(labels)
    [turn] = session["turns"]
    assert (turn["handler"], turn["input_summary"], turn["status"]) == (
        "sweep",
        "4 domains",
        "completed",
    )
    # sweep(SweepRequest): the input's type, as the agent's own agent_interface query says it.
    assert turn["input_type"] == "SweepRequest"
    assert session["stream"] == "turn_events"
    assert _row(session, "turn.started")["summary"] == "input: 4 domains"
    assert _row(session, "model.response")["summary"] == "“plan first”"

    # Linking (OD4): exact, by the tool_id in each tool activity's decrypted input.
    for tool in ("plan", "probe"):
        call, result = _row(session, "tool.call", tool), _row(session, "tool.result", tool)
        node = inside[tool]
        assert call["node"] == result["node"] == node["id"] == graph.nested_id(host["id"], tool)
        assert call["event_id"] == node["ref"]["event_id"]
        assert "node_fallback" not in call
    scheduled = {
        int(e["eventId"]): e["activityTaskScheduledEventAttributes"]["activityType"]["name"]
        for e in agent_history["events"]
        if "activityTaskScheduledEventAttributes" in e
    }
    assert scheduled[_row(session, "tool.call", "plan")["event_id"]] == "plan"

    # The handoff links its child node and nests the subagent's own rows (OD5).
    handoff = _row(session, "handoff")
    assert handoff["node"] == inside["CertChecker"]["id"]
    assert handoff["workflow_id"] == inside["CertChecker"]["ref"]["workflow_id"]
    assert handoff["event_id"] == inside["CertChecker"]["ref"]["event_id"]  # what started it
    assert {c["node"] for c in handoff["children"]} == {handoff["node"]}  # they link there too
    nested = [(c["label"], c["summary"]) for c in handoff["children"]]
    assert ("tool.call", "fetch_cert \N{MULTIPLICATION SIGN}4") in nested
    assert nested[-1][0] == "response"
    assert "link" not in handoff

    # The footer, and the stream: a change signal per change, never a row (OD2).
    footer = session["footer"]
    offsets = [r["offset"] for r in _rows(session)]
    assert offsets == sorted(set(offsets))  # offset order, each once
    assert footer["count"] > len(offsets)  # hidden events are counted, not shown
    assert footer["last_offset"] >= offsets[-1]
    assert [name for name, _ in frames] == ["session-stat", "end"]
    stat = frames[0][1]
    assert (stat["count"], stat["last_offset"], stat["status"], stat["turn"]) == (
        footer["count"],
        footer["last_offset"],
        "completed",
        1,
    )
    assert stat["history_length"] > len(agent_history["events"])
    # A hired turn's requests are not the console's to answer (OD7): refused, nothing sent.
    assert refused.status_code == 403
    assert "it is part of a hired turn" in refused.json()["error"]


async def test_bodies_reach_the_page_through_io_alone(
    dev_server: DevServer,
    tmp_path: Path,
    console_for: Callable[[Marketplace, TestParty], CodecServer],
) -> None:
    """Seam ⑤ for the session: the stream and ``/api/session`` carry no payload as stored
    (the job's, the agent child's, the subagent's), and the stream not even a body's
    plaintext; ``/api/io`` opens a nested node's payloads from the agent child's history and
    one event's verbatim body, for the page's own requests alone."""
    domains = [f"{MARKER}-{i}.example" for i in range(3)]
    with load_worker(_project(tmp_path / "p")) as worker:
        async with Marketplace(dev_server=dev_server) as mp:
            acme = await mp.party("acme")
            bob = await mp.party(f"bob-{uuid.uuid4().hex[:6]}", balance="100.00")
            async with mp.serve(worker, acme):
                k = bob.kontra(client=mp.client(bob.namespace))
                try:
                    job = await (await k.quote("acme/recon", domains=domains)).hire(
                        max_price="1.00"
                    )
                    await job.result(timeout=120)
                finally:
                    await k.aclose()
                srv = console_for(mp, acme)
                root = job_workflow_id(job.job_id)
                session = await asyncio.to_thread(_get, srv, f"/api/session?w={root}", **PAGE)
                stream = await asyncio.to_thread(_stream, srv, f"/api/events?w={root}")
                view = session.json()
                plan = _row(view["session"], "tool.call", "plan")
                node_io = await asyncio.to_thread(
                    _get, srv, f"/api/io?w={root}&node={plan['node']}", **PAGE
                )
                body = await asyncio.to_thread(_get, srv, f"/api/io?w={root}&event=0", **PAGE)
                refused = await asyncio.to_thread(_get, srv, f"/api/io?w={root}&event=0")
                no_header = await asyncio.to_thread(_get, srv, f"/api/session?w={root}")
                seller = mp.client(acme.namespace)
                handoff = _row(view["session"], "handoff")
                sealed = _sealed(
                    await _histories(
                        seller, root, agent_workflow_id(job.job_id), handoff["workflow_id"]
                    )
                )
    assert len(sealed) > 50
    _clean("/api/events", stream.decode(), sealed)
    _clean("/api/session", session.text, sealed)
    # The control: io opens what it is asked for, decoded with this machine's key.
    assert node_io.status_code == 200, node_io.text
    assert node_io.json()["input"][0] == domains  # plan's own input, from the child's history
    assert node_io.json()["result"] == {"hosts": [f"www.{d}" for d in domains]}
    assert body.status_code == 200, body.text
    accepted = body.json()["body"]
    assert accepted["type"] == ae.MESSAGE_ACCEPTED
    assert accepted["payload"]["domains"] == domains
    with pytest.raises(AssertionError):  # the scan finds the marker where it is
        _clean("the control", body.text, sealed)
    assert (refused.status_code, no_header.status_code) == (403, 403)


# --------------------------------------------------------------------------- case (b)


async def test_an_agent_that_hosts_its_stream_is_read_once_its_stream_is_polled(
    dev_server: DevServer,
    tmp_path: Path,
    console_for: Callable[[Marketplace, TestParty], CodecServer],
) -> None:
    """ReconAgent run as a workflow of its own (case b): its events are in its
    in-memory log until something polls them, so the pane says so (OD1); once its seller's
    client has read the stream, the rows are there, linked to the run's own tool nodes, and
    the approval it waits on is pending."""
    with load_worker(_project(tmp_path / "p")) as worker:
        [bound] = worker.offerings
        async with Marketplace(dev_server=dev_server) as mp:
            acme = await mp.party("acme")
            async with mp.serve(worker, acme):
                config = mp.client(acme.namespace).config()
                config["data_converter"] = seller_data_converter(_codec(acme)())
                seller = Client(**config)
                agent_wf = f"recon-{uuid.uuid4().hex[:8]}"
                handle = await seller.start_workflow(
                    "ReconAgent",
                    {},
                    id=agent_wf,
                    task_queue=bound.key,
                    execution_timeout=timedelta(minutes=5),
                )
                try:
                    await handle.execute_update(
                        ae.SEND_AGENT_MESSAGE_UPDATE,
                        {
                            "type": "sweep",
                            "payload": {"domains": ["a.example"], "want_approval": True},
                        },
                        result_type=dict,
                    )
                    srv = console_for(mp, acme)
                    before = await asyncio.to_thread(
                        _get, srv, f"/api/session?w={agent_wf}", **PAGE
                    )

                    async def until_approval() -> None:
                        stream = WorkflowStreamClient.create(seller, agent_wf)
                        async for item in stream.subscribe(
                            topics=[ae.TURN_EVENTS_TOPIC],
                            from_offset=0,
                            result_type=dict,
                            poll_cooldown=timedelta(milliseconds=50),
                        ):
                            if item.data[ae.EVENT][ae.EVENT_TYPE] == ae.TOOL_APPROVAL_REQUESTED:
                                return

                    await asyncio.wait_for(until_approval(), 60)
                    after = await asyncio.to_thread(_get, srv, f"/api/session?w={agent_wf}", **PAGE)
                finally:
                    await handle.terminate("the test is done")

    empty = before.json()["session"]
    assert (empty["empty"], empty["reason"], empty["agent"]["case"]) == (
        True,
        "not-in-history",
        "b",
    )
    view = after.json()
    session, g = view["session"], view["graph"]
    assert session["agent"]["status"] == "running"
    assert session["agent"]["interactive"] is True  # a running agent hosting its stream (OD7)
    for tool in ("plan", "probe"):
        call = _row(session, "tool.call", tool)
        assert call["node"] == tool  # the run's own activity node: nothing nested
        assert call["event_id"] == _node(g, tool)["ref"]["event_id"]
    assert _row(session, "handoff")["node"] == "CertChecker"
    approval = _row(session, "approval.requested")
    assert (approval["tool_name"], approval["pending"], approval["resolution"]) == (
        "post_findings",
        True,
        None,
    )
    assert approval["hint"].startswith("to: slack #sec")
    assert all("parent" not in n for n in g["nodes"])


# --------------------------------------------------------------------------- answers (dev only)


def _token(srv: CodecServer) -> str:
    """This console's token, as its page carries it (the one place it is)."""
    page = _get(srv, "/graph").text
    found = re.search(r'const SESSION_TOKEN = "([^"]+)"', page)
    assert found is not None
    return found.group(1)


def _post(srv: CodecServer, path: str, body: dict[str, Any], **headers: str) -> httpx.Response:
    origin = f"http://127.0.0.1:{srv.port}"
    sent = {
        "Content-Type": "application/json",
        "Origin": origin,
        "Sec-Fetch-Site": "same-origin",
        "X-Kontra-Console": "1",
        **headers,
    }
    with httpx.Client(trust_env=False, timeout=60) as http:
        return http.post(
            f"http://127.0.0.1:{srv.port}{path}", content=json.dumps(body), headers=sent
        )


def _updates(history: dict[str, Any], name: str) -> int:
    return sum(
        1
        for e in history["events"]
        if (e.get("workflowExecutionUpdateAcceptedEventAttributes") or {})
        .get("acceptedRequest", {})
        .get("input", {})
        .get("name")
        == name
    )


async def _until(check: Callable[[], Any], what: str, within: float = 60.0) -> Any:
    for _ in range(int(within * 5)):
        if got := await check():
            return got
        await asyncio.sleep(0.2)
    raise AssertionError(f"not within {within:.0f} s: {what}")


async def test_the_console_pulls_once_then_answers_an_approval_and_a_callback(
    dev_server: DevServer,
    tmp_path: Path,
    console_for: Callable[[Marketplace, TestParty], CodecServer],
) -> None:
    """Case b, dev only: a run nobody polled offers ``[Pull once]`` (OD1), one poll from seq 0;
    then the approval it waits on is answered from the console -- the harness's
    ``tool_approval`` Update to that run, once, refused for a request that is not pending --
    and so is the callback after it (``provide_callback_result``); the turn then completes."""
    with load_worker(_project(tmp_path / "p")) as worker:
        [bound] = worker.offerings
        async with Marketplace(dev_server=dev_server) as mp:
            acme = await mp.party("acme")
            async with mp.serve(worker, acme):
                config = mp.client(acme.namespace).config()
                config["data_converter"] = seller_data_converter(_codec(acme)())
                seller = Client(**config)
                agent_wf = f"recon-{uuid.uuid4().hex[:8]}"
                handle = await seller.start_workflow(
                    "ReconAgent", {}, id=agent_wf, task_queue=bound.key,
                    execution_timeout=timedelta(minutes=5),
                )  # fmt: skip
                try:
                    payload = {
                        "domains": ["a.example", "b.example"],
                        "want_approval": True,
                        "want_callback": True,
                    }
                    await handle.execute_update(
                        ae.SEND_AGENT_MESSAGE_UPDATE,
                        {"type": "sweep", "payload": payload},
                        result_type=dict,
                    )

                    async def waiting(on: str) -> bool:
                        status = await handle.query("agent_status", result_type=dict)
                        return bool(status.get(on))

                    await _until(lambda: waiting("pending_approvals"), "the approval pending")
                    srv = console_for(mp, acme)
                    token = _token(srv)
                    w = {"w": agent_wf}

                    def session() -> dict[str, Any]:
                        got = _get(srv, f"/api/session?w={agent_wf}", **PAGE)
                        assert got.status_code == 200, got.text
                        return got.json()["session"]

                    before = await asyncio.to_thread(session)
                    assert (before["reason"], before["can_pull"]) == ("not-in-history", True)
                    pulled = await asyncio.to_thread(
                        _post, srv, "/api/pull", w, **{"X-Kontra-Session": token}
                    )
                    assert pulled.status_code == 200, pulled.text
                    view = await asyncio.to_thread(session)
                    assert view["empty"] is False
                    assert view["agent"]["interactive"] is True
                    again = await asyncio.to_thread(
                        _post, srv, "/api/pull", w, **{"X-Kontra-Session": token}
                    )
                    assert again.status_code == 409  # its events are in history now: once is all
                    approval = _row(view, "approval.requested")
                    assert approval["pending"] is True
                    ask = {
                        **w,
                        "offset": approval["offset"],
                        "tool_id": approval["tool_id"],
                        "approved": True,
                    }
                    [history] = await _histories(seller, agent_wf)
                    assert _updates(history, ae.TOOL_APPROVAL_UPDATE) == 0
                    # Refused, and nothing sent: another tool_id, another offset, a callback's
                    # answer to an approval, another console's token.
                    for wrong, path, extra in (
                        ("another tool_id", "/api/approve", {"tool_id": "call-nope-0"}),
                        ("another offset", "/api/approve", {"offset": approval["offset"] + 1}),
                        ("the wrong kind", "/api/callback", {"result": "yes"}),
                    ):
                        r = await asyncio.to_thread(
                            _post, srv, path, {**ask, **extra}, **{"X-Kontra-Session": token}
                        )
                        assert r.status_code == 409, (wrong, r.text)
                    r = await asyncio.to_thread(
                        _post, srv, "/api/approve", ask, **{"X-Kontra-Session": "x" + token}
                    )
                    assert r.status_code == 403
                    [history] = await _histories(seller, agent_wf)
                    assert _updates(history, ae.TOOL_APPROVAL_UPDATE) == 0
                    # Allowed: one Update; the row is answered from history at once (the
                    # stream's own resolution is not read yet), so a second press sends none.
                    ok = await asyncio.to_thread(
                        _post, srv, "/api/approve", ask, **{"X-Kontra-Session": token}
                    )
                    assert (ok.status_code, ok.json()) == (200, {"accepted": True})
                    answered = _row(await asyncio.to_thread(session), "approval.requested")
                    assert (answered["pending"], answered["resolution"]) == (False, "allowed")
                    twice = await asyncio.to_thread(
                        _post, srv, "/api/approve", ask, **{"X-Kontra-Session": token}
                    )
                    assert twice.status_code == 409
                    [history] = await _histories(seller, agent_wf)
                    assert _updates(history, ae.TOOL_APPROVAL_UPDATE) == 1

                    # The callback, once its request is in history (a seller client reading the
                    # stream, as its own UI would).
                    await _until(lambda: waiting("pending_callbacks"), "the callback pending")

                    async def read_stream() -> None:
                        stream = WorkflowStreamClient.create(seller, agent_wf)
                        async for item in stream.subscribe(
                            topics=[ae.TURN_EVENTS_TOPIC], from_offset=0, result_type=dict,
                            poll_cooldown=timedelta(milliseconds=50),
                        ):  # fmt: skip
                            if item.data[ae.EVENT][ae.EVENT_TYPE] == ae.TURN_END:
                                return

                    reader = asyncio.create_task(read_stream())

                    async def callback_row() -> dict[str, Any] | None:
                        rows = _rows(await asyncio.to_thread(session))
                        return next((r for r in rows if r["label"] == "callback.requested"), None)

                    callback = await _until(callback_row, "the callback row")
                    assert (callback["tool_name"], callback["pending"]) == ("confirm", True)
                    # The page sends the value as the type the callback declares (a str here);
                    # a value of another type is the harness's to refuse, in its own words.
                    assert callback["result_type"] == "string"
                    wrong = await asyncio.to_thread(
                        _post, srv, "/api/callback",
                        {**w, "offset": callback["offset"], "tool_id": callback["tool_id"], "result": {"ok": True}},
                        **{"X-Kontra-Session": token},
                    )  # fmt: skip
                    assert wrong.status_code == 409, wrong.text
                    said = wrong.json()["error"]
                    assert said.startswith("the agent refused it: ")
                    assert "has no more" not in said  # the harness's own words, not a kind
                    sent = await asyncio.to_thread(
                        _post, srv, "/api/callback",
                        {**w, "offset": callback["offset"], "tool_id": callback["tool_id"], "result": "ship it"},
                        **{"X-Kontra-Session": token},
                    )  # fmt: skip
                    assert (sent.status_code, sent.json()) == (200, {"accepted": True})
                    await asyncio.wait_for(reader, 60)
                    final = await asyncio.to_thread(session)
                    assert (
                        _row(final, "callback.requested")["resolution"],
                        final["turns"][0]["status"],
                    ) == (
                        "ok",
                        "completed",
                    )
                    response = _row(final, "response")
                    body = await asyncio.to_thread(
                        _get, srv, f"/api/io?w={agent_wf}&event={response['offset']}", **PAGE
                    )
                    assert body.json()["body"]["output"]["confirmed"] == "ship it"
                    assert body.json()["body"]["output"]["posted"] == "posted to slack #sec"
                finally:
                    await handle.terminate("the test is done")


def test_a_non_dev_profile_s_console_answers_nothing(tmp_path: Path) -> None:
    """``connect_dev`` refuses a login that is not a dev one (FORBIDDEN): the whole console
    answers 403, every POST included, before any Update. A connect that raises so stands in
    for that profile (connect_dev's own refusal is ``tests/cli/test_console.py``'s)."""
    from http import HTTPStatus

    from kontra.cli._console import ConsoleError
    from kontra.codec.age import generate_identity

    async def refuse() -> Connected:
        raise ConsoleError(HTTPStatus.FORBIDDEN, "a login that is not a dev one: no console")

    secret, public = generate_identity()

    def codec() -> KontraCodec:
        return KontraCodec(secret, [public], role="seller")

    console = Console(tmp_path, codec, connect=refuse)
    srv = CodecServer(0, codec=codec, origins=["http://localhost:8233"], token="t", console=console)
    thread = threading.Thread(target=srv.serve_forever, daemon=True)
    thread.start()
    try:
        token = _token(srv)
        body = {"w": "recon-x", "offset": 0, "tool_id": "call-post_findings-0", "approved": True}
        for path in ("/api/approve", "/api/callback", "/api/pull"):
            r = _post(srv, path, {**body, "result": "x"}, **{"X-Kontra-Session": token})
            assert r.status_code == 403, (path, r.text)
        assert _get(srv, "/api/session?w=recon-x", **PAGE).status_code == 403
    finally:
        console.close()
        srv.shutdown()
        srv.server_close()
        thread.join(5)
