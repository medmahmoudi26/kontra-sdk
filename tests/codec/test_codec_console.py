"""The marker-string scan, extended to the graph page of ``kontra dev up --ui`` (seam ⑤,
decisions I28, I36): the page, ``/api/graph``, ``/api/workflows``, ``/api/session`` and the
live stream (``/api/events``, change signals alone) carry no input, result, failure message or
heartbeat -- not the marker, in any case, nor any payload's ciphertext or its base64 -- and
``/api/io`` is the one route that answers payload content, decoded with this machine's key,
only to the page's own requests. (An agent's session, whose rows carry short summaries, is
scanned in ``tests/cli/test_session_lifecycle.py``.)

A seller's worker (Kontra's payload codec, this machine's key, Kontra's sandbox and the batch
child ``serve()`` registers) runs on a real dev server a workflow whose input carries the
marker: an activity heartbeats it, fails with it in the message, and returns it upper-cased,
then a batch runs three items carrying it; another workflow fails with it. The codec server
serves the console on a thread, reading that server as the page does, while the run goes and
after. The control: ``/api/io`` with ``X-Kontra-Console: 1`` and the right ``Host`` answers the
marker decoded; a wrong ``Host``, a foreign ``Origin``, a cross-site fetch, a missing header
or a preflight is refused with none of it. What is encrypted to another key is shown as
``{"encrypted": true}``, a claim-check reference as ``{"claim_check": true}``.
"""

from __future__ import annotations

import asyncio
import base64
import json
import os
import threading
import time
import uuid
from collections.abc import Iterator, Sequence
from pathlib import Path
from typing import Any

import httpx
import pytest
from temporalio.api.common.v1 import Payload
from temporalio.client import Client, WorkflowFailureError
from temporalio.converter import DataConverter, PayloadCodec
from temporalio.worker import Worker

from kontra.cli._codec_server import CodecServer
from kontra.cli._console import Connected, Console
from kontra.codec import CLAIM_CHECK_ENCODING, ENCODING, KontraCodec
from kontra.codec.age import generate_identity
from kontra.contrib.parallel import ParallelPlugin
from kontra.serve.worker import seller_data_converter, workflow_runner
from kontra.testing.devserver import (
    CLI_ENV,
    DevServer,
    acquire_shared_dev_server,
    release_shared_dev_server,
)

from .console_flows import IdleFlow, LeakFlow, RefuseFlow, heartbeat_then_fail, item

pytestmark = pytest.mark.temporal

MARKER = "kontra-console-marker-7e4d"
SECRET, PUBLIC = generate_identity()
OTHER_SECRET, OTHER_PUBLIC = generate_identity()  # another party's
UI = "http://localhost:8233"
TOKEN = "stop-token-for-tests"


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


@pytest.fixture
def server(dev_server: DevServer, tmp_path: Path) -> Iterator[CodecServer]:
    async def connect() -> Connected:
        return Connected(await dev_server.connect("default"), "default")

    console = Console(
        tmp_path,
        _codec,
        temporal_ui=UI,
        grafana="http://localhost:3000",
        connect=connect,
    )
    srv = CodecServer(0, codec=_codec, origins=[UI], token=TOKEN, console=console)
    thread = threading.Thread(target=srv.serve_forever, daemon=True)
    thread.start()
    try:
        yield srv
    finally:
        console.close()
        srv.shutdown()
        srv.server_close()
        thread.join(5)


def _get(srv: CodecServer, path: str, **headers: str) -> httpx.Response:
    with httpx.Client(trust_env=False, timeout=60) as http:
        return http.get(f"http://127.0.0.1:{srv.port}{path}", headers=headers)


def _preflight(srv: CodecServer, path: str, origin: str) -> httpx.Response:
    with httpx.Client(trust_env=False, timeout=60) as http:
        return http.options(
            f"http://127.0.0.1:{srv.port}{path}",
            headers={
                "Origin": origin,
                "Access-Control-Request-Method": "GET",
                "Access-Control-Request-Headers": "x-kontra-console",
            },
        )


def _stream(srv: CodecServer, path: str) -> bytes:
    """The whole event stream: it ends after the closed run's graph."""
    out = b""
    with (
        httpx.Client(trust_env=False, timeout=120) as http,
        http.stream("GET", f"http://127.0.0.1:{srv.port}{path}") as r,
    ):
        assert r.status_code == 200, r.read()
        assert r.headers["Content-Type"] == "text/event-stream"
        for chunk in r.iter_bytes():
            out += chunk
    return out


def _payload_data(value: object) -> Iterator[str]:
    """The base64 data of every payload in proto3 JSON (a history, a describe answer)."""
    if isinstance(value, dict):
        if isinstance(value.get("metadata"), dict) and isinstance(value.get("data"), str):
            yield value["data"]
        for v in value.values():
            yield from _payload_data(v)
    elif isinstance(value, list):
        for v in value:
            yield from _payload_data(v)


def _sealed_forms(histories: Sequence[dict[str, Any]], extra: Sequence[str]) -> set[str]:
    """Each encrypted payload's base64, as history holds it and URL-safe, and a 32-character
    start of each (a page that showed a payload cut short)."""
    forms: set[str] = set()
    for data in [d for h in histories for d in _payload_data(h)] + list(extra):
        raw = base64.b64decode(data)
        assert MARKER.encode() not in raw  # history holds no plaintext: the codec ran
        for text in (data, base64.urlsafe_b64encode(raw).decode(), raw.hex()):
            forms |= {text, text[:32]}
    return forms


def _assert_clean(where: str, body: bytes | str, sealed: set[str]) -> None:
    text = body.decode() if isinstance(body, bytes) else body
    assert MARKER.lower() not in text.lower(), where
    for form in sealed:
        assert form not in text, (where, form[:16])


class _Claimed(PayloadCodec):
    """A client that leaves a claim-check reference in history in place of each payload."""

    async def encode(self, payloads: Sequence[Payload]) -> list[Payload]:
        ref = json.dumps({"sha256": "0" * 64, "size": 10}).encode()
        return [
            Payload(metadata={"encoding": CLAIM_CHECK_ENCODING, "kontra-v": b"1"}, data=ref)
            for _ in payloads
        ]

    async def decode(self, payloads: Sequence[Payload]) -> list[Payload]:
        return list(payloads)


async def test_the_graph_page_shows_payloads_through_its_io_route_alone(
    dev_server: DevServer, server: CodecServer
) -> None:
    client = await Client.connect(
        dev_server.grpc_address, namespace="default", data_converter=seller_data_converter(_codec())
    )
    queue = f"console-leak-{uuid.uuid4().hex[:8]}"
    wf, refused = f"console-{uuid.uuid4().hex}", f"console-refuse-{uuid.uuid4().hex}"
    seen: dict[str, bytes | str] = {}
    heartbeats: list[str] = []
    async with Worker(
        client,
        task_queue=queue,
        workflows=[LeakFlow, RefuseFlow],
        activities=[heartbeat_then_fail, item],
        workflow_runner=workflow_runner(),
        plugins=[ParallelPlugin()],
    ):
        handle = await client.start_workflow(
            LeakFlow.run, f"input {MARKER}", id=wf, task_queue=queue
        )
        run = handle.result_run_id or ""
        stream = asyncio.create_task(asyncio.to_thread(_stream, server, f"/api/events?w={wf}"))
        # While it runs: the graph, and the heartbeat as DescribeWorkflowExecution holds it.
        polls = 0
        while not stream.done():
            r = await asyncio.to_thread(_get, server, f"/api/graph?w={wf}&r={run}")
            assert r.status_code == 200, r.text
            seen[f"graph while running #{polls}"] = r.text
            polls += 1
            described = (await handle.describe()).raw_description
            for pending in described.pending_activities:
                if pending.heartbeat_details.payloads:
                    heartbeats += [
                        base64.b64encode(p.data).decode()
                        for p in [*pending.heartbeat_details.payloads]
                    ]
            await asyncio.sleep(0.3)
        seen["stream"] = await stream
        assert await handle.result() == f"RESULT INPUT {MARKER.upper()} and 3 items"
        with pytest.raises(WorkflowFailureError):
            await client.execute_workflow(
                RefuseFlow.run, f"refuse {MARKER}", id=refused, task_queue=queue
            )
    assert heartbeats, "the page never read the run while its heartbeat was pending"
    assert b"event: end" in seen["stream"]  # type: ignore[operator]
    for path in (f"/api/graph?w={wf}", f"/api/graph?w={refused}", "/graph", f"/graph?w={wf}"):
        r = _get(server, path)
        assert r.status_code == 200, (path, r.text)
        seen[path] = r.text
    deadline = time.monotonic() + 30
    while True:  # visibility catches up with the runs a moment later
        listed = _get(server, "/api/workflows")
        if {wf, refused} <= {
            x["workflow_id"] for x in listed.json()
        } or time.monotonic() > deadline:
            break
        await asyncio.sleep(0.5)
    seen["/api/workflows"] = listed.text
    # The runs, not the batch child each batch is (Temporal lists it, between them).
    listed_ids = {x["workflow_id"] for x in listed.json()}
    assert {wf, refused} <= listed_ids
    child_id = next(
        n for n in json.loads(seen[f"/api/graph?w={wf}"])["nodes"] if n["kind"] == "child"
    )["ref"]["workflow_id"]
    assert [x.id async for x in client.list_workflows(f"WorkflowId = '{child_id}'")] == [child_id]
    assert child_id not in listed_ids

    # Every payload of the runs and of their batch child, as stored, and the heartbeats.
    final = json.loads(seen[f"/api/graph?w={wf}"])
    [batch] = [n for n in final["nodes"] if n["kind"] == "child"]
    assert batch["id"] == "kontra.batch.v1[item]"  # named from the child's own history
    histories = [
        (await client.get_workflow_handle(w).fetch_history()).to_json_dict()
        for w in (wf, refused, batch["ref"]["workflow_id"])
    ]
    assert server.console is not None
    assert len(server.console._children) == 1  # the closed batch child's history, kept
    sealed = _sealed_forms(histories, heartbeats)
    assert len(sealed) > 20
    for where, body in seen.items():
        _assert_clean(where, body, sealed)

    # The control: the io route opens each of them, and only for the page's own requests.
    io = f"/api/io?w={wf}&r={run}&node=heartbeat_then_fail"
    page_req = {"X-Kontra-Console": "1"}
    opened = _get(server, io, **page_req)
    assert opened.status_code == 200, opened.text
    assert opened.headers["Cache-Control"] == "no-store"
    assert opened.headers["X-Content-Type-Options"] == "nosniff"
    with pytest.raises(AssertionError):  # the scan finds the marker where it is
        _assert_clean("the control", opened.text, sealed)
    found = opened.json()
    assert found["input"] == [f"input {MARKER}"]
    assert found["result"] == f"RESULT INPUT {MARKER.upper()}"
    assert found["failure"]["message"] == f"first attempt failed on input {MARKER}"
    workflow_io = _get(server, f"/api/io?w={wf}&node=", **page_req).json()
    assert workflow_io["input"] == [f"input {MARKER}"]
    assert MARKER.upper() in workflow_io["result"]
    batch_io = _get(server, f"/api/io?w={wf}&node={batch['id']}", **page_req).json()
    assert batch_io != found  # that node's own payloads, not the first node's
    assert all(f"input {MARKER} {i}" in json.dumps(batch_io["input"]) for i in range(3))
    refused_io = _get(server, f"/api/io?w={refused}&node=", **page_req).json()
    assert refused_io["failure"]["message"] == f"refused refuse {MARKER}"
    # /api/session: the same graph, and LeakFlow's session -- no agent, so no rows (OD2).
    sess = _get(server, f"/api/session?w={wf}&r={run}", **page_req)
    assert sess.status_code == 200, sess.text
    assert sess.json()["graph"] == final
    assert (sess.json()["session"]["empty"], sess.json()["session"]["reason"]) == (True, "no-agent")
    _assert_clean("/api/session", sess.text, sealed)
    assert _get(server, f"/api/io?w={wf}&r={run}&event=0", **page_req).status_code == 404
    same_origin = _get(
        server,
        io,
        **page_req,
        Origin=f"http://localhost:{server.port}",
        **{"Sec-Fetch-Site": "same-origin"},
        Host=f"localhost:{server.port}",
    )
    assert same_origin.json() == found

    refusals = {
        "no header": {},
        "another value": {"X-Kontra-Console": "true"},
        "a rebound name": {**page_req, "Host": f"evil.example:{server.port}"},
        "another port": {**page_req, "Host": "127.0.0.1:1"},
        "the UI's origin": {**page_req, "Origin": UI},
        "another site": {**page_req, "Origin": "https://evil.example"},
        "an opaque origin": {**page_req, "Origin": "null"},
        "a cross-site fetch": {**page_req, "Sec-Fetch-Site": "cross-site"},
        "a same-site fetch": {**page_req, "Sec-Fetch-Site": "same-site"},
    }
    for why, headers in refusals.items():
        for route in (io, f"/api/session?w={wf}&r={run}", f"/api/io?w={wf}&event=0"):
            r = _get(server, route, **headers)
            assert r.status_code == 403, (route, why)
            _assert_clean(why, r.text, sealed)
    for origin in (UI, f"http://127.0.0.1:{server.port}", "https://evil.example"):
        pre = _preflight(server, io, origin)
        assert pre.status_code == 403, origin
        assert "Access-Control-Allow-Origin" not in pre.headers


async def test_what_this_machine_cannot_open_is_shown_as_a_placeholder(
    dev_server: DevServer, server: CodecServer
) -> None:
    queue = f"console-idle-{uuid.uuid4().hex[:8]}"  # no worker: the runs never start
    theirs = KontraCodec(OTHER_SECRET, [OTHER_PUBLIC], role="seller")
    started: list[str] = []
    for name, codec in (("theirs", theirs), ("claimed", _Claimed())):
        client = await Client.connect(
            dev_server.grpc_address,
            namespace="default",
            data_converter=DataConverter(payload_codec=codec),
        )
        wf = f"console-{name}-{uuid.uuid4().hex}"
        await client.start_workflow(IdleFlow.run, f"sealed {MARKER}", id=wf, task_queue=queue)
        started.append(wf)
    try:
        theirs_io, claimed_io = (
            _get(server, f"/api/io?w={wf}&node=", **{"X-Kontra-Console": "1"}) for wf in started
        )
        assert theirs_io.status_code == 200, theirs_io.text
        assert theirs_io.json()["input"] == [{"encrypted": True}]
        assert claimed_io.json()["input"] == [{"claim_check": True}]
        histories = [
            (await dev_server.client.get_workflow_handle(wf).fetch_history()).to_json_dict()
            for wf in started[:1]
        ]
        _assert_clean("another key's", theirs_io.text, _sealed_forms(histories, []))
    finally:
        for wf in started:
            await dev_server.client.get_workflow_handle(wf).terminate()


def test_the_encode_and_decode_routes_keep_the_ui_s_origin_alone(server: CodecServer) -> None:
    """The console's own origin reaches the console's routes, never the codec's: a page on
    it has no business decrypting arbitrary payloads."""
    from google.protobuf import json_format
    from temporalio.api.common.v1 import Payloads

    sealed = Payload(metadata={"encoding": ENCODING, "kontra-v": b"1"}, data=b"x")
    body = json_format.MessageToJson(Payloads(payloads=[sealed])).encode()
    with httpx.Client(trust_env=False) as http:
        for origin in (f"http://127.0.0.1:{server.port}", f"http://localhost:{server.port}"):
            r = http.post(
                f"http://127.0.0.1:{server.port}/decode",
                content=body,
                headers={"Content-Type": "application/json", "Origin": origin},
            )
            assert r.status_code == 403, origin
