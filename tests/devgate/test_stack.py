"""``kontra.devgate.stack.DevStack``: the Host check on every request, the three control routes
(bearer token, constant-time compare, no ``Origin``), the access log, reconfiguration, and a
real stack: the Temporal child restarted on its db file, a graceful stop (decisions I34, §3,
§12).

The first part drives the stack's ASGI wrapper in process, with no Temporal (the checks sit in
front of the gate); the last test runs the real host pieces on free ports (marker
``temporal``)."""

from __future__ import annotations

import asyncio
import logging
import socket
import stat
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import httpx
import pytest

from kontra.devgate import stack as stack_module
from kontra.devgate import state
from kontra.devgate.gate import DevGate
from kontra.devgate.stack import DevStack, StackConfig
from kontra.devgate.state import Ports

TOKEN = "t0k3n-" + "x" * 40
ROUTES = (("GET", "ready"), ("POST", "config"), ("POST", "stop"))


@dataclass
class StubTemporal:
    """The Temporal child as ``reconfigure`` drives it: what it was asked, nothing started."""

    ui: bool = False
    metrics_port: int | None = None
    pid: int | None = 4243
    http_port: int = 40331
    restarts: list[tuple[bool, int | None]] = field(default_factory=list[tuple[bool, int | None]])

    async def restart(self) -> None:
        self.restarts.append((self.ui, self.metrics_port))


def _stack(tmp_path: Path, port: int = 18999) -> tuple[DevStack, StubTemporal]:
    ports = Ports(gate=port, temporal=port + 1, ui=port + 2, codec=port + 3)
    config = StackConfig(home=tmp_path / "home", ports=ports, cli=tmp_path / "no-cli")
    stack = DevStack(config, token=TOKEN)
    stack.gate = DevGate(None, base_url=stack.gate_url, dev=True)
    temporal = StubTemporal()
    stack._temporal = temporal  # type: ignore[assignment]
    return stack, temporal


def _client(stack: DevStack) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=stack._asgi),  # pyright: ignore[reportArgumentType]
        base_url=stack.gate_url,
    )


def _bearer(token: str = TOKEN, **extra: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}", **extra}


# --------------------------------------------------------------------------- the Host check


@pytest.mark.parametrize(
    "host",
    [
        "evil.example:18999",  # a page that rebinds its own name to 127.0.0.1
        "127.0.0.1.nip.io:18999",
        "127.0.0.1",  # no port: never what a client of this host sends
        "localhost",
        "127.0.0.1:19000",  # another port
        "127.0.0.1:18999.evil.example",
        "0.0.0.0:18999",
        "[::1]:18999",
        "LOCALHOST:18999.",
    ],
)
async def test_a_host_other_than_loopback_and_the_port_is_refused(
    tmp_path: Path, host: str
) -> None:
    stack, _ = _stack(tmp_path)
    async with _client(stack) as http:
        for path in ("/v1/info", "/.well-known/jwks.json", "/_devgate/ready"):
            r = await http.get(path, headers={"Host": host, **_bearer()})
            assert r.status_code == 403, (host, path)
            assert "unexpected Host" in r.text


@pytest.mark.parametrize("host", ["127.0.0.1:18999", "localhost:18999"])
async def test_loopback_with_the_port_is_served(tmp_path: Path, host: str) -> None:
    stack, _ = _stack(tmp_path)
    async with _client(stack) as http:
        r = await http.get("/v1/info", headers={"Host": host})
    assert r.status_code == 200, r.text
    assert r.json()["dev"] is True


async def test_a_request_without_a_host_is_refused(tmp_path: Path) -> None:
    stack, _ = _stack(tmp_path)
    sent: list[dict[str, Any]] = []

    async def receive() -> dict[str, Any]:
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message: dict[str, Any]) -> None:
        sent.append(message)

    scope = {
        "type": "http",
        "method": "GET",
        "path": "/_devgate/ready",
        "headers": [(b"authorization", f"Bearer {TOKEN}".encode())],
    }
    await stack._asgi(scope, receive, send)
    assert sent[0]["status"] == 403


async def test_websockets_are_refused(tmp_path: Path) -> None:
    stack, _ = _stack(tmp_path)
    sent: list[dict[str, Any]] = []

    async def receive() -> dict[str, Any]:
        return {"type": "websocket.connect"}

    async def send(message: dict[str, Any]) -> None:
        sent.append(message)

    scope = {"type": "websocket", "path": "/", "headers": [(b"host", b"127.0.0.1:18999")]}
    await stack._asgi(scope, receive, send)
    assert sent[0]["status"] == 403


# --------------------------------------------------------------------------- the control routes


@pytest.mark.parametrize(("method", "route"), ROUTES)
@pytest.mark.parametrize(
    "headers",
    [
        {},  # no token
        {"Authorization": "Bearer "},
        {"Authorization": "Bearer wrong"},
        {"Authorization": f"Bearer {TOKEN}x"},  # a longer token is not a prefix match
        {"Authorization": f"Bearer {TOKEN[:-1]}"},
        {"Authorization": f"bearer {TOKEN}"},  # the scheme exactly
        {"Authorization": TOKEN},
        {"Authorization": f"Basic {TOKEN}"},
        # The right token, from a page: any Origin is refused (a browser sends one).
        _bearer(Origin="http://127.0.0.1:18999"),
        _bearer(Origin="null"),
        _bearer(Origin="http://evil.example"),
    ],
)
async def test_a_control_route_needs_the_token_and_no_origin(
    tmp_path: Path, method: str, route: str, headers: dict[str, str]
) -> None:
    stack, temporal = _stack(tmp_path)
    stopped: list[bool] = []
    stack.request_stop = lambda: stopped.append(True)  # type: ignore[method-assign]
    async with _client(stack) as http:
        r = await http.request(method, f"/_devgate/{route}", headers=headers, json={"ui": True})
    assert r.status_code == 403
    assert "control token" in r.text
    # Nothing happened: no stop, no restart, no state leaked.
    assert stopped == []
    assert temporal.restarts == []
    assert "pid" not in r.text


async def test_the_token_is_compared_in_constant_time(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    compared: list[tuple[bytes, bytes]] = []
    real = stack_module.hmac.compare_digest

    def spy(a: bytes, b: bytes) -> bool:
        compared.append((a, b))
        return real(a, b)

    monkeypatch.setattr(stack_module.hmac, "compare_digest", spy)
    stack, _ = _stack(tmp_path)
    async with _client(stack) as http:
        assert (await http.get("/_devgate/ready", headers=_bearer("nope"))).status_code == 403
        assert (await http.get("/_devgate/ready", headers=_bearer())).status_code == 200
    want = f"Bearer {TOKEN}".encode()
    assert compared == [(b"Bearer nope", want), (want, want)]


async def test_ready_says_who_the_host_is(tmp_path: Path) -> None:
    stack, _ = _stack(tmp_path)
    async with _client(stack) as http:
        r = await http.get("/_devgate/ready", headers=_bearer())
        assert r.status_code == 200
        doc = r.json()
        assert doc["temporal_pid"] == 4243
        assert doc["temporal_http"] == 40331
        assert doc["counts"] == {"parties": 0, "offerings": 0, "open_jobs": 0}
        assert (doc["ui"], doc["metrics"], doc["persist"]) == (False, False, False)
        assert r.headers["cache-control"] == "no-store"
        # The wrong method, or a route that does not exist, with the right token.
        assert (await http.post("/_devgate/ready", headers=_bearer())).status_code == 404
        assert (await http.get("/_devgate/stop", headers=_bearer())).status_code == 404
        assert (await http.get("/_devgate/nope", headers=_bearer())).status_code == 404


async def test_hello_proves_the_token_without_being_given_it(tmp_path: Path) -> None:
    """The CLI sends the token nowhere before the port proves it holds it: ``hello`` answers
    HMAC(token, nonce) for the CLI's fresh nonce, with no token asked and nothing else told."""
    stack, temporal = _stack(tmp_path)
    nonce = "0f" * 16
    async with _client(stack) as http:
        r = await http.get("/_devgate/hello", params={"nonce": nonce})
        assert r.status_code == 200, r.text
        assert r.json() == {"proof": state.hello_proof(TOKEN, nonce)}
        assert r.headers["cache-control"] == "no-store"
        # From a page (any Origin), with a bad nonce, or by another method: no proof.
        page = await http.get(
            "/_devgate/hello", params={"nonce": nonce}, headers={"Origin": "http://evil.example"}
        )
        assert page.status_code == 403
        for bad in ("", "0f", "XYZ" * 16, "0f" * 65):
            assert (await http.get("/_devgate/hello", params={"nonce": bad})).status_code == 400
        assert (await http.post("/_devgate/hello", params={"nonce": nonce})).status_code == 404
    assert temporal.restarts == []


async def test_stop_asks_the_server_to_stop(tmp_path: Path) -> None:
    stack, _ = _stack(tmp_path)
    stopped: list[bool] = []
    stack.request_stop = lambda: stopped.append(True)  # type: ignore[method-assign]
    async with _client(stack) as http:
        r = await http.post("/_devgate/stop", headers=_bearer())
    assert r.status_code == 204
    assert stopped == [True]


async def test_config_restarts_temporal_only_when_ui_or_metrics_change(tmp_path: Path) -> None:
    stack, temporal = _stack(tmp_path)
    async with _client(stack) as http:

        async def config(**want: bool) -> dict[str, Any]:
            r = await http.post("/_devgate/config", headers=_bearer(), json=want)
            assert r.status_code == 200, r.text
            return r.json()

        assert (await config(ui=True))["restarted"] is True
        assert temporal.restarts == [(True, None)]
        assert (await config(ui=True))["restarted"] is False  # unchanged: no restart
        assert (await config(ui=True, metrics=True))["restarted"] is True
        assert temporal.restarts[-1] == (True, stack.config.ports.temporal_metrics)
        # Turning persist on snapshots at once, 0600; it restarts nothing.
        done = await config(ui=True, metrics=True, persist=True)
        assert (done["persist"], done["restarted"]) == (True, False)
    snapshot = state.data_dir(stack.config.home) / "gate.json"
    assert stat.S_IMODE(snapshot.stat().st_mode) == 0o600
    assert len(temporal.restarts) == 2
    if stack._persist_task is not None:
        stack._persist_task.cancel()


async def test_persist_turned_off_stops_writing_and_drops_the_snapshot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``kontra dev up --no-persist`` on a running stack: the loop stops, ``gate.json`` (the
    signing key, every session) and its blobs go, and nothing writes them again -- a crash
    later cannot bring back the state kept before."""
    monkeypatch.setattr(stack_module, "PERSIST_EVERY", 0.05)
    stack, _ = _stack(tmp_path)
    stack._loop = asyncio.get_running_loop()
    snapshot = state.data_dir(stack.config.home) / "gate.json"
    blobs = snapshot.parent / "blobs"
    await stack.reconfigure(ui=False, metrics=False, persist=True)
    stack.gate.blobs["ab" * 32] = b"x" * (64 * 1024)
    await asyncio.sleep(0.3)  # a few passes: the blob is a file now
    assert snapshot.exists()
    assert len(list(blobs.iterdir())) == 1
    assert stack._persist_task is not None
    done = await stack.reconfigure(ui=False, metrics=False, persist=False)
    assert done["persist"] is False
    assert stack._persist_task is None
    assert not snapshot.exists()
    assert not blobs.exists()
    stack.gate.blobs["cd" * 32] = b"y" * 10  # the state changes; nothing persists it
    await asyncio.sleep(0.3)
    assert not snapshot.exists()


async def test_a_persist_pass_writes_each_blob_once_and_only_on_change(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """§5.4: blobs are ``data/blobs/<sha256>``, written once, named by digest in ``gate.json``;
    a pass rewrites ``gate.json`` only when its digest changed."""
    monkeypatch.setattr(stack_module, "PERSIST_EVERY", 0.05)
    stack, _ = _stack(tmp_path)
    stack._loop = asyncio.get_running_loop()
    big = b"z" * (1 << 20)
    stack.gate.blobs["ef" * 32] = big
    await stack.reconfigure(ui=False, metrics=False, persist=True)
    snapshot = state.data_dir(stack.config.home) / "gate.json"
    (blob,) = list((snapshot.parent / "blobs").iterdir())
    first = (blob.stat().st_ino, blob.stat().st_mtime_ns, snapshot.stat().st_mtime_ns)
    assert snapshot.stat().st_size < 64 * 1024  # the MiB is not in it
    assert stat.S_IMODE(blob.stat().st_mode) == 0o600
    await asyncio.sleep(0.3)  # passes with nothing changed: nothing rewritten
    assert (blob.stat().st_ino, blob.stat().st_mtime_ns, snapshot.stat().st_mtime_ns) == first
    await stack.reconfigure(ui=False, metrics=False, persist=False)


@pytest.mark.parametrize("failure", ["refused state", "gate port taken"])
async def test_a_failed_start_stops_the_temporal_child(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    """``__aexit__`` does not run when ``__aenter__`` raises: whatever fails after the
    Temporal child started must stop it, or it is orphaned holding the Temporal port."""
    events: list[str] = []

    class Child:
        def __init__(self, **_: Any) -> None:
            self.http_base, self.http_port, self.pid = "http://127.0.0.1:1", 1, 4243

        async def start(self) -> None:
            events.append("start")

        async def stop(self) -> None:
            events.append("stop")

    async def connect(*_: Any, **__: Any) -> object:
        return object()

    from temporalio.client import Client

    def gate(_control: object, **options: Any) -> DevGate:
        return DevGate(None, base_url=options["base_url"], dev=True)

    monkeypatch.setattr(stack_module, "TemporalChild", Child)
    monkeypatch.setattr(stack_module, "DevGate", gate)
    monkeypatch.setattr(Client, "connect", connect)
    home = tmp_path / "home"
    held = socket.socket()
    held.bind(("127.0.0.1", 0))
    held.listen()
    gate_port = held.getsockname()[1] if failure == "gate port taken" else _free()
    if failure == "refused state":
        refused = state.data_dir(home) / "gate.json"
        refused.write_text('{"format": "kontra-devgate/0"}')
        refused.chmod(0o600)
    config = StackConfig(
        home=home,
        ports=Ports(gate=gate_port, temporal=_free(), ui=_free(), codec=_free()),
        cli=tmp_path / "temporal",
        persist=failure == "refused state",
    )
    try:
        with pytest.raises((ValueError, OSError)):
            async with DevStack(config, token=TOKEN):
                pytest.fail("the stack should not have started")
    finally:
        held.close()
    assert events == ["start", "stop"]


async def test_the_access_log_drops_query_strings(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """A ``/_blob/`` URL carries its signature in the query: it never reaches the log."""
    stack, _ = _stack(tmp_path)
    with caplog.at_level(logging.INFO, logger="kontra.devgate.access"):
        async with _client(stack) as http:
            await http.get("/v1/info?sig=s3cr3t&exp=1")
            await http.get("/_blob/x?sig=s3cr3t")
    lines = [r.getMessage() for r in caplog.records if r.name == "kontra.devgate.access"]
    assert "GET /v1/info 200" in lines
    assert any(line.startswith("GET /_blob/x ") for line in lines)
    assert not any("s3cr3t" in line or "?" in line for line in lines)


# --------------------------------------------------------------------------- a real stack


def _free() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def _listening(port: int) -> bool:
    with socket.socket() as s:
        s.settimeout(0.5)
        return s.connect_ex(("127.0.0.1", port)) == 0


@pytest.mark.temporal
async def test_a_real_stack_restarts_its_temporal_on_the_same_db_and_stops(
    tmp_path: Path,
) -> None:
    from temporalio.api.workflowservice.v1 import DescribeNamespaceRequest
    from temporalio.client import Client

    from kontra.devgate import temporal_cli

    try:
        cli = temporal_cli.ensure()
    except temporal_cli.TemporalCLIError as e:
        pytest.skip(f"no Temporal CLI: {e}")
    ports = Ports(gate=_free(), temporal=_free(), ui=_free(), codec=_free())
    config = StackConfig(home=tmp_path / "home", ports=ports, cli=cli)
    async with DevStack(config, token=TOKEN) as stack:
        gate = f"http://127.0.0.1:{ports.gate}"
        async with httpx.AsyncClient(trust_env=False, timeout=10) as http:
            ready = await http.get(f"{gate}/_devgate/ready", headers=_bearer())
            assert ready.status_code == 200
            first_pid = ready.json()["temporal_pid"]
            # Over the real socket too: a rebinding name is refused.
            evil = await http.get(f"{gate}/v1/info", headers={"Host": f"evil.example:{ports.gate}"})
            assert evil.status_code == 403
            # A namespace made before the restart is there after it: the same db file.
            assert stack._control is not None
            await stack._control.ensure_namespace("devgate-kept")
            r = await http.post(f"{gate}/_devgate/config", headers=_bearer(), json={"ui": True})
            assert r.json()["restarted"] is True
            after = (await http.get(f"{gate}/_devgate/ready", headers=_bearer())).json()
            assert after["temporal_pid"] != first_pid
            assert after["ui"] is True
            assert _listening(ports.ui)  # the dev server's own UI, on loopback
            client = await Client.connect(stack.temporal_address)
            described = await client.workflow_service.describe_namespace(
                DescribeNamespaceRequest(namespace="devgate-kept")
            )
            assert described.namespace_info.name == "devgate-kept"
            stopped = await http.post(f"{gate}/_devgate/stop", headers=_bearer())
            assert stopped.status_code == 204
        await asyncio.wait_for(stack.wait_closed(), 30)
        temporal = stack._temporal
        assert temporal is not None
    assert temporal.pid is None  # stopped with the stack
    assert not _listening(ports.gate)
    assert not _listening(ports.temporal)
    assert (state.logs_dir(config.home) / "temporal.log").exists()


@pytest.mark.temporal
async def test_a_real_stack_that_fails_to_start_leaves_no_temporal(tmp_path: Path) -> None:
    """The review's case, for real: a refused ``--persist`` state after the Temporal child is
    up. The error propagates and nothing holds the Temporal port afterwards."""
    from kontra.devgate import temporal_cli

    try:
        cli = temporal_cli.ensure()
    except temporal_cli.TemporalCLIError as e:
        pytest.skip(f"no Temporal CLI: {e}")
    home = tmp_path / "home"
    refused = state.data_dir(home) / "gate.json"
    refused.write_text('{"format": "kontra-devgate/0"}')
    refused.chmod(0o600)
    ports = Ports(gate=_free(), temporal=_free(), ui=_free(), codec=_free())
    config = StackConfig(home=home, ports=ports, cli=cli, persist=True)
    with pytest.raises(ValueError, match="kontra dev down --reset"):
        async with DevStack(config, token=TOKEN):
            pytest.fail("the stack should not have started")
    assert not _listening(ports.temporal)
    assert not _listening(ports.gate)
