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
