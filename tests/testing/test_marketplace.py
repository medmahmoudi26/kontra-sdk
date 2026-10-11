"""The harness itself: a Marketplace wires a dev server, the fake gate and logged-in
parties together, and tears down what it started; a served gate runs on its own thread."""

from __future__ import annotations

import socket
import threading

import httpx
import pytest
from temporalio.api.workflowservice.v1 import DescribeNamespaceRequest

from kontra.contract.names import NameError_, is_endpoint_name
from kontra.gate_client import GateError
from kontra.testing import Marketplace, devserver, fake_gate, marketplace
from kontra.testing.devserver import DevServer, acquire_shared_dev_server, release_shared_dev_server
from kontra.testing.fake_gate import FakeGate

from .support import handle, json_payload, offering_doc, wire


async def test_a_marketplace_without_temporal() -> None:
    async with Marketplace(temporal=False) as mp:
        seller = await mp.party("acme", balance="12.34")
        buyer = await mp.party("bob", balance=0)
        judge = await mp.party("judge", resolver=True)
        assert (await seller.balance()).available == "12.34"
        assert (await buyer.balance()).available_minor == 0
        assert "judge" in mp.gate.resolvers
        assert (await judge.gate.disputes()).items == []
        assert seller.env() == {"KONTRA_HOME": str(seller.home)}
        assert seller.identity.startswith("AGE-SECRET-KEY-1")
        assert seller.public_key == (await seller.gate.me()).public_key
        assert (seller.home / "profile").exists()
        entry = await seller.gate.publish(offering_doc("acme", seller.public_key))
        assert is_endpoint_name(entry.endpoint)
        with pytest.raises(GateError) as e:  # nobody can answer without Temporal
            await buyer.gate.quote("acme/enrich", wire(json_payload({"text": "x"})))
        assert e.value.code == "seller_unavailable"
        with pytest.raises(ValueError, match="already exists"):
            await mp.party("acme")
        with pytest.raises(NameError_):
            await mp.party("Not A Handle")
        with pytest.raises(RuntimeError, match="without Temporal"):
            _ = mp.control
        root = seller.home.parent
    assert not root.exists()  # the temporary homes are gone


@pytest.mark.temporal
async def test_a_marketplace_on_a_shared_dev_server(dev_server: DevServer) -> None:
    async with Marketplace(dev_server=dev_server) as mp:
        assert mp.dev_server is dev_server
        assert mp.gate.temporal_address == dev_server.grpc_address
        name = handle("ns")
        party = await mp.party(name)
        me = await party.gate.me()
        assert (me.namespace, me.temporal_address, me.dev) == (name, dev_server.grpc_address, True)
        described = await dev_server.client.workflow_service.describe_namespace(
            DescribeNamespaceRequest(namespace=name)
        )
        assert described.namespace_info.name == name
        client = (
            await party.temporal_client()
        )  # the worker JWT as api_key, refreshed in the background
        assert client.namespace == name
        assert client.api_key is not None
        assert mp.client(name).namespace == name
    # the shared server outlives the marketplace
    await dev_server.client.workflow_service.describe_namespace(
        DescribeNamespaceRequest(namespace=name)
    )


@pytest.mark.temporal
async def test_a_marketplace_starts_and_stops_its_own_dev_server(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Run alone, a real second server is started and stopped. In a full run the shared
    server is up (one dev server at a time on this machine), so the marketplace is handed a
    borrowed one: the wiring -- start one, use it, stop exactly that one -- is still tested
    instead of the test being skipped every time."""
    stopped: list[DevServer] = []
    if devserver._refs:  # pyright: ignore[reportPrivateUsage]
        shared = acquire_shared_dev_server()

        class Borrowed(DevServer):
            async def stop(self) -> None:
                stopped.append(self)

        borrowed = Borrowed(shared.env, shared.grpc_address, shared.http_base)

        async def start_borrowed() -> DevServer:
            return borrowed

        monkeypatch.setattr(marketplace, "start_dev_server", start_borrowed)
        try:
            async with Marketplace() as mp:
                assert mp.dev_server is borrowed
                party = await mp.party(handle("own"))
                assert (await party.gate.me()).temporal_address == shared.grpc_address
            assert stopped == [borrowed]
        finally:
            release_shared_dev_server()
        return
    async with Marketplace() as mp:
        address = mp.dev_server.grpc_address
        party = await mp.party(handle("own"))
        assert (await party.gate.me()).temporal_address == address
    host, port = address.rsplit(":", 1)
    with pytest.raises(ConnectionRefusedError):  # the server is gone
        socket.create_connection((host, int(port)), timeout=2).close()


@pytest.mark.temporal
def test_the_shared_dev_server_is_reference_counted(dev_server: DevServer) -> None:
    again = acquire_shared_dev_server()
    try:
        assert again is dev_server
    finally:
        release_shared_dev_server()
    assert devserver._shared is dev_server  # pyright: ignore[reportPrivateUsage]


async def test_a_served_gate_lives_on_its_own_thread() -> None:
    gate = fake_gate(None, sweep_interval=0.05)
    assert isinstance(gate, FakeGate)
    base = await gate.serve()
    try:
        assert gate.base_url == base
        assert base.startswith("http://127.0.0.1:")
        with pytest.raises(RuntimeError, match="already serving"):
            await gate.serve()
        async with httpx.AsyncClient(base_url=base) as http:
            assert (await http.get("/healthz")).json() == {"status": "ok", "dev": True}

        async def where() -> str:
            return threading.current_thread().name

        assert await gate.run(where) == "fake-gate"
    finally:
        await gate.stop()
        await gate.stop()  # idempotent
    async with httpx.AsyncClient() as http:
        with pytest.raises(httpx.TransportError):
            await http.get(base + "/healthz")


def test_the_factory_and_the_module_agree() -> None:
    import kontra.testing.fake_gate as module

    assert isinstance(module(None), FakeGate)  # type: ignore[operator]
    assert isinstance(fake_gate(None, resolvers=("judge",)), FakeGate)
