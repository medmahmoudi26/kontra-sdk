"""A job outlives its version (decisions I22). Retiring the version a job was hired under,
or the platform keeping that version's document as spec v1 sealed it (before 0.2.0, with the
seller's ``public_key``), must not make the delivery unreadable: ``Kontra.job(id).result()``
checks it against the exact contract the job names. Nobody hires that version again."""

from __future__ import annotations

import base64
import dataclasses
from collections.abc import AsyncIterator

import pytest
import pytest_asyncio
from temporalio.api.common.v1 import Payload

from kontra.contract import contract_hash, seal
from kontra.gate_client import GateError
from kontra.gate_client import models as m
from kontra.hire import HireRefused, Kontra, Record
from kontra.mcp._server import KontraTools
from kontra.testing.devserver import DevServer
from kontra.testing.fake_gate import FakeGate

from .support import Buyer, World, open_world

pytestmark = [pytest.mark.temporal, pytest.mark.asyncio(loop_scope="module")]


@pytest_asyncio.fixture(loop_scope="module", scope="module")
async def world(
    dev_server: DevServer, tmp_path_factory: pytest.TempPathFactory
) -> AsyncIterator[World]:
    async with open_world(dev_server, tmp_path_factory.mktemp("acme")) as w:
        yield w


def _retire(gate: FakeGate, key: str) -> None:
    gate.offerings[key].retired = True


def _retire_as_v1(gate: FakeGate, key: str, seller_key: str) -> str:
    """What the platform's release left behind: the version retired, its document as spec v1
    sealed it (the seller's key in x-kontra, under the hash), and its jobs naming that hash."""
    row = gate.offerings[key]
    doc = row.info.document
    v1 = seal({**doc, "x-kontra": {**doc["x-kontra"], "public_key": seller_key}})
    row.info = dataclasses.replace(row.info, document=v1, contract_hash=contract_hash(v1))
    for job in gate.jobs.values():
        if job.record.offering == key:
            job.contract_hash = row.info.contract_hash
    row.retired = True
    return row.info.contract_hash


async def _delivered(buyer: Buyer, key: str, text: str) -> str:
    job = await buyer.k.hire(key, text=text)
    assert (await job.result(timeout=30)).text == text.upper()
    return job.job_id


async def _refused_to_hire(k: Kontra, key: str) -> None:
    """The exact version is answered (retired: true), so the SDK refuses it itself; the gate
    would not quote it anyway."""
    with pytest.raises(HireRefused) as refused:
        await k.quote(key, text="again")
    assert refused.value.code == "offering_retired"
    with pytest.raises(HireRefused) as refused:
        await k.hire(key, text="again")
    assert refused.value.code == "offering_retired"
    plain = Payload(metadata={"encoding": b"json/plain"}, data=b'{"text": "again"}')
    wire = m.WirePayload(b64=base64.b64encode(plain.SerializeToString()).decode())
    with pytest.raises(GateError) as e:
        await k.gate.quote(key, wire)
    assert (e.value.status, e.value.code) == (404, "offering_not_found")


async def test_a_job_reads_back_once_its_version_is_retired(world: World) -> None:
    buyer, key = await world.buyer(), world.key()
    job_id = await _delivered(buyer, key, "before the release")
    await world.gate(lambda g: _retire(g, key))

    # Another process (`kontra job <id> --result`, an agent's MCP server): nothing cached.
    async with buyer.party.kontra(client=world.mp.client(buyer.party.namespace)) as later:
        assert (await later.gate.get_offering(key)).retired
        result = await (await later.job(job_id)).result(timeout=30)
        assert isinstance(result, Record)
        assert result.text == "BEFORE THE RELEASE"
        await _refused_to_hire(later, key)


async def test_a_job_reads_back_when_its_contract_is_spec_v1(world: World) -> None:
    buyer, key = await world.buyer(), world.key("limited")
    job_id = await _delivered(buyer, key, "hired under v1")
    seller_key = world.seller.public_key
    v1_hash = await world.gate(lambda g: _retire_as_v1(g, key, seller_key))

    async with buyer.party.kontra(client=world.mp.client(buyer.party.namespace)) as later:
        entry = await later.gate.get_offering(key)
        assert (entry.retired, entry.contract_hash) == (True, v1_hash)
        assert entry.document["x-kontra"]["public_key"] == seller_key
        result = await (await later.job(job_id)).result(timeout=30)
        assert result.text == "HIRED UNDER V1"
        tools = KontraTools(kontra=later)  # the MCP server's `result` tool
        got = await tools.result(job_id, None, 30)
        assert got.ready
        assert got.output["text"] == "HIRED UNDER V1"
        await _refused_to_hire(later, key)
