"""``examples/enrich`` end to end: its own ``worker.py`` serving as ``acme``, a buyer hiring it
through the fake gate, the records file downloaded and verified, and the heartbeats of a
plain activity arriving as progress, decrypted and in order (acceptance 13, buyer half).
The URLs are local web sites (:mod:`.sites`)."""

from __future__ import annotations

import base64
import hashlib
import json
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from kontra.codec.age import AgeError, decrypt
from kontra.gate_client import models as m
from kontra.hire import Progress
from kontra.testing.fake_gate import AGE_MAGIC
from kontra.types import Artifact

from .support import START, Market
from .test_golden import derive

pytestmark = [pytest.mark.temporal, pytest.mark.asyncio(loop_scope="package")]

OFFERING = "acme/enrich"


def _record(url: str, *, status: int | None = None, title: str | None = None, **rest: Any) -> Any:
    return {
        "url": url,
        "domain": "127.0.0.1",
        "status": status,
        "title": title,
        "error": None,
    } | rest


async def test_enrich_end_to_end(market: Market, tmp_path: Path) -> None:
    sites = market.sites
    buyer = await market.buyer()
    refused = sites.refused()
    urls = [
        *(sites.secure(f"/page/{i}") for i in range(96)),
        sites.plain("/"),
        sites.secure("/moved"),
        sites.plain("/missing"),
        refused,
    ]
    q = await buyer.k.quote(OFFERING, urls=urls)
    assert (q.ok, q.offering, q.price, q.currency) == (
        True,
        "acme/enrich@1.2.0",
        Decimal("0.10"),
        "USD",
    )
    job = await q.hire(max_price=1)

    events: list[Progress] = [p async for p in job.progress()]
    beats = [p for p in events if p.source == "heartbeat"]
    assert [p.message for p in beats] == [f"{n} records" for n in range(1, 101)]
    assert [p.percent for p in beats] == [float(n) for n in range(1, 101)]
    assert {p.activity for p in beats} == {"fetch_batch"}
    assert beats[0].details == {"percent": 1.0, "message": "1 records"}  # decrypted, as sent
    assert str(beats[49]) == "50% 50 records"
    assert [p.seq for p in events] == sorted(p.seq for p in events)
    assert any(p.source == "history" for p in events)  # liveness the gate reads itself
    assert events[-1].state is m.JobState.COMPLETED

    result = await job.result()
    assert result.summary == {"urls": 100, "ok": 98, "failed": 2}
    records = result.records
    assert isinstance(records, Artifact)
    assert records.content_type == "application/x-ndjson"
    path = await records.download(tmp_path / "records.jsonl")  # verifies size and sha256
    data = path.read_bytes()
    assert (len(data), hashlib.sha256(data).hexdigest()) == (records.size, records.sha256)
    lines = [json.loads(line) for line in data.splitlines()]
    assert [line["url"] for line in lines] == urls
    assert lines[0] == _record(urls[0], status=200, title="Page 0")
    assert lines[96:] == [
        _record(urls[96], status=200, title="Home"),
        _record(urls[97], status=200, title="Page moved"),  # the redirect was followed
        _record(urls[98], status=404, title="Not found"),
        _record(refused, error="ConnectError"),
    ]

    # The gate holds ciphertext only: the progress events and the records file.
    stored = await market.gate(lambda g: list(g.jobs[job.job_id].events))
    sealed = [base64.b64decode(e.ciphertext_b64) for e in stored if e.ciphertext_b64]
    assert len(sealed) == 100
    for raw in sealed:
        assert raw.startswith(AGE_MAGIC)
        assert b"records" not in raw
        with pytest.raises(AgeError):
            decrypt(raw, market.seller.identity)  # for the buyer only
    stored_file = await market.gate(lambda g: g.artifacts[records.key].data())
    assert stored_file.startswith(AGE_MAGIC)
    assert b"Page 0" not in stored_file

    view = await job.accept()
    assert (view.state, view.escrow) == (m.JobState.COMPLETED, m.Escrow.RELEASED)
    bal = await buyer.balance()
    assert (bal.available_minor, bal.held_minor) == (START - 10, 0)


async def test_no_url_can_fail_the_job(market: Market, tmp_path: Path) -> None:
    """URLs are the buyer's: a site that answers garbage or streams forever, redirects in
    circles, and URLs that pass the contract's pattern but no URL parser, each become one
    record's error -- the job still delivers. Nothing a site sends is held beyond the head
    of the final page: not an endless page, not the endless body of a redirect, and not a
    page the site compressed (two layers of gzip that inflate to 16 MB)."""
    sites = market.sites
    buyer = await market.buyer()
    urls = [
        sites.secure("/endless"),
        sites.plain("/redirect-endless"),
        sites.plain("/bomb"),
        sites.plain("/garbage"),
        sites.plain("/loop"),
        "http://[::1/",  # an unclosed IPv6 bracket
        "http://127.0.0.1:99999/",  # a port out of range
        "http://127.0.0.1:port/",
        sites.secure("/"),
    ]
    job = await buyer.k.hire(OFFERING, max_price=1, urls=urls)
    result = await job.result(timeout=120)
    assert result.summary == {"urls": 9, "ok": 4, "failed": 5}
    lines = [json.loads(line) async for line in result.records.lines()]
    assert [(r["status"], r["title"], r["error"]) for r in lines] == [
        (200, "Endless", None),
        (200, "Page landed", None),  # the redirect's body was never read
        (200, None, None),  # read as sent: never inflated, so no title
        (None, None, "RemoteProtocolError"),
        (None, None, "TooManyRedirects"),
        (None, None, "InvalidURL"),
        (None, None, lines[6]["error"]),  # whatever the resolver raises for port 99999
        (None, None, "InvalidURL"),
        (200, "Home", None),
    ]
    assert lines[5] == {
        "url": urls[5],
        "domain": "",
        "status": None,
        "title": None,
        "error": "InvalidURL",
    }
    assert all(r["error"] for r in lines[3:8])
    assert not sites.endless_capped  # the seller stopped reading; it did not buffer it all


async def test_the_served_contract_is_the_golden_one(market: Market) -> None:
    """What the worker published IS the golden document, byte for byte. No key is sealed
    into a contract any more (spec v2, decisions I7), so there is nothing left that
    differs between one seller's document and the golden file -- the contract hash is the
    same one too."""
    buyer = await market.buyer()
    entry = await buyer.k.offering(OFFERING)
    served = json.loads(json.dumps(entry.document))
    golden = derive("enrich")
    assert "public_key" not in served["x-kontra"]
    assert served == golden
