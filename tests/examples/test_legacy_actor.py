"""``examples/legacy-actor`` end to end: the old ``probe`` actor's ``head`` method as an
offering (``acme/probe``), its own ``worker.py`` serving as ``acme``, hired by a buyer."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import pytest

from kontra.gate_client import models as m
from kontra.hire import Progress, QuoteRejected

from .sites import SERVER
from .support import START, Market

pytestmark = [pytest.mark.temporal, pytest.mark.asyncio(loop_scope="package")]

OFFERING = "acme/probe"
PAGES = 241


def _head(url: str, **fields: Any) -> dict[str, Any]:
    return {"url": url, "status": None, "server": "", "error": None} | fields


async def test_probe_end_to_end(market: Market) -> None:
    sites = market.sites
    buyer = await market.buyer()
    refused = sites.refused()
    targets: list[dict[str, str]] = [
        {"url": sites.plain("/")},
        {"host": sites.host("plain")},  # a host: probed as http://<host>
        {"url": sites.secure("/moved")},  # HEAD follows the redirect
        {"url": sites.plain("/missing")},
        {"url": sites.plain("/redirect-twice")},  # HEAD all the way: no body is ever read
        {"url": sites.plain("/garbage")},  # not HTTP at all
        {"url": "http://[::1/"},  # passes the contract's pattern, but no URL parser
        {"url": refused},
        {"url": sites.plain("/loop")},
        *({"url": sites.secure(f"/page/{i}")} for i in range(PAGES)),
    ]
    total = len(targets)  # 250: three batches of 100, 100 and 50
    q = await buyer.k.quote(OFFERING, targets=targets)
    assert (q.ok, q.offering, q.price) == (True, "acme/probe@1.0.0", Decimal("0.13"))  # 0.125 up
    job = await q.hire(max_price="0.50")

    events: list[Progress] = [p async for p in job.progress()]
    beats = [p for p in events if p.source == "heartbeat"]
    # The job's progress across its batches, not each batch's own 0..100%.
    assert [p.message for p in beats] == [f"{n}/{total} targets" for n in range(1, total + 1)]
    assert [p.percent for p in beats] == [round(100 * n / total, 1) for n in range(1, total + 1)]
    assert {p.activity for p in beats} == {"head_batch"}
    assert events[-1].state is m.JobState.COMPLETED

    result = await job.result()
    heads = result.to_dict()["heads"]
    assert [h["url"] for h in heads[:9]] == [
        sites.plain("/"),
        "http://" + sites.host("plain"),
        sites.secure("/moved"),
        sites.plain("/missing"),
        sites.plain("/redirect-twice"),
        sites.plain("/garbage"),
        "http://[::1/",
        refused,
        sites.plain("/loop"),
    ]
    assert heads[:5] == [
        _head(heads[0]["url"], status=200, server=SERVER),
        _head(heads[1]["url"], status=200, server=SERVER),
        _head(heads[2]["url"], status=200, server=SERVER),
        _head(heads[3]["url"], status=404, server=SERVER),
        _head(heads[4]["url"], status=200, server=SERVER),
    ]
    assert [h["error"] for h in heads[5:9]] == [
        "RemoteProtocolError",
        "InvalidURL",
        "ConnectError",
        "TooManyRedirects",
    ]
    assert heads[9:] == [
        _head(sites.secure(f"/page/{i}"), status=200, server=SERVER) for i in range(PAGES)
    ]
    assert not sites.endless_capped  # the redirect's endless body (to a GET) was never read

    view = await job.accept()
    assert (view.state, view.escrow) == (m.JobState.COMPLETED, m.Escrow.RELEASED)
    bal = await buyer.balance()
    assert (bal.available_minor, bal.held_minor) == (START - 13, 0)


@pytest.mark.parametrize("target", [{}, {"url": "http://example.com/", "host": "example.com"}])
async def test_a_target_is_a_url_or_a_host(market: Market, target: dict[str, str]) -> None:
    """The schema cannot say "exactly one of url and host"; the model can, and the seller's
    handler checks it, so such a target is a rejected quote: nothing is locked."""
    buyer = await market.buyer()
    targets = [{"url": market.sites.plain("/")}, target]
    q = await buyer.k.quote(OFFERING, targets=targets)
    assert q.status is m.QuoteStatus.REJECTED
    assert "does not match the contract" in (q.reason or "")
    with pytest.raises(QuoteRejected):
        await buyer.k.hire(OFFERING, max_price=1, targets=targets)
    bal = await buyer.balance()
    assert (bal.available_minor, bal.held_minor) == (START, 0)
    assert await buyer.k.jobs() == []
