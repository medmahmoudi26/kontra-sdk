"""``examples/hello`` end to end: the smallest offering (``acme/hello``), its own
``worker.py`` serving as ``acme``, hired by a buyer. A flat cent per greeting, the greeting
delivered, and a blank name refused before anything is locked: an empty one by the contract
on the buyer's machine, one of only spaces by the seller's quote."""

from __future__ import annotations

from decimal import Decimal

import pytest

from kontra.gate_client import models as m
from kontra.hire import InvalidInput, QuoteRejected

from .support import START, Market

pytestmark = [pytest.mark.temporal, pytest.mark.asyncio(loop_scope="package")]

OFFERING = "acme/hello"


async def test_hello_end_to_end(market: Market) -> None:
    buyer = await market.buyer()
    q = await buyer.k.quote(OFFERING, name="Ada Lovelace")
    assert (q.ok, q.offering, q.price, q.currency) == (
        True,
        "acme/hello@0.1.0",
        Decimal("0.01"),
        "USD",
    )
    job = await q.hire(max_price="0.01")

    result = await job.result(timeout=60)
    assert result.greeting == "Hi Ada Lovelace"
    assert result.to_dict() == {"greeting": "Hi Ada Lovelace"}

    view = await job.accept()
    assert (view.state, view.escrow) == (m.JobState.COMPLETED, m.Escrow.RELEASED)
    bal = await buyer.balance()
    assert (bal.available_minor, bal.held_minor) == (START - 1, 0)


@pytest.mark.parametrize("name", [" ", " \t "])
async def test_a_blank_name_is_a_rejected_quote(market: Market, name: str) -> None:
    """A name of only whitespace passes the contract (``minLength: 1``); the seller's quote
    refuses it, so hiring it locks nothing and starts no job."""
    buyer = await market.buyer()
    q = await buyer.k.quote(OFFERING, name=name)
    assert (q.ok, q.status, q.reason) == (False, m.QuoteStatus.REJECTED, "the name is blank")
    with pytest.raises(QuoteRejected):
        await buyer.k.hire(OFFERING, max_price=1, name=name)
    bal = await buyer.balance()
    assert (bal.available_minor, bal.held_minor) == (START, 0)
    assert await buyer.k.jobs() == []


@pytest.mark.parametrize("name", ["", "x" * 101])
async def test_a_name_outside_the_contract_is_never_sent(market: Market, name: str) -> None:
    """1 to 100 characters, says the contract: anything else is refused by the buyer's own
    client, naming the field, before it is encrypted or sent."""
    buyer = await market.buyer()
    with pytest.raises(InvalidInput) as refused:
        await buyer.k.quote(OFFERING, name=name)
    assert refused.value.path == "input.name"
    assert await buyer.k.jobs() == []
