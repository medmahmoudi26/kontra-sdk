from __future__ import annotations

from datetime import timedelta
from decimal import Decimal
from typing import Any

import pytest
from pydantic import ValidationError

from kontra.contract import KontraQuote, OfferingSpec, Quote, QuoteError
from kontra.ledger import Money


def test_price_becomes_minor_units(enrich_spec: OfferingSpec) -> None:
    q = Quote(price=12.4)
    assert q.status == "ok"
    assert q.money == Money(1240, "USD")
    assert q.price == Decimal("12.40")
    assert q.currency == "USD"
    wire = q.to_wire(enrich_spec)
    assert wire == KontraQuote(
        status="ok", amount_minor=1240, currency="USD", max_duration_seconds=21600
    )


@pytest.mark.parametrize(
    ("price", "minor"),
    [(0.1 * 3, 30), ("12.345", 1235), (Decimal("0.004"), 0), (7, 700), (0, 0)],
)
def test_rounding_is_half_up_on_the_decimal_spelling(price: Any, minor: int) -> None:
    assert Quote(price=price).money == Money(minor, "USD")


def test_other_currency() -> None:
    assert Quote(price="1.5", currency="USDC").money == Money(1_500_000, "USDC")


@pytest.mark.parametrize(
    ("kwargs", "fragment"),
    [
        ({"price": -1}, "negative"),
        ({"price": True}, "bool"),
        ({"price": 1, "currency": "EUR"}, "unsupported currency"),
        ({"price": float("nan")}, "finite"),
        ({"price": "abc"}, "price"),
        ({"price": 1, "max_duration": timedelta(0)}, "positive"),
    ],
)
def test_invalid_prices(kwargs: dict[str, Any], fragment: str) -> None:
    with pytest.raises(QuoteError, match=fragment):
        Quote(**kwargs)


def test_max_duration_defaults_to_the_offering(enrich_spec: OfferingSpec) -> None:
    assert Quote(price=1).to_wire(enrich_spec).max_duration_seconds == 6 * 3600


def test_max_duration_may_be_shorter(enrich_spec: OfferingSpec) -> None:
    q = Quote(price=1, max_duration=timedelta(minutes=10, milliseconds=1))
    assert q.to_wire(enrich_spec).max_duration_seconds == 601  # rounded up, never down


def test_max_duration_may_not_exceed_the_offering(enrich_spec: OfferingSpec) -> None:
    with pytest.raises(QuoteError, match="exceeds"):
        Quote(price=1, max_duration=timedelta(hours=7)).to_wire(enrich_spec)


def test_reject(enrich_spec: OfferingSpec) -> None:
    q = Quote.reject("  max 10,000 urls per job ")
    assert q.status == "rejected"
    assert q.reason == "max 10,000 urls per job"
    assert q.price is None
    assert q.to_wire(enrich_spec) == KontraQuote(
        status="rejected", reason="max 10,000 urls per job"
    )


@pytest.mark.parametrize("reason", ["", "   "])
def test_reject_needs_a_reason(reason: str) -> None:
    with pytest.raises(QuoteError, match="reason"):
        Quote.reject(reason)


def test_reject_reason_is_capped(enrich_spec: OfferingSpec) -> None:
    wire = Quote.reject("x" * 5000).to_wire(enrich_spec)
    assert wire.reason is not None
    assert len(wire.reason) == 1000


def test_busy(enrich_spec: OfferingSpec) -> None:
    assert Quote.busy(timedelta(seconds=90.2)).to_wire(enrich_spec) == KontraQuote(
        status="busy", eta_seconds=91
    )
    assert Quote.busy(30, reason="10 jobs open").to_wire(enrich_spec) == KontraQuote(
        status="busy", eta_seconds=30, reason="10 jobs open"
    )
    assert Quote.busy(timedelta(days=90)).to_wire(enrich_spec).eta_seconds == 30 * 86400


@pytest.mark.parametrize("eta", [-1, timedelta(seconds=-1), True])
def test_busy_eta(eta: Any) -> None:
    with pytest.raises(QuoteError):
        Quote.busy(eta)


def test_quote_is_an_immutable_value() -> None:
    q = Quote(price=1)
    with pytest.raises(AttributeError):
        q.foo = 1  # type: ignore[attr-defined]
    assert q == Quote(price="1.00")
    assert hash(q) == hash(Quote(price="1.00"))
    assert q != Quote(price=2)
    assert q != Quote.reject("no")
    assert (q == 1) is False
    assert repr(q) == "Quote(price='1.00 USD')"
    assert repr(Quote.reject("no")) == "Quote.reject('no')"
    assert "Quote.busy" in repr(Quote.busy(3))
    assert "max_duration" in repr(Quote(price=1, max_duration=timedelta(hours=1)))


# --------------------------------------------------------------------------- KontraQuote


def test_absent_members_are_omitted_not_null() -> None:
    q = KontraQuote(status="rejected", reason="no")
    assert q.model_dump() == {"status": "rejected", "reason": "no"}
    assert q.model_dump_json() == '{"status":"rejected","reason":"no"}'
    assert KontraQuote.model_validate_json(q.model_dump_json()) == q


@pytest.mark.parametrize(
    "data",
    [
        {"status": "ok", "amount_minor": 1, "currency": "USD"},
        {
            "status": "ok",
            "amount_minor": 1,
            "currency": "USD",
            "max_duration_seconds": 1,
            "reason": "x",
        },
        {
            "status": "ok",
            "amount_minor": 1,
            "currency": "USD",
            "max_duration_seconds": 1,
            "eta_seconds": 1,
        },
        {"status": "rejected"},
        {"status": "rejected", "reason": "x", "eta_seconds": 1},
        {"status": "rejected", "reason": "x", "amount_minor": 1},
        {"status": "busy"},
        {"status": "busy", "eta_seconds": 1, "currency": "USD"},
        {"status": "maybe"},
        {"status": "ok", "amount_minor": "12", "currency": "USD", "max_duration_seconds": 1},
        {"status": "ok", "amount_minor": True, "currency": "USD", "max_duration_seconds": 1},
        {"status": "ok", "amount_minor": -1, "currency": "USD", "max_duration_seconds": 1},
        {"status": "ok", "amount_minor": 1, "currency": "usd", "max_duration_seconds": 1},
        {"status": "ok", "amount_minor": 1, "currency": "USD", "max_duration_seconds": 0},
        {"status": "rejected", "reason": "x", "extra": 1},
    ],
)
def test_wire_consistency(data: dict[str, Any]) -> None:
    with pytest.raises(ValidationError):
        KontraQuote.model_validate(data)


def test_wire_money() -> None:
    q = KontraQuote(status="ok", amount_minor=1240, currency="USD", max_duration_seconds=60)
    assert q.money == Money(1240, "USD")
    assert KontraQuote(status="busy", eta_seconds=1).money is None


# --------------------------------------------------------------------------- hardening


def test_wire_amount_is_bounded() -> None:
    """The quote comes from the seller's process and the gate locks it: an amount past
    2^53-1 loses precision in JavaScript and overflows a bigint downstream."""
    ok = {"status": "ok", "currency": "USD", "max_duration_seconds": 1}
    assert KontraQuote.model_validate({**ok, "amount_minor": 2**53 - 1}).amount_minor == 2**53 - 1
    with pytest.raises(ValidationError):
        KontraQuote.model_validate({**ok, "amount_minor": 2**53})
    with pytest.raises(ValidationError):
        KontraQuote.model_validate({**ok, "amount_minor": 10**30})


def test_wire_currency_must_be_one_the_ledger_holds() -> None:
    """``.money`` used to raise InvalidAmount on a seller's "EUR"; now the quote is refused
    when it is read, where the gate handles a bad quote."""
    with pytest.raises(ValidationError, match="ledger"):
        KontraQuote.model_validate(
            {"status": "ok", "amount_minor": 1, "currency": "EUR", "max_duration_seconds": 1}
        )


def test_a_price_too_large_to_quote() -> None:
    with pytest.raises(QuoteError):
        Quote(price=10**28)
    with pytest.raises(QuoteError, match="too large"):
        Quote(price=2**53, currency="USDC")


def test_max_duration_must_be_a_timedelta() -> None:
    with pytest.raises(QuoteError, match="timedelta"):
        Quote(price=1, max_duration=60)  # type: ignore[arg-type]


@pytest.mark.parametrize("eta", [float("nan"), float("inf"), float("-inf"), "5"])
def test_busy_eta_must_be_a_finite_number(eta: Any) -> None:
    with pytest.raises(QuoteError):
        Quote.busy(eta)


def test_busy_reason_is_capped_like_a_rejection(enrich_spec: OfferingSpec) -> None:
    """A long reason used to pass Quote.busy and then fail to_wire with a ValidationError."""
    wire = Quote.busy(5, reason="x" * 5000).to_wire(enrich_spec)
    assert wire.reason is not None
    assert len(wire.reason) == 1000


def test_busy_eta_is_clamped(enrich_spec: OfferingSpec) -> None:
    assert Quote.busy(10**30).to_wire(enrich_spec).eta_seconds == 30 * 86400
