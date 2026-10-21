"""Binding the test seller: quote functions and ``offering(...)`` calls, as a seller's
``worker.py`` / ``quote.py`` would write them."""

from __future__ import annotations

from datetime import timedelta
from decimal import Decimal
from pathlib import Path

from kontra.serve import BoundOffering, Quote, offering

from .flows import UpperFlow, UpperFlowV2
from .models import Order, OrderV2
from .streamflows import SellerStreamJob, StreamOrder

#: One cent per character per copy.
CENTS_PER_CHAR = 1


async def quote(order: Order) -> Quote:
    if order.text == "reject me":
        return Quote.reject("this text is not for sale")
    if order.text == "busy":
        return Quote.busy(30)
    price = Decimal(len(order.text) * order.copies * CENTS_PER_CHAR) / 100
    if order.text.startswith("quick"):  # a job the seller does in a minute
        return Quote(price=price, max_duration=timedelta(seconds=60))
    return Quote(price=price)


def bind(path: str | Path) -> BoundOffering:
    """The seller's offering at ``path`` (an absolute path: tests write one per version)."""
    return offering(path, workflow=UpperFlow, quote=quote)


def bind_relative() -> BoundOffering:
    """As worker.py writes it: relative to this module's directory."""
    return offering("offering.yaml", workflow=UpperFlow, quote=quote)


async def quote_v2(order: OrderV2) -> Quote:
    return Quote(price=Decimal(len(order.text)) / 100)


def bind_v2(path: str | Path) -> BoundOffering:
    """The same offering.yaml bound to edited types: a different contract hash."""
    return offering(path, workflow=UpperFlowV2, quote=quote_v2)


async def quote_stream(order: StreamOrder) -> Quote:
    return Quote(price=Decimal("0.05"))


def bind_stream(path: str | Path) -> BoundOffering:
    """The seam ⑤ seller (``tests/serve/test_streams.py``): a workflow that hosts its own
    stream and lists ``events``."""
    return offering(path, workflow=SellerStreamJob, quote=quote_stream)
