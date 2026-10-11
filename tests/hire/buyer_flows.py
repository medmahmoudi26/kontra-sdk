"""Buyer workflows that hire through ``service()`` (PRD §7.2): an importable module, as
Temporal's sandbox requires. Buyer code may import ``kontra.hire`` (D1 is about sellers)."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel
from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from kontra.hire import KontraError, service


class Order(BaseModel):
    offering: str
    text: str
    mode: str = "ok"
    max_price: str | None = None


@workflow.defn
class HireOne:
    """Hires one job and returns what the Record offers (errors fail the workflow)."""

    @workflow.run
    async def run(self, order: Order) -> dict[str, Any]:
        out = await service(order.offering, max_price=order.max_price).execute(
            text=order.text, mode=order.mode
        )
        return {
            "record": out.to_dict(),
            "text": out.text,
            "chars": out.summary.chars,
            "second_part": out.parts[1]["index"],
        }


@workflow.defn
class HireCatching:
    """Hires one job and reports a hire error as data: the error classes survive the trip
    from the hire activity and the Nexus operation into workflow code."""

    @workflow.run
    async def run(self, order: Order) -> dict[str, Any]:
        try:
            out = await service(order.offering, max_price=order.max_price).execute(
                {"text": order.text, "mode": order.mode}
            )
        except KontraError as err:
            return {"error": type(err).__name__, "message": str(err), "fields": err.fields}
        return {"record": out.to_dict()}
