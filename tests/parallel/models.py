"""Pydantic models for the batch tests: results and an accumulator that must come back as
their own types, not as the dicts they travel as, and a page of the seller's own."""

from __future__ import annotations

from pydantic import BaseModel


class Hit(BaseModel):
    target: str
    open: bool


class Tally(BaseModel):
    seen: int = 0
    open: list[str] = []


class Words(BaseModel):
    """A page as a seller's API has it: no ``kontra`` type needed."""

    items: list[str]
    next: str | None = None
