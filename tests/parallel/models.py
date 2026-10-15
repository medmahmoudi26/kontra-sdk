"""Pydantic models for the batch tests: results and an accumulator that must come back as
their own types, not as the dicts they travel as."""

from __future__ import annotations

from pydantic import BaseModel


class Hit(BaseModel):
    target: str
    open: bool


class Tally(BaseModel):
    seen: int = 0
    open: list[str] = []
