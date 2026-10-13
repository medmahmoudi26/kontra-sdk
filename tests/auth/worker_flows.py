"""A trivial seller for the session-lifetime test: plain Temporal and pydantic (D1), in an
importable module as the workflow sandbox needs."""

from __future__ import annotations

from pydantic import BaseModel
from temporalio import workflow


class Ping(BaseModel):
    text: str


class Pong(BaseModel):
    text: str


@workflow.defn
class PingFlow:
    @workflow.run
    async def run(self, ping: Ping) -> Pong:
        return Pong(text=ping.text)
