"""The sellers of the codec's end-to-end tests through ``kontra.serve``: plain Temporal and
pydantic, nothing from kontra (D1), in an importable module as the workflow sandbox needs."""

from __future__ import annotations

from datetime import timedelta

from pydantic import BaseModel, Field
from temporalio import activity, workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError


class Doc(BaseModel):
    text: str = Field(max_length=100_000)


class Measured(BaseModel):
    length: int


@workflow.defn
class Measure:
    @workflow.run
    async def run(self, doc: Doc) -> Measured:
        return Measured(length=len(doc.text))


class Shouted(BaseModel):
    text: str


@activity.defn
async def shout(doc: Doc) -> Shouted:
    """Heartbeats the text, fails its first attempt with the text in the message, then
    returns it upper-cased: every place a job's data can reach a worker's telemetry."""
    activity.heartbeat(f"shouting {doc.text}")
    if activity.info().attempt == 1:
        raise ApplicationError(f"not yet: {doc.text}")
    return Shouted(text=doc.text.upper())


@workflow.defn
class Shout:
    @workflow.run
    async def run(self, doc: Doc) -> Shouted:
        if doc.text.startswith("fail"):
            raise ApplicationError(f"refusing {doc.text}", non_retryable=True)
        return await workflow.execute_activity(
            shout,
            doc,
            start_to_close_timeout=timedelta(seconds=30),
            heartbeat_timeout=timedelta(seconds=30),
            retry_policy=RetryPolicy(initial_interval=timedelta(milliseconds=100)),
        )
