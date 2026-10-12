"""The seller of the codec's end-to-end test through ``kontra.serve``: plain Temporal and
pydantic, nothing from kontra (D1), in an importable module as the workflow sandbox needs."""

from __future__ import annotations

from pydantic import BaseModel, Field
from temporalio import workflow


class Doc(BaseModel):
    text: str = Field(max_length=100_000)


class Measured(BaseModel):
    length: int


@workflow.defn
class Measure:
    @workflow.run
    async def run(self, doc: Doc) -> Measured:
        return Measured(length=len(doc.text))
