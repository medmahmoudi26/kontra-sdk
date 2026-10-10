"""Pydantic In/Out types. The workflow's signature is the contract: `kontra publish` derives
the JSON Schema from these (`extra="forbid"` becomes `additionalProperties: false`).

`Artifact` is the only Kontra type allowed here (D1): a reference to an encrypted file in the
claim-check bucket, never the bytes.
"""

from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field

from kontra.types import Artifact

__all__ = ["Artifact", "Batch", "EnrichInput", "EnrichOutput", "Record"]

Url = Annotated[str, Field(pattern=r"^https?://[^\s]+$", max_length=2048)]


class EnrichInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    urls: list[Url] = Field(min_length=1, max_length=10_000)


class EnrichOutput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    records: Artifact  # JSONL, one Record per url; can be gigabytes
    summary: dict[str, int]  # small, inline


class Record(BaseModel):
    """One line of the `records` artifact. Not part of the contract: the buyer reads it
    from the file."""

    url: str
    domain: str
    status: int | None = None
    title: str | None = None
    error: str | None = None


class Batch(BaseModel):
    """What one `fetch_batch` activity works on, and where it sits in the whole job (so its
    heartbeats can report overall progress)."""

    urls: list[str]
    done: int
    total: int
