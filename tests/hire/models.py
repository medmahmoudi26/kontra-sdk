"""The test sellers' pydantic types (contracts are derived from them).

``Artifact`` is the only Kontra type here, as in a seller's ``models.py`` (D1)."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from kontra.types import Artifact

Mode = Literal["ok", "fail", "slow", "graceful", "beats", "quiet", "artifact"]


class TextJob(BaseModel):
    """``acme/text``: upper-case ``text``. ``mode`` picks what the seller's workflow does:
    ``ok``; ``fail`` (the workflow fails); ``slow`` (runs until cancelled); ``graceful``
    (runs until cancelled, then completes with what it has instead of ending canceled); ``beats``
    (an activity heartbeats ``beats`` times with percent and message); ``quiet`` (an
    activity works ``beats`` seconds, 2 if 0, without heartbeats); ``artifact`` (the output carries
    a file with ``beats`` lines, and ``doc``'s bytes are echoed into ``text``)."""

    model_config = ConfigDict(extra="forbid")

    text: str = Field(max_length=2000)
    mode: Mode = "ok"
    beats: int = Field(default=0, ge=0, le=500)
    doc: Artifact | None = None


class Part(BaseModel):
    model_config = ConfigDict(extra="forbid")

    index: int
    file: Artifact | None = None


class TextOut(BaseModel):
    model_config = ConfigDict(extra="forbid")

    text: str
    length: int
    summary: dict[str, int]
    parts: list[Part]
    file: Artifact | None = None
    files: dict[str, Artifact] = Field(default_factory=dict[str, Artifact])


class Target(BaseModel):
    model_config = ConfigDict(extra="forbid")

    url: str = Field(max_length=2048)


class ProbeInput(BaseModel):
    """``acme/probe`` (the recon example's first offering)."""

    model_config = ConfigDict(extra="forbid")

    targets: list[Target] = Field(min_length=1, max_length=100)


class Head(BaseModel):
    model_config = ConfigDict(extra="forbid")

    url: str
    status: int


class ProbeOutput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    heads: list[Head]


class EnrichInput(BaseModel):
    """``acme/enrich`` (the recon example's second offering)."""

    model_config = ConfigDict(extra="forbid")

    urls: list[str] = Field(min_length=1, max_length=100)


class EnrichOutput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    domains: list[str]


class FeedJob(BaseModel):
    """``acme/feed`` (``streams: [events]``): ``count`` events on the job's stream, under
    ``events``, from an activity -- and as many on ``secret``, a topic it never lists."""

    model_config = ConfigDict(extra="forbid")

    count: int = Field(ge=0, le=100)
    note: str = Field(default="", max_length=200)


class FeedOut(BaseModel):
    model_config = ConfigDict(extra="forbid")

    published: int


class FeedEvent(BaseModel):
    """An item of ``events``: the seller's, never in the contract (decisions I31, OD11); the
    buyer reads it with its own copy of the model."""

    model_config = ConfigDict(extra="forbid")

    n: int
    note: str
