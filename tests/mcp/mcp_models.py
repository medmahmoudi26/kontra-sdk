"""The test seller's types (its contract is derived from them). ``Artifact`` is the only
Kontra type, as in a seller's ``models.py`` (D1)."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from kontra.types import Artifact

Mode = Literal["ok", "fail", "beats", "stuck", "files"]


class WordsJob(BaseModel):
    """``acme/words``: upper-case ``text`` and count its words. ``mode``: ``ok``; ``fail``
    (the workflow fails); ``beats`` (an activity heartbeats ``beats`` times with percent and
    message); ``stuck`` (heartbeats once, at 50%, then waits until cancelled); ``files`` (the
    output carries files)."""

    model_config = ConfigDict(extra="forbid")

    text: str = Field(min_length=1, max_length=2000)
    mode: Mode = "ok"
    beats: int = Field(default=0, ge=0, le=100)


class Count(BaseModel):
    model_config = ConfigDict(extra="forbid")

    word: str
    n: int


class WordsOut(BaseModel):
    model_config = ConfigDict(extra="forbid")

    upper: str
    words: int
    counts: list[Count]
    file: Artifact | None = None
    files: dict[str, Artifact] = Field(default_factory=dict[str, Artifact])


class DocJob(BaseModel):
    """``acme/docs``: an input that is a file, which an agent cannot upload over MCP."""

    model_config = ConfigDict(extra="forbid")

    doc: Artifact


class DocOut(BaseModel):
    model_config = ConfigDict(extra="forbid")

    size: int
