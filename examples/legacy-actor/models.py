"""The old actor's `@actor.method(takes=Target, emits=Head)` becomes the workflow's
signature: a Batch of units is a list in the input, and `dataset.push` becomes a list in the
output (or an Artifact, when the output is large)."""

from typing import Annotated, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

__all__ = ["Batch", "Head", "ProbeInput", "ProbeOutput", "Target"]

Host = Annotated[str, Field(pattern=r"^[A-Za-z0-9.-]+(:[0-9]{1,5})?$", max_length=253)]
Url = Annotated[str, Field(pattern=r"^https?://[^\s]+$", max_length=2048)]


class Target(BaseModel):
    """A URL, or a host to build one from (the old actor accepted either)."""

    model_config = ConfigDict(extra="forbid")

    url: Url | None = None
    host: Host | None = None

    @model_validator(mode="after")
    def _one_address(self) -> Self:
        # The contract's schema cannot say "exactly one of" in nexgen's subset; the worker's
        # handler checks the model too, so a target with neither (or both) gets a rejected
        # quote, before any job exists.
        if (self.url is None) == (self.host is None):
            raise ValueError("a target is a url or a host: exactly one of the two")
        return self

    def address(self) -> str:
        if self.url is not None:
            return self.url
        assert self.host is not None  # the validator above
        return "http://" + self.host


class ProbeInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    targets: list[Target] = Field(min_length=1, max_length=5_000)


class Head(BaseModel):
    model_config = ConfigDict(extra="forbid")

    url: str
    status: int | None = None
    server: str = ""
    error: str | None = None


class ProbeOutput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    heads: list[Head]


class Batch(BaseModel):
    """What one `head_batch` activity works on, and where it sits in the whole job (so its
    heartbeats report the job's progress, not the batch's). Not part of the contract."""

    targets: list[Target]
    done: int
    total: int
