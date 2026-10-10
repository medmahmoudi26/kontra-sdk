"""The old actor's `@actor.method(takes=Target, emits=Head)` becomes the workflow's
signature: a Batch of units is a list in the input, and `dataset.push` becomes a list in the
output (or an Artifact, when the output is large)."""

from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field

__all__ = ["Head", "ProbeInput", "ProbeOutput", "Target"]

Host = Annotated[str, Field(pattern=r"^[A-Za-z0-9.-]+(:[0-9]{1,5})?$", max_length=253)]
Url = Annotated[str, Field(pattern=r"^https?://[^\s]+$", max_length=2048)]


class Target(BaseModel):
    """A URL, or a host to build one from (the old actor accepted either)."""

    model_config = ConfigDict(extra="forbid")

    url: Url | None = None
    host: Host | None = None

    def address(self) -> str:
        if self.url:
            return self.url
        if not self.host:
            raise ValueError("a target needs a url or a host")
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
