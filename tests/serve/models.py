"""The test seller's pydantic types (the contract is derived from them).

``Order`` keeps pydantic's default ``extra="ignore"`` on purpose: the contract closes it
anyway (``additionalProperties: false``), and the handler must refuse an extra field that
this model would silently drop."""

from __future__ import annotations

from pydantic import BaseModel, Field


class Order(BaseModel):
    text: str = Field(max_length=2000)
    copies: int = Field(default=1, ge=1, le=100)
    #: Heartbeats the ``beat`` activity sends (async), then ``sync_beat`` (in a thread).
    beats: int = Field(default=0, ge=0, le=500)
    sync_beats: int = Field(default=0, ge=0, le=50)
    #: Wait for the ``release`` signal before returning.
    hold: bool = False


class Shout(BaseModel):
    text: str
    length: int
    #: The job the root's activity, its local activity and the child's activity ran for
    #: (from Kontra's job context).
    job: str | None = None
    local_job: str | None = None
    child_job: str | None = None


class Echo(BaseModel):
    text: str


class Echoed(BaseModel):
    text: str
    job: str | None = None


class Beats(BaseModel):
    count: int
    note: str


class OrderV2(BaseModel):
    """``Order`` after the seller "just added a field" without republishing."""

    text: str = Field(max_length=2000)
    copies: int = Field(default=1, ge=1, le=100)
    beats: int = Field(default=0, ge=0, le=500)
    sync_beats: int = Field(default=0, ge=0, le=50)
    shout_louder: bool = False
