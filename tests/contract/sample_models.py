"""The enrich example's models (PRD §6.2, §10.8), as a seller would write them.

``urls`` is a plain ``str`` annotated ``format: uri`` rather than pydantic's ``AnyUrl``,
which would normalise (``https://a.com`` -> ``https://a.com/``) and so change the value the
buyer sent. ``summary`` is the PRD's ``summary: dict`` made nexgen-expressible
(decisions I2: ``dict[str, int]``).
"""

from __future__ import annotations

from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field

from kontra.types import Artifact

Url = Annotated[str, Field(min_length=1, max_length=2048, json_schema_extra={"format": "uri"})]


class EnrichInput(BaseModel):
    """URLs to resolve to company records."""

    model_config = ConfigDict(extra="forbid")

    urls: list[Url] = Field(min_length=1, max_length=10_000)


class EnrichOutput(BaseModel):
    """Company records as JSON lines, and a count per resolution status."""

    model_config = ConfigDict(extra="forbid")

    records: Artifact
    summary: dict[str, int]
