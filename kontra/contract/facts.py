"""The ``x-kontra`` block (``spec/x-kontra.v1.json``): the offering facts as published.

Unlike :class:`~kontra.contract.offering.OfferingSpec`, which forgives the spellings people
type, this model accepts only canonical spellings from a document (``6h``, never
``360m``), because the block is hashed: two spellings of one fact would be two contracts.
"""

from __future__ import annotations

import re
from datetime import timedelta
from typing import Any

import pyrage
from pydantic import BaseModel, ConfigDict, field_serializer, field_validator

from kontra.contract.names import OfferingId
from kontra.contract.offering import (
    ARTIFACT_RETENTION_RANGE,
    MAX_DURATION_RANGE,
    REVIEW_WINDOW_RANGE,
    OfferingSpec,
    check_concurrency,
    check_description,
    check_duration,
    check_name,
    check_pricing_hint,
    check_size,
    check_tags,
    check_version,
)
from kontra.contract.units import format_duration, format_size

__all__ = ["XKontra", "check_public_key"]

_HASH = re.compile(r"sha256:[0-9a-f]{64}", re.ASCII)

#: pyrage is a native module without type stubs; its x25519 submodule is an attribute.
_X25519: Any = vars(pyrage)["x25519"]


def check_public_key(value: object) -> str:
    """An age X25519 recipient (``age1...``), checksum included."""
    if not isinstance(value, str) or not value.startswith("age1"):
        raise ValueError("must be an age X25519 public key (age1...)")
    try:
        _X25519.Recipient.from_str(value)
    except Exception as exc:
        raise ValueError(f"{value!r} is not a valid age public key: {exc}") from None
    return value


class XKontra(BaseModel):
    """Offering facts plus the seller's public key and the contract hash."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    name: str
    version: str
    description: str
    pricing_hint: str | None = None
    tags: list[str]
    max_duration: timedelta
    review_window: timedelta
    max_concurrent_jobs: int | None = None
    max_output_bytes: int
    artifact_retention: timedelta
    public_key: str
    contract_hash: str | None = None

    @field_validator("name", mode="before")
    @classmethod
    def _name(cls, v: object) -> str:
        return check_name(v)

    @field_validator("version", mode="before")
    @classmethod
    def _version(cls, v: object) -> str:
        return check_version(v)

    @field_validator("description", mode="before")
    @classmethod
    def _description(cls, v: object) -> str:
        return check_description(v)

    @field_validator("pricing_hint", mode="before")
    @classmethod
    def _pricing_hint(cls, v: object) -> str | None:
        return check_pricing_hint(v)

    @field_validator("tags", mode="before")
    @classmethod
    def _tags(cls, v: object) -> list[str]:
        return check_tags(v, canonical=True)

    @field_validator("max_duration", mode="before")
    @classmethod
    def _max_duration(cls, v: object) -> timedelta:
        return check_duration(v, MAX_DURATION_RANGE, canonical=True)

    @field_validator("review_window", mode="before")
    @classmethod
    def _review_window(cls, v: object) -> timedelta:
        return check_duration(v, REVIEW_WINDOW_RANGE, canonical=True)

    @field_validator("artifact_retention", mode="before")
    @classmethod
    def _artifact_retention(cls, v: object) -> timedelta:
        return check_duration(v, ARTIFACT_RETENTION_RANGE, canonical=True)

    @field_validator("max_output_bytes", mode="before")
    @classmethod
    def _max_output_bytes(cls, v: object) -> int:
        return check_size(v, canonical=True)

    @field_validator("max_concurrent_jobs", mode="before")
    @classmethod
    def _max_concurrent_jobs(cls, v: object) -> int | None:
        return check_concurrency(v)

    @field_validator("public_key", mode="before")
    @classmethod
    def _public_key(cls, v: object) -> str:
        return check_public_key(v)

    @field_validator("contract_hash", mode="before")
    @classmethod
    def _contract_hash(cls, v: object) -> str | None:
        if v is None:
            return None
        if not isinstance(v, str) or not _HASH.fullmatch(v):
            raise ValueError("must be sha256: and 64 lowercase hex digits")
        return v

    @field_serializer("max_duration", "review_window", "artifact_retention")
    def _ser_duration(self, v: timedelta) -> str:
        return format_duration(v)

    @field_serializer("max_output_bytes")
    def _ser_size(self, v: int) -> str:
        return format_size(v)

    @classmethod
    def from_offering(
        cls, spec: OfferingSpec, public_key: str, contract_hash: str | None = None
    ) -> XKontra:
        return cls(
            name=spec.name,
            version=spec.version,
            description=spec.description,
            pricing_hint=spec.pricing_hint,
            tags=list(spec.tags),
            max_duration=spec.max_duration,
            review_window=spec.review_window,
            max_concurrent_jobs=spec.max_concurrent_jobs,
            max_output_bytes=spec.max_output_bytes,
            artifact_retention=spec.artifact_retention,
            public_key=public_key,
            contract_hash=contract_hash,
        )

    @property
    def offering_id(self) -> OfferingId:
        return OfferingId.from_parts(self.name, self.version)

    def to_block(self) -> dict[str, Any]:
        """The ``x-kontra`` JSON: canonical strings, unset members omitted."""
        return self.model_dump(mode="json", exclude_none=True)
