"""``offering.yaml`` (PRD §6.3): the declarative facts of one offering version.

Everything here is readable by the catalog, the console and agents without executing
Python, and nothing here names code (D11). The binding of a workflow and a quote function
to the offering is code, in ``worker.py``; this loader refuses keys that try to put it in
the yaml, and values that look like an import path.

``streams:`` lists the topics of the job's workflow stream a buyer may read (decisions I31):
data names, never code. It is validated here (and stated language-neutrally in
``spec/offering-streams.v1.json``) but is **not** an ``x-kontra`` fact: it is not in the
contract hash, and travels as catalog data (``PublishRequest.streams``), fixed per version.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from datetime import timedelta
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

from kontra.contract.errors import OfferingError
from kontra.contract.names import (
    RESERVED_TOPIC_PREFIX,
    NameError_,
    OfferingId,
    Version,
    validate_handle,
)
from kontra.contract.units import format_duration, format_size, parse_duration, parse_size

__all__ = [
    "ARTIFACT_RETENTION_RANGE",
    "MAX_CONCURRENT_JOBS_MAX",
    "MAX_DURATION_RANGE",
    "MAX_OUTPUT_BYTES_MAX",
    "REVIEW_WINDOW_RANGE",
    "STREAMS_MAX",
    "TOPIC_PATTERN",
    "OfferingSpec",
    "check_streams",
    "check_topic",
    "load_offering",
    "parse_offering",
]

#: Inclusive bounds. 30 days is far beyond any job the PRD imagines and keeps a hold from
#: locking a buyer's money for a season; a zero review window means "accept on delivery".
MAX_DURATION_RANGE = (timedelta(seconds=1), timedelta(days=30))
REVIEW_WINDOW_RANGE = (timedelta(0), timedelta(days=30))
ARTIFACT_RETENTION_RANGE = (timedelta(seconds=1), timedelta(days=90))
#: S3's largest object.
MAX_OUTPUT_BYTES_MAX = 5 * 1024**4
MAX_CONCURRENT_JOBS_MAX = 10_000
DESCRIPTION_MAX = 2000
PRICING_HINT_MAX = 200
TAGS_MAX = 16
#: A workflow-stream topic an offering lists (``streams:``): ASCII, a letter first, at most
#: 64 characters. Unanchored: match it whole (``fullmatch``, or ``^...$`` in a schema).
TOPIC_PATTERN = r"[A-Za-z][A-Za-z0-9_.-]{0,63}"
#: The most topics one offering lists.
STREAMS_MAX = 8

_TAG = re.compile(r"[a-z0-9][a-z0-9-]{0,31}", re.ASCII)
_TOPIC = re.compile(TOPIC_PATTERN, re.ASCII)
_IDENT = r"[A-Za-z_][A-Za-z0-9_]*"
#: ``workflows.EnrichFlow``, ``workflows:EnrichFlow``, ``quote:quote``.
_SYMBOL = re.compile(
    rf"{_IDENT}(?:\.{_IDENT})*:{_IDENT}(?:\.{_IDENT})*|{_IDENT}(?:\.{_IDENT})+", re.ASCII
)
_CONTROL = re.compile(r"[\x00-\x08\x0b-\x1f\x7f]")

#: Keys people reach for that bind code; they belong in worker.py (D11).
_CODE_KEYS = frozenset(
    {
        "workflow",
        "workflows",
        "quote",
        "activities",
        "activity",
        "handler",
        "entrypoint",
        "module",
        "class",
        "function",
        "worker",
        "task_queue",
        "image",
        "command",
    }
)

_KEYS_HINT = (
    "offering.yaml holds name, version, description, pricing_hint, tags, max_duration, "
    "review_window, max_concurrent_jobs, max_output_bytes, artifact_retention, "
    "uses_hosted_model and streams"
)


# --------------------------------------------------------------------------- field rules
# Shared by OfferingSpec (lenient spellings) and XKontra (canonical spellings only).


def check_name(value: object) -> str:
    if not isinstance(value, str):
        raise ValueError("must be a string party/offering, e.g. acme/enrich")
    party, sep, name = value.partition("/")
    if not sep:
        raise ValueError(f"{value!r} must be party/offering, e.g. acme/enrich")
    try:
        validate_handle(party, what="party")
        validate_handle(name, what="offering name", allow_reserved=True)
    except NameError_ as exc:
        raise ValueError(str(exc)) from None
    return value


def check_version(value: object) -> str:
    if not isinstance(value, str):
        raise ValueError(
            f"must be a string MAJOR.MINOR.PATCH; YAML read {value!r} as a "
            f'{type(value).__name__}, so quote it: version: "1.2.0"'
        )
    try:
        Version.parse(value)
    except NameError_ as exc:
        raise ValueError(str(exc)) from None
    return value


def _no_symbol(value: str) -> None:
    if _SYMBOL.fullmatch(value.strip()):
        raise ValueError(
            f"{value!r} looks like a Python import path; offering.yaml never names code "
            "(bind the workflow and quote function in worker.py)"
        )


def check_description(value: object) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("must be a non-empty string")
    if len(value) > DESCRIPTION_MAX:
        raise ValueError(f"must be at most {DESCRIPTION_MAX} characters")
    if _CONTROL.search(value):
        raise ValueError("must not contain control characters")
    _no_symbol(value)
    return value


def check_pricing_hint(value: object) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise ValueError("must be a non-empty string (or left out)")
    if len(value) > PRICING_HINT_MAX or "\n" in value:
        raise ValueError(f"must be one line of at most {PRICING_HINT_MAX} characters")
    if _CONTROL.search(value):
        raise ValueError("must not contain control characters")
    _no_symbol(value)
    return value


def check_tags(value: object, *, canonical: bool) -> list[str]:
    if not isinstance(value, list | tuple):
        raise ValueError("must be a list of tags, e.g. [enrichment, companies]")
    tags: list[str] = []
    for tag in value:  # pyright: ignore[reportUnknownVariableType]
        if not isinstance(tag, str) or not _TAG.fullmatch(tag):
            raise ValueError(
                f"tag {tag!r} must be lowercase letters, digits and hyphens, at most 32 characters"
            )
        if tag in tags:
            raise ValueError(f"tag {tag!r} appears twice")
        tags.append(tag)
    if len(tags) > TAGS_MAX:
        raise ValueError(f"at most {TAGS_MAX} tags")
    ordered = sorted(tags)
    if canonical and tags != ordered:
        raise ValueError(f"tags must be sorted: {ordered}")
    return ordered


def check_duration(
    value: object, bounds: tuple[timedelta, timedelta], *, canonical: bool
) -> timedelta:
    if isinstance(value, timedelta):
        td = value
    elif isinstance(value, str):
        td = parse_duration(value)
        if canonical and value != format_duration(td):
            raise ValueError(f"{value!r} is not canonical; write {format_duration(td)!r}")
    else:
        raise ValueError(f"must be a duration with a unit (90s, 30m, 6h, 7d), got {value!r}")
    if td.total_seconds() != int(td.total_seconds()):
        raise ValueError("must be a whole number of seconds")
    lo, hi = bounds
    if not lo <= td <= hi:
        raise ValueError(
            f"{format_duration(td) if td >= timedelta(0) else td} is outside "
            f"{format_duration(lo)}..{format_duration(hi)}"
        )
    return td


def check_size(value: object, *, canonical: bool) -> int:
    if isinstance(value, bool):
        raise ValueError(f"must be a size such as 5GB, got {value!r}")
    if isinstance(value, int):
        size = value
    elif isinstance(value, str):
        size = parse_size(value)
        if canonical and value != format_size(size):
            raise ValueError(f"{value!r} is not canonical; write {format_size(size)!r}")
    else:
        raise ValueError(f"must be a size such as 512MB, 5GB or 1GiB, got {value!r}")
    if not 0 <= size <= MAX_OUTPUT_BYTES_MAX:
        raise ValueError(f"must be between 0B and {format_size(MAX_OUTPUT_BYTES_MAX)}")
    return size


def check_concurrency(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"must be a whole number, got {value!r}")
    if not 1 <= value <= MAX_CONCURRENT_JOBS_MAX:
        raise ValueError(f"must be between 1 and {MAX_CONCURRENT_JOBS_MAX} (or left out)")
    return value


def check_hosted_model(value: object) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"must be true or false, got {value!r}")
    return value


def check_topic(value: object) -> str:
    """One topic name of ``streams:`` (or of a stream request): :data:`TOPIC_PATTERN`, whole."""
    if not isinstance(value, str) or _TOPIC.fullmatch(value) is None:
        if not isinstance(value, str):
            shown = f"of type {type(value).__name__}"
        else:
            shown = repr(value) if len(value) <= 80 else f"of {len(value)} characters"
        raise ValueError(
            f"topic {shown} must be ASCII letters, digits, '_', '.' and '-', start with a "
            "letter, and be at most 64 characters"
        )
    return value


def check_streams(value: object) -> tuple[str, ...]:
    """``streams:``: the workflow-stream topics a buyer may read (decisions I31). At most
    :data:`STREAMS_MAX` distinct :func:`check_topic` names, none of them Kontra's own
    (``kontra.``, any case: ``kontra.progress`` is relayed for every job). Returned sorted:
    one spelling per list, so a catalog comparing two lists compares tuples."""
    if not isinstance(value, list | tuple):
        raise ValueError("must be a list of topic names, e.g. [turn_events]")
    items: list[object] = list(value)  # pyright: ignore[reportUnknownArgumentType]
    if len(items) > STREAMS_MAX:
        raise ValueError(f"at most {STREAMS_MAX} topics")
    topics: list[str] = []
    for item in items:
        topic = check_topic(item)
        if topic.lower().startswith(RESERVED_TOPIC_PREFIX):
            raise ValueError(
                f"topic {topic!r} is reserved: kontra.progress is relayed for every job; "
                "name your own topics"
            )
        if topic in topics:
            raise ValueError(f"topic {topic!r} appears twice")
        topics.append(topic)
    return tuple(sorted(topics))


# --------------------------------------------------------------------------- the model


class OfferingSpec(BaseModel):
    """``offering.yaml``, parsed. Durations are :class:`timedelta`, sizes are bytes."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    name: str
    version: str
    description: str
    pricing_hint: str | None = None
    tags: list[str] = Field(default_factory=list[str])
    max_duration: timedelta
    review_window: timedelta = timedelta(hours=24)
    max_concurrent_jobs: int | None = None
    max_output_bytes: int = 1000**3
    artifact_retention: timedelta = timedelta(days=7)
    uses_hosted_model: bool = False
    #: The topics of the job's workflow stream a buyer may read: catalog data, never in the
    #: contract hash (:meth:`canonical` leaves it out). Sorted.
    streams: tuple[str, ...] = ()

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
        return check_tags(v, canonical=False)

    @field_validator("max_duration", mode="before")
    @classmethod
    def _max_duration(cls, v: object) -> timedelta:
        return check_duration(v, MAX_DURATION_RANGE, canonical=False)

    @field_validator("review_window", mode="before")
    @classmethod
    def _review_window(cls, v: object) -> timedelta:
        return check_duration(v, REVIEW_WINDOW_RANGE, canonical=False)

    @field_validator("artifact_retention", mode="before")
    @classmethod
    def _artifact_retention(cls, v: object) -> timedelta:
        return check_duration(v, ARTIFACT_RETENTION_RANGE, canonical=False)

    @field_validator("max_output_bytes", mode="before")
    @classmethod
    def _max_output_bytes(cls, v: object) -> int:
        return check_size(v, canonical=False)

    @field_validator("max_concurrent_jobs", mode="before")
    @classmethod
    def _max_concurrent_jobs(cls, v: object) -> int | None:
        return check_concurrency(v)

    @field_validator("uses_hosted_model", mode="before")
    @classmethod
    def _uses_hosted_model(cls, v: object) -> bool:
        return check_hosted_model(v)

    @field_validator("streams", mode="before")
    @classmethod
    def _streams(cls, v: object) -> tuple[str, ...]:
        return check_streams(v)

    @property
    def offering_id(self) -> OfferingId:
        return OfferingId.from_parts(self.name, self.version)

    def canonical(self) -> dict[str, Any]:
        """The facts as they appear in ``x-kontra`` (canonical strings, unset omitted).
        ``streams`` is not one of them: it is catalog data, outside the contract hash."""
        out: dict[str, Any] = {
            "name": self.name,
            "version": self.version,
            "description": self.description,
        }
        if self.pricing_hint is not None:
            out["pricing_hint"] = self.pricing_hint
        out["tags"] = list(self.tags)
        out["max_duration"] = format_duration(self.max_duration)
        out["review_window"] = format_duration(self.review_window)
        if self.max_concurrent_jobs is not None:
            out["max_concurrent_jobs"] = self.max_concurrent_jobs
        out["max_output_bytes"] = format_size(self.max_output_bytes)
        out["artifact_retention"] = format_duration(self.artifact_retention)
        if self.uses_hosted_model:  # false is absent: one spelling per fact
            out["uses_hosted_model"] = True
        return out


def _message(err: Mapping[str, Any]) -> str:
    key = ".".join(str(p) for p in err.get("loc", ())) or "offering"
    kind = err.get("type")
    if kind == "extra_forbidden":
        if key in _CODE_KEYS:
            return (
                f"unknown key {key!r}: offering.yaml never names code; bind it in worker.py "
                '(offering("offering.yaml", workflow=..., quote=...))'
            )
        return f"unknown key {key!r}; {_KEYS_HINT}"
    if kind == "missing":
        return "is required"
    msg = str(err.get("msg", "is invalid"))
    return msg.removeprefix("Value error, ")


def parse_offering(data: object, *, source: str | None = None) -> OfferingSpec:
    """Validate an already-parsed ``offering.yaml`` mapping."""
    if not isinstance(data, Mapping):
        raise OfferingError("offering", "must be a mapping of keys to values", source=source)
    try:
        return OfferingSpec.model_validate(data)
    except ValidationError as exc:
        errors = [
            (".".join(str(p) for p in e["loc"]) or "offering", _message(e))  # type: ignore[arg-type]
            for e in exc.errors()
        ]
        path, message = errors[0]
        raise OfferingError(path, message, source=source, errors=errors) from None


def load_offering(path: str | Path) -> OfferingSpec:
    """Load and validate ``offering.yaml``. Unknown keys and import paths are refused."""
    from kontra.contract.errors import ContractError
    from kontra.contract.yaml_io import load_yaml_file

    p = Path(path)
    try:
        data = load_yaml_file(p)
    except ContractError as exc:
        raise OfferingError(exc.path, exc.message, source=str(p)) from None
    return parse_offering(data, source=str(p))
