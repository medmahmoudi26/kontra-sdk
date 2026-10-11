"""The seller's output, as the buyer reads it: a :class:`Record`.

A delivery is JSON shaped by the offering's published output schema. :func:`wrap` walks the
value and the schema together and turns every object into a :class:`Record` (attribute and
item access, ``to_dict()`` for plain JSON) and every value the schema types as
``#/$defs/KontraArtifact`` into a :class:`kontra.types.Artifact`, so
``await result.records.download(path)`` works without the buyer declaring any type.
Artifacts are found by the schema, never by a value's shape: a seller cannot make a field
the contract calls a plain object into a download.

This module runs inside workflow code (``service().execute`` returns a Record): it imports
only the standard library, pydantic and :mod:`kontra.types`, and it is deterministic.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping
from typing import Any, cast

from pydantic import GetCoreSchemaHandler, ValidationError
from pydantic_core import core_schema

from kontra.hire._errors import InvalidDelivery
from kontra.types import Artifact

__all__ = ["ARTIFACT_REF", "Record", "to_json", "wrap"]

#: The reserved definition every contract uses for an artifact (decisions I2).
ARTIFACT_REF = "#/$defs/KontraArtifact"
_DEFS_PREFIX = "#/$defs/"
#: Schemas are acyclic (the catalog refuses reference loops), but a value is walked by
#: following references: stop following them past this depth rather than recurse forever.
_MAX_REFS = 64

ArtifactFactory = Callable[[Artifact], Artifact]


class Record(Mapping[str, Any]):
    """A JSON object from a delivery. ``rec.name`` and ``rec["name"]`` are the same field;
    nested objects are Records, arrays are lists, artifacts are
    :class:`~kontra.types.Artifact`. A field named like a mapping method (``items``,
    ``keys``, ``values``, ``get``) is reached with ``rec["items"]``. Read-only."""

    __slots__ = ("_data",)

    def __init__(self, data: Mapping[str, Any] | None = None) -> None:
        object.__setattr__(self, "_data", dict(data or {}))

    def __getattr__(self, name: str) -> Any:
        if name.startswith("__"):
            raise AttributeError(name)
        data: dict[str, Any] = object.__getattribute__(self, "_data")
        try:
            return data[name]
        except KeyError:
            fields = ", ".join(sorted(data)) or "none"
            raise AttributeError(f"this record has no field {name!r} (fields: {fields})") from None

    def __setattr__(self, name: str, value: Any) -> None:
        raise AttributeError("a Record is read-only; use to_dict() for a mutable copy")

    def __reduce__(self) -> tuple[type[Record], tuple[dict[str, Any]]]:
        """Copy and pickle through the constructor (attribute assignment is refused)."""
        return (Record, (self._data,))

    def __getitem__(self, key: str) -> Any:
        return self._data[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._data)

    def __len__(self) -> int:
        return len(self._data)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Record):
            return self.to_dict() == other.to_dict()
        if isinstance(other, Mapping):
            return self.to_dict() == to_json(other)
        return NotImplemented

    __hash__ = None  # type: ignore[assignment]  # mutable-looking mapping: unhashable

    def __repr__(self) -> str:
        return f"Record({self.to_dict()!r})"

    def __dir__(self) -> list[str]:
        return sorted({*super().__dir__(), *(k for k in self._data if k.isidentifier())})

    def to_dict(self) -> dict[str, Any]:
        """Plain JSON (artifacts as their ``{key, size, sha256, content_type}`` objects)."""
        return cast("dict[str, Any]", to_json(self._data))

    @classmethod
    def __get_pydantic_core_schema__(
        cls, source: Any, handler: GetCoreSchemaHandler
    ) -> core_schema.CoreSchema:
        """A Record serialises as its JSON (so a workflow may return one) and validates
        from any mapping."""

        def validate(value: Any) -> Record:
            if isinstance(value, Record):
                return value
            if isinstance(value, Mapping):
                return wrap(value, None)
            raise ValueError("a Record is made from a JSON object")

        return core_schema.no_info_plain_validator_function(
            validate,
            serialization=core_schema.plain_serializer_function_ser_schema(
                lambda r: r.to_dict(), when_used="always"
            ),
        )


def to_json(value: Any) -> Any:
    """Records, artifacts and other pydantic models down to plain JSON values."""
    if isinstance(value, Record):
        value = object.__getattribute__(value, "_data")
    if isinstance(value, Artifact):
        return value.model_dump(mode="json")
    if isinstance(value, Mapping):
        items = cast("Mapping[Any, Any]", value).items()
        return {str(k): to_json(v) for k, v in items}
    if isinstance(value, list | tuple):
        return [to_json(v) for v in cast("list[Any] | tuple[Any, ...]", value)]
    if hasattr(value, "model_dump"):
        return to_json(value.model_dump(mode="json", by_alias=True))
    return value


def wrap(
    value: Any,
    schema: Mapping[str, Any] | None,
    *,
    artifact: ArtifactFactory | None = None,
) -> Any:
    """``value`` (JSON) as Records and Artifacts, following ``schema`` (a self-contained
    JSON Schema: the root inline, references under ``$defs``). ``artifact`` may replace each
    Artifact made (the buyer's client binds them to its keys). Without a schema, objects
    still become Records but nothing becomes an Artifact."""
    root: Mapping[str, Any] = schema or {}
    defs_raw = root.get("$defs")
    defs: Mapping[str, Any] = (
        cast("Mapping[str, Any]", defs_raw) if isinstance(defs_raw, Mapping) else {}
    )
    return _Walker(defs, artifact).walk(value, root, 0)


class _Walker:
    def __init__(self, defs: Mapping[str, Any], artifact: ArtifactFactory | None) -> None:
        self._defs = defs
        self._artifact = artifact

    def walk(self, value: Any, schema: Any, refs: int) -> Any:
        node: Mapping[str, Any] = (
            cast("Mapping[str, Any]", schema) if isinstance(schema, Mapping) else {}
        )
        ref = node.get("$ref")
        if isinstance(ref, str):
            if ref == ARTIFACT_REF:
                return self._make_artifact(value)
            target = self._deref(ref)
            if target is not None and refs < _MAX_REFS:
                return self.walk(value, target, refs + 1)
            return self._plain(value)
        branch = self._branch(value, node)
        if branch is not None:
            return self.walk(value, branch, refs)
        if isinstance(value, Mapping):
            obj = cast("Mapping[str, Any]", value)
            props_raw = node.get("properties")
            props: Mapping[str, Any] = (
                cast("Mapping[str, Any]", props_raw) if isinstance(props_raw, Mapping) else {}
            )
            extra = node.get("additionalProperties")
            return Record(
                {str(k): self.walk(v, props.get(str(k), extra), refs) for k, v in obj.items()}
            )
        if isinstance(value, list):
            items = node.get("items")
            return [self.walk(v, items, refs) for v in cast("list[Any]", value)]
        return value

    def _deref(self, ref: str) -> Any:
        if not ref.startswith(_DEFS_PREFIX):
            return None
        return self._defs.get(ref[len(_DEFS_PREFIX) :])

    def _branch(self, value: Any, node: Mapping[str, Any]) -> Any:
        """The ``oneOf``/``allOf`` member that describes ``value`` (an ``Optional[T]`` is
        ``oneOf: [T, {type: null}]``), or None when the node is not a union."""
        one_of = node.get("oneOf")
        if isinstance(one_of, list):
            options = [o for o in cast("list[Any]", one_of) if not _is_null(o)]
            if value is None:
                return None
            if len(options) == 1:
                return options[0]
            for option in options:
                if self._fits(value, option, 0):
                    return option
            return None
        all_of = node.get("allOf")
        if isinstance(all_of, list) and len(cast("list[Any]", all_of)) == 1:
            return cast("list[Any]", all_of)[0]
        return None

    def _fits(self, value: Any, schema: Any, refs: int) -> bool:
        """A cheap structural test for picking a union member (the value was already
        validated against the whole schema where validation runs)."""
        node: Mapping[str, Any] = (
            cast("Mapping[str, Any]", schema) if isinstance(schema, Mapping) else {}
        )
        ref = node.get("$ref")
        if isinstance(ref, str):
            if ref == ARTIFACT_REF:
                return isinstance(value, Mapping) and set(cast("Mapping[str, Any]", value)) == set(
                    Artifact.model_fields
                )
            target = self._deref(ref)
            return target is not None and refs < _MAX_REFS and self._fits(value, target, refs + 1)
        kind = node.get("type")
        if kind == "object":
            if not isinstance(value, Mapping):
                return False
            keys = set(cast("Mapping[str, Any]", value))
            props = node.get("properties")
            known = set(cast("Mapping[str, Any]", props)) if isinstance(props, Mapping) else set()
            required = node.get("required")
            needed = set(cast("list[str]", required)) if isinstance(required, list) else set()
            closed = node.get("additionalProperties") is False
            return needed <= keys and (not closed or keys <= known)
        checks: dict[str, Callable[[Any], bool]] = {
            "array": lambda v: isinstance(v, list),
            "string": lambda v: isinstance(v, str),
            "integer": lambda v: isinstance(v, int) and not isinstance(v, bool),
            "number": lambda v: isinstance(v, int | float) and not isinstance(v, bool),
            "boolean": lambda v: isinstance(v, bool),
            "null": lambda v: v is None,
        }
        check = checks.get(kind) if isinstance(kind, str) else None
        return check(value) if check is not None else True

    def _make_artifact(self, value: Any) -> Any:
        if value is None:
            return None
        try:
            made = value if isinstance(value, Artifact) else Artifact.model_validate(value)
        except ValidationError:
            # A seller's delivery is untrusted input: in workflow code a plain exception
            # would retry the workflow task forever, so this is a (non-retryable) hire error.
            raise InvalidDelivery(
                "the delivery has something other than an artifact where its contract puts one"
            ) from None
        return self._artifact(made) if self._artifact is not None else made

    def _plain(self, value: Any) -> Any:
        return self.walk(value, None, _MAX_REFS)


def _is_null(schema: Any) -> bool:
    return isinstance(schema, Mapping) and cast("Mapping[str, Any]", schema).get("type") == "null"
