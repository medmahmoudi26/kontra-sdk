"""Producing a contract document: types-first (derive) and schema-first (merge).

Types-first (§11.1): the workflow's input and output pydantic models become JSON Schema
2020-12 through pydantic's own ``models_json_schema``, then are *lowered* to nexgen's
strict subset (decisions I2):

* ``Optional[T]`` becomes ``oneOf: [T, {type: null}]`` (pydantic writes ``anyOf``);
  ``default: null`` is dropped (it is the absence of a default, and nexgen refuses it);
* every model is closed (``additionalProperties: false``) whatever its pydantic ``extra``
  setting -- a seller cannot open the contract (§11.3);
* ``dict[str, T]`` stays a typed map; ``kontra.types.Artifact`` becomes the reserved
  ``KontraArtifact`` with the body from ``spec/``;
* auto-generated titles (``"Urls"``) are dropped and ``required`` is sorted, so the hash
  moves only when the contract does;
* an enum or a constrained scalar pydantic names in ``$defs`` is written out where it is
  used, its use site's constraints *conjoined* with its own, never overriding them
  (:func:`inline_non_object_defs`);
* what nexgen cannot lower identically in four languages (other unions, tuples, untyped
  ``dict``/``Any``, unsupported formats) is refused with a fix-it naming the field.

Schema-first: a hand-written Nexus document is validated, gains the reserved definitions
it lacks and the ``x-kontra`` facts, and is otherwise emitted unchanged.
"""

from __future__ import annotations

import copy
import math
from collections.abc import Mapping
from typing import Any, cast

from pydantic import BaseModel, ValidationError
from pydantic.json_schema import (
    CoreModeRef,
    DefsRef,
    GenerateJsonSchema,
    JsonSchemaValue,
    models_json_schema,
)
from pydantic_core import core_schema

from kontra.contract import spec as _spec
from kontra.contract.canonical import canonical_json, seal
from kontra.contract.errors import ROOT, ContractError, join_path
from kontra.contract.facts import XKontra
from kontra.contract.offering import OfferingSpec
from kontra.contract.validate import DIALECT, NEXUSRPC, iter_refs, validate_document
from kontra.types import Artifact

__all__ = [
    "EXECUTE_DESCRIPTION",
    "QUOTE_DESCRIPTION",
    "derive_document",
    "inline_non_object_defs",
    "lower_schema",
    "merge_document",
    "nexgen_view",
]

QUOTE_DESCRIPTION = (
    "Price this input. Cheap and synchronous: answers ok with a fixed price, rejected with "
    "a reason, or busy with an ETA. Nothing runs and nothing is charged."
)
EXECUTE_DESCRIPTION = (
    "Run the job on this input. Requires a Kontra hold token in the kontra-hold header; "
    "the result is delivered encrypted to the buyer and paid from escrow."
)

_ARTIFACT_REF_PREFIX = f"{Artifact.__module__}.{Artifact.__qualname__}:"
_ARTIFACT = "KontraArtifact"


class _Generator(GenerateJsonSchema):
    """pydantic's generator, minus the noise and with the reserved names pinned."""

    def field_title_should_be_set(self, schema: Any) -> bool:
        return False

    def _drop_class_title(self, js: JsonSchemaValue, cls: type[Any]) -> JsonSchemaValue:
        config = getattr(cls, "model_config", {})
        if "title" not in config and js.get("title") == cls.__name__:
            js.pop("title")
        return js

    def model_schema(self, schema: core_schema.ModelSchema) -> JsonSchemaValue:
        return self._drop_class_title(super().model_schema(schema), schema["cls"])

    def dataclass_schema(self, schema: core_schema.DataclassSchema) -> JsonSchemaValue:
        return self._drop_class_title(super().dataclass_schema(schema), schema["cls"])

    def enum_schema(self, schema: core_schema.EnumSchema) -> JsonSchemaValue:
        return self._drop_class_title(super().enum_schema(schema), schema["cls"])

    def typed_dict_schema(self, schema: core_schema.TypedDictSchema) -> JsonSchemaValue:
        js = super().typed_dict_schema(schema)
        cls = schema.get("cls")
        return js if cls is None else self._drop_class_title(js, cls)

    def get_defs_ref(self, core_mode_ref: CoreModeRef) -> DefsRef:
        ref = super().get_defs_ref(core_mode_ref)
        core_ref, _mode = core_mode_ref
        if core_ref.startswith(_ARTIFACT_REF_PREFIX) and core_ref.endswith(f":{id(Artifact)}"):
            self._prioritized_defsref_choices[ref] = [DefsRef(_ARTIFACT), ref]
        return ref


# --------------------------------------------------------------------------- lowering


def _is_null(schema: Any) -> bool:
    return isinstance(schema, dict) and schema == {"type": "null"}


def lower_schema(node: Any, path: str) -> Any:
    """One pydantic schema node, lowered to nexgen's subset (see the module docstring).
    Refusals name ``path``. Pure: returns a new value."""
    if not isinstance(node, dict):
        return node
    src = cast(dict[str, Any], node)
    if not src:
        raise ContractError(
            path,
            "accepts anything (Any, or a bare list/dict); give it a type, e.g. list[str] or dict[str, int]",
        )
    out: dict[str, Any] = {}
    for key, value in src.items():
        if key == "anyOf":
            branches = cast(list[Any], value)
            nulls = [b for b in branches if _is_null(b)]
            if len(branches) == 2 and len(nulls) == 1:
                out["oneOf"] = [
                    lower_schema(b, join_path(path, "oneOf", i)) for i, b in enumerate(branches)
                ]
                continue
            raise ContractError(
                path,
                "is a union (anyOf); nexgen lowers only Optional[T]. Use one type, or a tagged "
                "union of models (Annotated[A | B, Field(discriminator=...)])"
                + (
                    " -- Decimal is number|string: use float or str"
                    if _looks_decimal(branches)
                    else ""
                ),
            )
        if key == "prefixItems":
            raise ContractError(path, "is a tuple (prefixItems); use a model or list[T]")
        if key == "discriminator":
            continue  # OpenAPI; the branches' const tags select the branch
        if key == "default" and (value is None or isinstance(value, dict | list)):
            # null is no default at all; nexgen materialises only scalar defaults, and an
            # absent optional field already reads back as empty in every target.
            continue
        if key in ("properties", "patternProperties", "dependentSchemas"):
            out[key] = {
                name: lower_schema(sub, join_path(path, key, name))
                for name, sub in cast(dict[str, Any], value).items()
            }
        elif key in ("items", "contains", "propertyNames"):
            out[key] = lower_schema(value, join_path(path, key))
        elif key == "additionalProperties":
            out[key] = (
                value if isinstance(value, bool) else lower_schema(value, join_path(path, key))
            )
        elif key in ("oneOf", "allOf"):
            out[key] = [
                lower_schema(b, join_path(path, key, i))
                for i, b in enumerate(cast(list[Any], value))
            ]
        elif key == "required":
            out[key] = sorted(cast(list[str], value))
        else:
            out[key] = copy.deepcopy(value)

    if out.get("type") == "object":
        has_props = "properties" in out
        ap = out.get("additionalProperties")
        if has_props:
            out["additionalProperties"] = False  # a model: closed, always (§11.3)
        elif ap is None or ap is True or ap == {}:
            raise ContractError(
                path,
                "is an untyped dict; give the values a type (dict[str, int]) or use a model",
            )
    if out.get("type") == "array" and "items" not in out:
        raise ContractError(path, "is an untyped list; give the items a type (list[str])")
    if out.get("format") == "binary":
        raise ContractError(
            path,
            "is bytes (format: binary), which nexgen does not support; send a KontraArtifact "
            "(kontra.types.Artifact) for files, or base64 text",
        )
    return out


def _without_docs(schema: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in schema.items() if k not in ("description", "title")}


def _looks_decimal(branches: list[Any]) -> bool:
    kinds = {b.get("type") for b in branches if isinstance(b, dict)}
    return kinds == {"number", "string"}


def _is_named_type(body: Mapping[str, Any]) -> bool:
    """What nexgen accepts as a top-level definition: an object, a oneOf union, or a
    bare ``$ref`` alias."""
    if body.get("type") == "object" or "oneOf" in body:
        return True
    return "$ref" in body and not (set(body) - {"$ref", "description", "title", "$comment"})


#: A use site's annotations replace its definition's (a field's own ``description``, its
#: ``default``): they say what the value means, not which values are valid.
_REF_ANNOTATIONS = frozenset(
    {
        "title",
        "description",
        "examples",
        "default",
        "$comment",
        "deprecated",
        "readOnly",
        "writeOnly",
    }
)
#: Bounds both sides write: the conjunction is the tighter of the two.
_UPPER_BOUNDS = frozenset(
    {"maximum", "exclusiveMaximum", "maxLength", "maxItems", "maxContains", "maxProperties"}
)
_LOWER_BOUNDS = frozenset(
    {"minimum", "exclusiveMinimum", "minLength", "minItems", "minContains", "minProperties"}
)


def _same(a: Any, b: Any) -> bool:
    """Equal as JSON values: ``1`` is ``1.0``, ``true`` is not ``1``."""
    return canonical_json(a) == canonical_json(b)


def _is_number(value: Any) -> bool:
    return isinstance(value, int | float) and not isinstance(value, bool)


def _is_positive_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value > 0


def _conjoin(key: str, defined: Any, used: Any, *, name: str, path: str) -> Any:
    """One keyword both a definition (``name``) and its use site (``path``) assert, as one
    value that asserts exactly what the two do; or a refusal naming both values."""
    if _same(defined, used):
        return copy.deepcopy(defined)
    unsatisfiable = ContractError(
        path,
        f"no value satisfies both: {name} says {key} {defined!r} and this field says {key} "
        f"{used!r}, and a $ref and its siblings both hold. Fix one of the two",
    )
    if key in _UPPER_BOUNDS and _is_number(defined) and _is_number(used):
        return min(defined, used)
    if key in _LOWER_BOUNDS and _is_number(defined) and _is_number(used):
        return max(defined, used)
    if key == "type" and isinstance(defined, str) and isinstance(used, str):
        if {defined, used} == {"integer", "number"}:
            return "integer"
        raise unsatisfiable
    if key == "enum" and isinstance(defined, list) and isinstance(used, list):
        both = [
            copy.deepcopy(v)
            for v in cast(list[Any], defined)
            if any(_same(v, u) for u in cast(list[Any], used))
        ]
        if not both:
            raise unsatisfiable
        return both  # in the definition's order
    if key == "const":
        raise unsatisfiable
    if key == "multipleOf" and _is_positive_int(defined) and _is_positive_int(used):
        return math.lcm(cast(int, defined), cast(int, used))
    if key == "uniqueItems" and isinstance(defined, bool) and isinstance(used, bool):
        return True  # one of them is true, and false asserts nothing
    raise ContractError(
        path,
        f"re-constrains {name}: {name} says {key} {defined!r} and this field says {key} "
        f"{used!r}. Both hold (a $ref and its siblings are conjoined), and nexgen's subset "
        f"cannot write the two as one; constrain it in one place, on {name} or on the field",
    )


def _write_out(
    name: str, use: Mapping[str, Any], body: Mapping[str, Any], path: str
) -> dict[str, Any]:
    """The definition ``name`` written out at a use site (``use``, its ``$ref`` and
    siblings): the use site's annotations replace the definition's, every other keyword
    both write is conjoined (:func:`_conjoin`), and the rest of each is kept."""
    merged = {k: copy.deepcopy(v) for k, v in body.items() if k != "title"}
    for key, value in use.items():
        if key == "$ref":
            continue
        if key in _REF_ANNOTATIONS or key not in merged:
            merged[key] = value
        else:
            merged[key] = _conjoin(key, merged[key], value, name=name, path=path)
    return merged


def _substitute(
    node: Any, inline: Mapping[str, dict[str, Any]], path: str, chain: tuple[str, ...] = ()
) -> Any:
    if isinstance(node, list):
        return [
            _substitute(v, inline, join_path(path, i), chain)
            for i, v in enumerate(cast(list[Any], node))
        ]
    if not isinstance(node, dict):
        return node
    src = cast(dict[str, Any], node)
    out = {k: _substitute(v, inline, join_path(path, k), chain) for k, v in src.items()}
    ref = src.get("$ref")
    if isinstance(ref, str) and ref.startswith("#/$defs/") and ref[8:] in inline:
        name = ref[8:]
        if name in chain:
            raise ContractError(
                path,
                f"{name} refers to itself ({' -> '.join((*chain, name))}) and is not a model, so "
                "it cannot be written out where it is used; make it a model with a field",
            )
        body = _substitute(inline[name], inline, join_path("$defs", name), (*chain, name))
        return _write_out(name, out, body, path)
    return out


def inline_non_object_defs(defs: Mapping[str, Any]) -> dict[str, Any]:
    """nexgen names only objects and unions; an enum or a constrained scalar that pydantic
    put in ``$defs`` (``class Color(Enum)``, a ``RootModel`` of a string) is written out
    where it is used instead. JSON Schema asserts a ``$ref``'s definition and the members
    beside it both, so the result must assert what the two did (decisions I38): the use
    site's annotations (a field's own ``description`` or ``default``) replace the
    definition's; a keyword both constrain is conjoined -- the tighter bound, the common
    ``enum`` values, ``integer`` within ``number``, the least common ``multipleOf`` -- and
    one that cannot be written as one value (two different patterns, an empty ``enum``
    intersection) is refused, naming the field and both values, rather than published
    looser than the seller's model validates."""
    inline = {n: cast(dict[str, Any], b) for n, b in defs.items() if not _is_named_type(b)}
    return {
        n: _substitute(b, inline, join_path("$defs", n)) for n, b in defs.items() if n not in inline
    }


# --------------------------------------------------------------------------- derive


def _facts(spec: OfferingSpec) -> dict[str, Any]:
    try:
        return XKontra.from_offering(spec).to_block()
    except ValidationError as exc:
        first = exc.errors()[0]
        raise ContractError(
            join_path("x-kontra", *first["loc"]), str(first["msg"]).removeprefix("Value error, ")
        ) from None


def _model_class(tp: Any, side: str) -> type[BaseModel]:
    if isinstance(tp, type) and issubclass(tp, BaseModel):
        return tp
    name = getattr(tp, "__name__", repr(tp))
    raise ContractError(
        f"execute.{side}",
        f"{name} is not a pydantic model; a Nexus {side} must be an object. Wrap it: "
        f"class My{side.capitalize()}(BaseModel): value: {name}",
    )


def _service(spec: OfferingSpec, input_name: str, output_name: str) -> dict[str, Any]:
    oid = spec.offering_id

    def ref(name: str) -> dict[str, str]:
        return {"$ref": f"#/$defs/{name}"}

    return {
        oid.service_key: {
            "fqn": oid.service_name,
            "description": spec.description,
            "operations": {
                "quote": {
                    "fqn": "quote",
                    "description": QUOTE_DESCRIPTION,
                    "input": ref(input_name),
                    "output": ref("KontraQuote"),
                },
                "execute": {
                    "fqn": "execute",
                    "description": EXECUTE_DESCRIPTION,
                    "input": ref(input_name),
                    "output": ref(output_name),
                },
            },
        }
    }


def derive_document(
    spec: OfferingSpec, input_type: type[Any], output_type: type[Any]
) -> dict[str, Any]:
    """Types-first: the sealed contract document for a workflow ``run(input) -> output``.
    Deterministic: the same models and offering always give the same bytes and hash."""
    inp = _model_class(input_type, "input")
    out = _model_class(output_type, "output")
    try:
        key_map, generated = models_json_schema(
            [(inp, "validation"), (out, "validation")],
            ref_template="#/$defs/{model}",
            schema_generator=_Generator,
        )
    except Exception as exc:  # pydantic refuses some types outright (e.g. callables)
        raise ContractError("$defs", f"pydantic cannot describe these types: {exc}") from exc

    raw_defs = cast(dict[str, Any], generated.get("$defs", {}))
    names = {
        side: cast(str, key_map[(tp, "validation")]["$ref"]).rsplit("/", 1)[1]
        for side, tp in (("input", inp), ("output", out))
    }
    defs: dict[str, Any] = {}
    for name in sorted(raw_defs):
        path = join_path("$defs", name)
        if name == _ARTIFACT:
            # kontra.types.Artifact: the spec's body replaces pydantic's. They must agree
            # field for field, or this is a seller type that merely took the name.
            if _without_docs(lower_schema(raw_defs[name], path)) != _without_docs(
                _spec.reserved_def(_ARTIFACT)
            ):
                raise ContractError(
                    path,
                    f"{_ARTIFACT} is reserved for kontra.types.Artifact, and this type differs "
                    "from spec/; use kontra.types.Artifact or rename the type",
                )
            continue
        if name.startswith(_spec.RESERVED_PREFIX):
            raise ContractError(
                path,
                f"type names starting with {_spec.RESERVED_PREFIX} are reserved; rename {name}",
            )
        defs[name] = lower_schema(raw_defs[name], path)
    for side in ("input", "output"):
        body = defs.get(names[side]) or _spec.reserved_def(names[side])
        if body.get("type") != "object":
            raise ContractError(
                f"execute.{side}",
                f"{names[side]} is a {body.get('type', 'non-object')}; a Nexus {side} must be an "
                "object (a model with fields, not a RootModel of a list or scalar)",
            )
    defs = inline_non_object_defs(defs)
    if _ARTIFACT in raw_defs:
        defs[_ARTIFACT] = _spec.reserved_def(_ARTIFACT)
    defs["KontraQuote"] = _spec.reserved_def("KontraQuote")

    doc: dict[str, Any] = {
        "nexusrpc": NEXUSRPC,
        "$schema": DIALECT,
        "services": _service(spec, names["input"], names["output"]),
        "$defs": {k: defs[k] for k in sorted(defs)},
        "x-kontra": _facts(spec),
    }
    sealed = seal(doc)
    validate_document(sealed)  # derivation must only ever emit what the catalog accepts
    return sealed


# --------------------------------------------------------------------------- merge

_HAND_KEYS = ("nexusrpc", "$schema", "services", "$defs")


def merge_document(spec: OfferingSpec, nexus_doc: Mapping[str, Any]) -> dict[str, Any]:
    """Schema-first: validate a hand-written Nexus document against the catalog rules and
    merge the offering facts into ``x-kontra``. Reserved ``Kontra*`` definitions it uses
    but lacks are added from ``spec/``; nothing else is changed."""
    if not isinstance(nexus_doc, Mapping):  # pyright: ignore[reportUnnecessaryIsInstance]
        raise ContractError(ROOT, "a Nexus document is a JSON object")
    if "x-kontra" in nexus_doc:
        raise ContractError(
            "x-kontra",
            "is written by kontra publish from offering.yaml; remove it from the hand-written document",
        )
    for key in nexus_doc:
        if key not in _HAND_KEYS:
            raise ContractError(
                join_path(ROOT, key),
                f"unknown top-level member; a Nexus document here has {', '.join(_HAND_KEYS)}"
                + (" (offering text belongs in offering.yaml)" if key == "description" else ""),
            )
    doc: dict[str, Any] = copy.deepcopy(dict(nexus_doc))
    doc.setdefault("$schema", DIALECT)
    defs = doc.get("$defs")
    if isinstance(defs, dict):
        typed_defs = cast(dict[str, Any], defs)
        used = set(iter_refs(doc))
        for name in sorted(_spec.reserved_names()):
            if name not in typed_defs and f"#/$defs/{name}" in used:
                typed_defs[name] = _spec.reserved_def(name)
    doc["x-kontra"] = _facts(spec)
    ordered = {k: doc[k] for k in (*_HAND_KEYS, "x-kontra") if k in doc}
    sealed = seal(ordered)
    validate_document(sealed)
    return sealed


def nexgen_view(doc: Mapping[str, Any]) -> dict[str, Any]:
    """The document nexgen accepts: without ``x-kontra``, which nexgen 0.2.5 refuses as an
    unknown root keyword (decisions I2). Verify the hash *before* taking this view."""
    return {k: copy.deepcopy(v) for k, v in doc.items() if k != "x-kontra"}
