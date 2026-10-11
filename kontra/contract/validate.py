"""The catalog's rules for a contract document (PRD §11.1-§11.3, decisions I1/I2).

A document is untrusted input to the catalog and, through the catalog, to every buyer
that generates a client from it. So the rules are exact rather than permissive: exact
top-level keys, exactly one service named after the offering, exactly the two
operations, only local ``#/$defs/<Name>`` references (resolving anything else would be an
SSRF), ``additionalProperties`` closed on every object, reserved ``Kontra*`` definitions
byte-identical to ``spec/``, nexgen's strict subset, size caps, and a matching hash.
Every refusal names the JSON path of the offence.

Two rules exist because a contract is also *source code* on a buyer's machine and a
*regex* in a seller's handler: strings nexgen writes into generated code are limited to
inert characters (it does not escape property names in Go struct tags, nor enum values
in TypeScript template literals), and patterns must pass the gate in
:mod:`kontra.contract.patterns` (no exponential backtracking, nothing a target cannot
compile). A document that loops through ``$ref`` without consuming a value is refused,
because every validator would recurse until it crashed.
"""

from __future__ import annotations

import copy
import re
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from typing import Any

from jsonschema import Draft202012Validator
from pydantic import ValidationError
from referencing import Registry, Resource
from referencing.jsonschema import DRAFT202012

from kontra.contract import spec as _spec
from kontra.contract.canonical import canonical_json, check_json_value, contract_hash
from kontra.contract.errors import ROOT, ContractError, join_path
from kontra.contract.facts import XKontra
from kontra.contract.formats import FORMATS, ContractValidator, format_checker, pattern_problem
from kontra.contract.names import OfferingId

__all__ = [
    "DIALECT",
    "DOCUMENT_KEYS",
    "MAX_CANONICAL_BYTES",
    "MAX_DEFS",
    "MAX_DEPTH",
    "NEXUSRPC",
    "OPERATIONS",
    "ContractInfo",
    "check_schema_subset",
    "def_closure",
    "iter_refs",
    "validate_document",
]

NEXUSRPC = "1.0.0"
DIALECT = "https://json-schema.org/draft/2020-12/schema"
DOCUMENT_KEYS = ("nexusrpc", "$schema", "services", "$defs", "x-kontra")
OPERATIONS = ("quote", "execute")
MAX_DEPTH = 32
MAX_CANONICAL_BYTES = 256 * 1024
MAX_DEFS = 200

_SERVICE_KEYS = frozenset({"fqn", "description", "operations"})
_OPERATION_KEYS = frozenset({"fqn", "description", "input", "output"})
_DEF_NAME_PATTERN = r"[A-Za-z][A-Za-z0-9_]{0,99}"
_DEF_NAME = re.compile(_DEF_NAME_PATTERN, re.ASCII)
_LOCAL_REF = re.compile(rf"#/\$defs/({_DEF_NAME_PATTERN})", re.ASCII)
_TYPES = frozenset({"object", "array", "string", "integer", "number", "boolean", "null"})
_ANNOTATIONS = frozenset({"description", "title", "$comment", "examples", "deprecated", "default"})
_KEYWORDS = _ANNOTATIONS | {
    "type",
    "properties",
    "required",
    "additionalProperties",
    "items",
    "minItems",
    "maxItems",
    "uniqueItems",
    "contains",
    "minContains",
    "maxContains",
    "minLength",
    "maxLength",
    "pattern",
    "format",
    "contentEncoding",
    "minimum",
    "maximum",
    "exclusiveMinimum",
    "exclusiveMaximum",
    "multipleOf",
    "enum",
    "const",
    "oneOf",
    "allOf",
    "$ref",
    "minProperties",
    "maxProperties",
    "propertyNames",
    "dependentRequired",
}
#: nexgen's per-language identifier overrides (``x-go-name`` ...).
_LANG_OVERRIDE = re.compile(r"x-(?:go|ts|py|java)-(?:name|const-name)", re.ASCII)

# Strings nexgen writes into generated source. nexgen 0.2.5 does not escape all of them for
# every context: a property name lands verbatim inside a Go struct tag (a raw `...` string,
# so a backtick ends it and the rest is Go code that runs in init()), and enum/const
# strings land verbatim inside TypeScript template literals (so ${...} runs when a value is
# checked). A seller's contract must never become code on a buyer's machine, so these
# strings are restricted to characters that are inert in every target's string, template,
# raw-string, comment and format-string syntax.
_PROPERTY_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_-]{0,99}", re.ASCII)
_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,99}", re.ASCII)
_CODE_SAFE_LITERAL = re.compile(r"[A-Za-z0-9_.:/@+=!?,;#~^|()\[\]-]{1,128}", re.ASCII)
_CODE_SAFE_HINT = "letters, digits and _ . : / @ + = ! ? , ; # ~ ^ | ( ) [ ] -"
_NO_REMOTE = frozenset(
    {
        "$id",
        "$anchor",
        "$dynamicRef",
        "$dynamicAnchor",
        "$recursiveRef",
        "$recursiveAnchor",
        "$vocabulary",
        "$schema",
        "$defs",
        "definitions",
    }
)
_UNSUPPORTED = {
    "anyOf": "anyOf is outside nexgen's subset; for an optional value write "
    "oneOf: [<schema>, {type: null}], otherwise use one type or a tagged oneOf",
    "not": "not is outside nexgen's subset",
    "if": "if/then/else is outside nexgen's subset",
    "then": "if/then/else is outside nexgen's subset",
    "else": "if/then/else is outside nexgen's subset",
    "dependentSchemas": "dependentSchemas is outside nexgen's subset",
    "prefixItems": "tuples (prefixItems) are outside nexgen's subset; use an object or a list of one type",
    "unevaluatedItems": "unevaluatedItems is outside nexgen's subset",
    "unevaluatedProperties": "unevaluatedProperties is outside nexgen's subset; use additionalProperties",
    "patternProperties": "patternProperties is outside nexgen's subset; use a typed map (additionalProperties: <schema>)",
    "readOnly": "readOnly is outside nexgen's subset",
    "writeOnly": "writeOnly is outside nexgen's subset",
    "contentMediaType": "contentMediaType is outside nexgen's subset; large content is a KontraArtifact",
    "contentSchema": "contentSchema is outside nexgen's subset",
    "discriminator": "discriminator is OpenAPI, not JSON Schema; the oneOf branches' const tags select the branch",
    "nullable": "nullable is OpenAPI, not JSON Schema; write oneOf: [<schema>, {type: null}]",
}


@dataclass(frozen=True, slots=True)
class ContractInfo:
    """What a valid document says, ready to use.

    ``input_schema``/``output_schema`` are self-contained JSON Schemas of the ``execute``
    input and output (the root definition inlined, its closure under ``$defs``), the
    shape MCP tools and form renderers expect.
    """

    offering: OfferingId
    facts: XKontra
    contract_hash: str
    input_schema: dict[str, Any]
    output_schema: dict[str, Any]
    document: dict[str, Any]
    input_type: str
    output_type: str


# --------------------------------------------------------------------------- helpers


def iter_refs(node: Any) -> Iterator[str]:
    """Every ``$ref`` string anywhere under ``node``."""
    stack = [node]
    while stack:
        cur = stack.pop()
        if isinstance(cur, dict):
            ref = cur.get("$ref")  # pyright: ignore[reportUnknownMemberType]
            if isinstance(ref, str):
                yield ref
            stack.extend(cur.values())  # pyright: ignore[reportUnknownMemberType, reportUnknownArgumentType]
        elif isinstance(cur, list):
            stack.extend(cur)  # pyright: ignore[reportUnknownArgumentType]


def def_closure(defs: Mapping[str, Any], root: str) -> list[str]:
    """Names of ``root`` and every definition it reaches, sorted."""
    seen = {root}
    stack = [defs[root]]
    while stack:
        for ref in iter_refs(stack.pop()):
            m = _LOCAL_REF.fullmatch(ref)
            if m and m.group(1) in defs and m.group(1) not in seen:
                seen.add(m.group(1))
                stack.append(defs[m.group(1)])
    return sorted(seen)


def _self_contained(defs: Mapping[str, Any], root: str) -> dict[str, Any]:
    out: dict[str, Any] = {"$schema": DIALECT}
    out.update(copy.deepcopy(defs[root]))
    out["$defs"] = {n: copy.deepcopy(defs[n]) for n in def_closure(defs, root)}
    return out


def _ref_name(value: Any, path: str, defs: Mapping[str, Any]) -> str:
    """The definition an operation's ``input``/``output`` names; only ``{$ref: ...}``."""
    if not isinstance(value, dict) or set(value) != {"$ref"}:  # pyright: ignore[reportUnknownArgumentType]
        raise ContractError(path, 'must be exactly {"$ref": "#/$defs/<Name>"}')
    ref = value["$ref"]
    if not isinstance(ref, str):
        raise ContractError(join_path(path, "$ref"), "must be a string")
    m = _LOCAL_REF.fullmatch(ref)
    if m is None:
        raise ContractError(
            join_path(path, "$ref"),
            f"{ref!r} is not a local reference; only #/$defs/<Name> is allowed "
            "(resolving anything else would make the catalog fetch URLs)",
        )
    if m.group(1) not in defs:
        raise ContractError(
            join_path(path, "$ref"), f"{ref!r} does not resolve: no $defs.{m.group(1)}"
        )
    return m.group(1)


# --------------------------------------------------------------------------- subset


def _is_number(v: Any) -> bool:
    return isinstance(v, int | float) and not isinstance(v, bool)


def _is_count(v: Any) -> bool:
    return isinstance(v, int) and not isinstance(v, bool) and v >= 0


def _is_scalar(v: Any) -> bool:
    return isinstance(v, str | int | float | bool)


def check_schema_subset(
    node: Any, path: str, defs: Mapping[str, Any], *, branch: bool = False
) -> None:
    """Refuse anything nexgen 0.2.5 cannot lower identically in Go, Python, TypeScript and
    Java, and any object that leaves ``additionalProperties`` open or unset."""
    if not isinstance(node, dict):
        raise ContractError(path, "must be a schema object (true, false and other values are not)")
    schema: dict[str, Any] = node  # pyright: ignore[reportUnknownVariableType]
    for key in schema:
        if key in _NO_REMOTE:
            raise ContractError(
                join_path(path, key),
                f"{key} is not allowed: references resolve only to #/$defs/<Name> of this document",
            )
        if key in _UNSUPPORTED:
            raise ContractError(join_path(path, key), _UNSUPPORTED[key])
        if key not in _KEYWORDS and not _LANG_OVERRIDE.fullmatch(key):
            raise ContractError(
                join_path(path, key), f"unknown keyword {key!r} (not in nexgen's subset)"
            )
        if _LANG_OVERRIDE.fullmatch(key) and not (
            isinstance(schema[key], str) and _IDENTIFIER.fullmatch(schema[key])
        ):
            raise ContractError(
                join_path(path, key),
                "must be an identifier: a letter or _, then letters, digits or _",
            )
    _check_annotations(schema, path)

    typ = schema.get("type")
    if typ is not None:
        if not isinstance(typ, str):
            raise ContractError(
                join_path(path, "type"),
                "must be one string; for a nullable value write oneOf: [<schema>, {type: null}]",
            )
        if typ not in _TYPES:
            raise ContractError(join_path(path, "type"), f"unknown type {typ!r}")
        if typ == "null" and not (branch and set(schema) == {"type"}):
            raise ContractError(
                join_path(path, "type"),
                'null is only allowed as a bare oneOf branch: oneOf: [<schema>, {type: "null"}]',
            )
    elif not ({"$ref", "oneOf", "allOf"} & schema.keys()):
        raise ContractError(
            path, "has no type (a schema that accepts anything is outside nexgen's subset)"
        )

    if "default" in schema:
        if schema["default"] is None:
            raise ContractError(
                join_path(path, "default"),
                "default: null is outside nexgen's subset; leave the default out",
            )
        if not _is_scalar(schema["default"]):
            raise ContractError(
                join_path(path, "default"),
                "only scalar defaults are in nexgen's subset; leave this one out",
            )

    if "$ref" in schema:
        ref = schema["$ref"]
        rpath = join_path(path, "$ref")
        if not isinstance(ref, str):
            raise ContractError(rpath, "must be a string")
        m = _LOCAL_REF.fullmatch(ref)
        if m is None:
            raise ContractError(
                rpath,
                f"{ref!r} is an external reference; only #/$defs/<Name> is allowed "
                "(resolving anything else would make the catalog fetch URLs)",
            )
        if m.group(1) not in defs:
            raise ContractError(rpath, f"{ref!r} does not resolve: no $defs.{m.group(1)}")

    is_object = typ == "object" or "properties" in schema
    if "properties" in schema and typ != "object":
        raise ContractError(join_path(path, "type"), 'properties needs type: "object"')
    if is_object:
        if "additionalProperties" not in schema:
            raise ContractError(
                join_path(path, "additionalProperties"),
                "is unset; every object must set additionalProperties: false "
                "(or a schema, for a typed map)",
            )
        ap = schema["additionalProperties"]
        if ap is True or ap == {}:
            raise ContractError(
                join_path(path, "additionalProperties"),
                "must not be open (true or {}); set false, or a schema for a typed map",
            )
    if "additionalProperties" in schema:
        ap = schema["additionalProperties"]
        if typ != "object":
            raise ContractError(join_path(path, "additionalProperties"), 'needs type: "object"')
        if ap is not False:
            check_schema_subset(ap, join_path(path, "additionalProperties"), defs)

    props = schema.get("properties")
    if props is not None:
        if not isinstance(props, dict):
            raise ContractError(join_path(path, "properties"), "must be an object")
        for name, sub in props.items():  # pyright: ignore[reportUnknownVariableType]
            ppath = join_path(path, "properties", name)  # pyright: ignore[reportUnknownArgumentType]
            _check_property_name(name, ppath)
            check_schema_subset(sub, ppath, defs)
    if "required" in schema:
        req = schema["required"]
        rpath = join_path(path, "required")
        if not isinstance(req, list) or not all(isinstance(r, str) for r in req):  # pyright: ignore[reportUnknownVariableType]
            raise ContractError(rpath, "must be a list of property names")
        if len(set(req)) != len(req):  # pyright: ignore[reportUnknownArgumentType]
            raise ContractError(rpath, "names a property twice")
        declared: dict[str, Any] = props if isinstance(props, dict) else {}  # pyright: ignore[reportUnknownVariableType]
        for i, r in enumerate(req):  # pyright: ignore[reportUnknownVariableType, reportUnknownArgumentType]
            if r not in declared:
                raise ContractError(
                    join_path(rpath, i), f"{r!r} is required but not declared in properties"
                )
            if isinstance(declared[r], dict) and "default" in declared[r]:
                raise ContractError(
                    join_path(path, "properties", r, "default"),  # pyright: ignore[reportUnknownArgumentType]
                    "a required member never uses its default (nexgen refuses it); make the "
                    "member optional or drop the default",
                )
    if "dependentRequired" in schema:
        _check_dependent_required(schema["dependentRequired"], join_path(path, "dependentRequired"))

    if typ == "array" and "items" not in schema:
        raise ContractError(join_path(path, "items"), "is unset; an array needs items: <schema>")
    for key in ("items", "contains", "propertyNames"):
        if key in schema:
            check_schema_subset(schema[key], join_path(path, key), defs)

    for key in ("oneOf", "allOf"):
        if key in schema:
            branches = schema[key]
            bpath = join_path(path, key)
            minimum = 2 if key == "oneOf" else 1
            if not isinstance(branches, list) or len(branches) < minimum:  # pyright: ignore[reportUnknownArgumentType]
                raise ContractError(bpath, f"must be a list of at least {minimum} schemas")
            for i, sub in enumerate(branches):  # pyright: ignore[reportUnknownVariableType, reportUnknownArgumentType]
                check_schema_subset(sub, join_path(bpath, i), defs, branch=key == "oneOf")

    if "format" in schema:
        fmt = schema["format"]
        if typ != "string":
            raise ContractError(join_path(path, "format"), 'format needs type: "string"')
        if not isinstance(fmt, str) or fmt not in FORMATS:
            raise ContractError(
                join_path(path, "format"),
                f"format {fmt!r} is outside nexgen's subset; use one of {', '.join(sorted(FORMATS))} "
                "(bytes: a string with contentEncoding: base64, or a KontraArtifact)",
            )
    if "pattern" in schema:
        pat = schema["pattern"]
        if not isinstance(pat, str):
            raise ContractError(join_path(path, "pattern"), "must be a string")
        problem = pattern_problem(pat)
        if problem:
            raise ContractError(join_path(path, "pattern"), problem)
    if "contentEncoding" in schema and schema["contentEncoding"] not in ("base64", "base64url"):
        raise ContractError(join_path(path, "contentEncoding"), "must be base64 or base64url")
    if "enum" in schema:
        values = schema["enum"]
        if not isinstance(values, list) or not values or not all(_is_scalar(v) for v in values):  # pyright: ignore[reportUnknownVariableType]
            raise ContractError(
                join_path(path, "enum"), "must be a non-empty list of non-null scalars"
            )
        if len(set(map(_literal_key, values))) != len(values):  # pyright: ignore[reportUnknownArgumentType]
            raise ContractError(join_path(path, "enum"), "lists a value twice")
        for i, v in enumerate(values):  # pyright: ignore[reportUnknownVariableType, reportUnknownArgumentType]
            _check_code_safe_literal(v, join_path(path, "enum", i))
    if "const" in schema:
        if not _is_scalar(schema["const"]):
            raise ContractError(join_path(path, "const"), "must be a non-null scalar")
        _check_code_safe_literal(schema["const"], join_path(path, "const"))
    if "multipleOf" in schema:
        mo = schema["multipleOf"]
        if not (isinstance(mo, int) and not isinstance(mo, bool) and mo > 0):
            raise ContractError(
                join_path(path, "multipleOf"), "must be a positive integer (nexgen's subset)"
            )
    for key in ("minimum", "maximum", "exclusiveMinimum", "exclusiveMaximum"):
        if key in schema and not _is_number(schema[key]):
            raise ContractError(join_path(path, key), "must be a number")
    for key in (
        "minLength",
        "maxLength",
        "minItems",
        "maxItems",
        "minContains",
        "maxContains",
        "minProperties",
        "maxProperties",
    ):
        if key in schema and not _is_count(schema[key]):
            raise ContractError(join_path(path, key), "must be a non-negative integer")
    _check_literals(schema, path)


def _check_property_name(name: Any, path: str) -> None:
    if not (isinstance(name, str) and _PROPERTY_NAME.fullmatch(name)):
        raise ContractError(
            path,
            "a property name is a letter or _, then letters, digits, _ or - (at most 100): "
            "generated clients write it into source code. Rename the field (a pydantic alias "
            "too)",
        )


def _check_dependent_required(value: Any, path: str) -> None:
    if not isinstance(value, dict):
        raise ContractError(path, "must map a property to the properties it requires")
    for trigger, needed in value.items():  # pyright: ignore[reportUnknownVariableType]
        tpath = join_path(path, trigger)  # pyright: ignore[reportUnknownArgumentType]
        _check_property_name(trigger, tpath)
        if not isinstance(needed, list):
            raise ContractError(tpath, "must be a list of property names")
        for i, name in enumerate(needed):  # pyright: ignore[reportUnknownVariableType, reportUnknownArgumentType]
            _check_property_name(name, join_path(tpath, i))


def _literal_key(value: Any) -> tuple[str, Any]:
    """Equality as JSON sees it: ``true`` is not ``1``."""
    return (type(value).__name__ if isinstance(value, bool | str) else "number", value)


def _check_code_safe_literal(value: Any, path: str) -> None:
    if isinstance(value, str) and not _CODE_SAFE_LITERAL.fullmatch(value):
        raise ContractError(
            path,
            f"enum and const strings are written into generated source code, so they may "
            f"use only {_CODE_SAFE_HINT} (1 to 128 characters); {value!r} does not",
        )


def _check_text(value: Any, path: str, *, single_line: bool) -> None:
    """``description``/``title``: nexgen renders them as doc comments and refuses empty
    text and control characters other than newline and tab (and newlines in a title)."""
    if not isinstance(value, str):
        raise ContractError(path, "must be a string")
    if not value.strip():
        raise ContractError(path, "must not be empty or whitespace only; drop it or write text")
    if single_line and ("\n" in value or "\r" in value):
        raise ContractError(path, "must be one line; put longer prose in description")
    bad = next((c for c in value if (c < " " and c not in "\n\t") or c == "\x7f"), None)
    if bad is not None:
        raise ContractError(path, f"must not contain the control character U+{ord(bad):04X}")


_LITERAL_CONSTRAINTS = (
    "type",
    "enum",
    "const",
    "pattern",
    "format",
    "minLength",
    "maxLength",
    "minimum",
    "maximum",
    "exclusiveMinimum",
    "exclusiveMaximum",
    "multipleOf",
)
_FORMAT_CHECKER = format_checker()


def _literal_fits(value: Any, constraints: Mapping[str, Any], path: str) -> None:
    if not constraints:
        return
    validator = ContractValidator(dict(constraints), format_checker=_FORMAT_CHECKER)
    error = next(iter(validator.iter_errors(value)), None)
    if error is not None:
        raise ContractError(
            path,
            f"{value!r} breaks the constraints of its own schema ({error.message}); every "
            "target would refuse it, and nexgen refuses the document",
        )


def _check_literals(schema: Mapping[str, Any], path: str) -> None:
    """A ``default``, ``const`` or ``enum`` value must satisfy the constraints beside it
    (type, pattern, format, bounds, lengths), as nexgen requires."""
    constraints = {k: schema[k] for k in _LITERAL_CONSTRAINTS if k in schema}
    if "default" in schema:
        _literal_fits(schema["default"], constraints, join_path(path, "default"))
    if "const" in schema:
        others = {k: v for k, v in constraints.items() if k != "const"}
        _literal_fits(schema["const"], others, join_path(path, "const"))
    if isinstance(schema.get("enum"), list):
        others = {k: v for k, v in constraints.items() if k != "enum"}
        for i, value in enumerate(schema["enum"]):
            _literal_fits(value, others, join_path(path, "enum", i))


def _same_instance_refs(body: Any) -> set[str]:
    """Definitions ``body`` applies to the *same* value: through ``$ref``, ``oneOf`` and
    ``allOf``, not through keywords that descend into a member or an item."""
    out: set[str] = set()
    stack = [body]
    while stack:
        node = stack.pop()
        if not isinstance(node, dict):
            continue
        ref = node.get("$ref")  # pyright: ignore[reportUnknownMemberType]
        if isinstance(ref, str):
            m = _LOCAL_REF.fullmatch(ref)
            if m:
                out.add(m.group(1))
        for key in ("oneOf", "allOf"):
            branches = node.get(key)  # pyright: ignore[reportUnknownMemberType]
            if isinstance(branches, list):
                stack.extend(branches)  # pyright: ignore[reportUnknownArgumentType]
    return out


def _check_ref_cycles(defs: Mapping[str, Any]) -> None:
    """Refuse a definition that reaches itself without consuming any of the value
    (``A: {$ref: B}``, ``B: {oneOf: [{$ref: A}, ...]}``): every validator, ours included,
    would recurse until it crashes. Recursion through a property or an item (a tree) is
    fine: the value's depth bounds it."""
    graph = {name: sorted(_same_instance_refs(body)) for name, body in defs.items()}
    state: dict[str, int] = {}  # 1 on the current path, 2 done
    for root in sorted(graph):
        if state.get(root):
            continue
        path = [root]
        state[root] = 1
        iters = [iter(graph[root])]
        while iters:
            nxt = next(iters[-1], None)
            if nxt is None:
                state[path.pop()] = 2
                iters.pop()
                continue
            if nxt not in graph or state.get(nxt) == 2:
                continue
            if state.get(nxt) == 1:
                loop = [*path[path.index(nxt) :], nxt]
                raise ContractError(
                    join_path("$defs", nxt),
                    f"refers back to itself without consuming any of the value "
                    f"({' -> '.join(loop)}); a validator would recurse forever. Break the "
                    "loop with a property or an array item",
                )
            state[nxt] = 1
            path.append(nxt)
            iters.append(iter(graph[nxt]))


def _check_annotations(schema: Mapping[str, Any], path: str) -> None:
    if "description" in schema:
        _check_text(schema["description"], join_path(path, "description"), single_line=False)
    if "title" in schema:
        _check_text(schema["title"], join_path(path, "title"), single_line=True)
    if "$comment" in schema and not isinstance(schema["$comment"], str):
        raise ContractError(join_path(path, "$comment"), "must be a string")
    if "deprecated" in schema and not isinstance(schema["deprecated"], bool):
        raise ContractError(join_path(path, "deprecated"), "must be true or false")


# --------------------------------------------------------------------------- the rules


def _check_facts(doc: Mapping[str, Any]) -> XKontra:
    block = doc["x-kontra"]
    if not isinstance(block, dict):
        raise ContractError("x-kontra", "must be an object")
    schema = _spec.x_kontra_schema()
    registry: Registry[Any] = Registry(retrieve=_no_retrieval).with_resource(
        schema["$id"], DRAFT202012.create_resource(schema)
    )
    validator = Draft202012Validator(schema, registry=registry)
    error = next(
        iter(sorted(validator.iter_errors(block), key=lambda e: list(e.absolute_path))), None
    )  # pyright: ignore[reportUnknownArgumentType]
    if error is not None:
        raise ContractError(join_path("x-kontra", *error.absolute_path), error.message)
    try:
        facts = XKontra.model_validate(block)
    except ValidationError as exc:
        first = exc.errors()[0]
        raise ContractError(
            join_path("x-kontra", *first["loc"]),
            str(first["msg"]).removeprefix("Value error, "),
        ) from None
    if facts.contract_hash is None:
        raise ContractError("x-kontra.contract_hash", "is missing: the document is not sealed")
    return facts


def _no_retrieval(uri: str) -> Resource[Any]:
    raise ContractError(ROOT, f"refusing to retrieve {uri!r}: contracts never fetch anything")


def _check_defs(doc: Mapping[str, Any]) -> dict[str, Any]:
    defs = doc["$defs"]
    if not isinstance(defs, dict):
        raise ContractError("$defs", "must be an object")
    if len(defs) > MAX_DEFS:  # pyright: ignore[reportUnknownArgumentType]
        raise ContractError("$defs", f"has {len(defs)} definitions; at most {MAX_DEFS}")  # pyright: ignore[reportUnknownArgumentType]
    reserved = _spec.reserved_names()
    for name, body in defs.items():  # pyright: ignore[reportUnknownVariableType]
        path = join_path("$defs", name)  # pyright: ignore[reportUnknownArgumentType]
        if not _DEF_NAME.fullmatch(name):  # pyright: ignore[reportUnknownArgumentType]
            raise ContractError(
                path, "a definition name is a letter then letters, digits or _ (at most 100)"
            )
        if name.startswith(_spec.RESERVED_PREFIX):  # pyright: ignore[reportUnknownMemberType]
            if name not in reserved:
                raise ContractError(
                    path,
                    f"names starting with {_spec.RESERVED_PREFIX} are reserved for spec/ "
                    f"({', '.join(sorted(reserved))}); rename this type",
                )
            if canonical_json(body) != canonical_json(_spec.reserved_def(name)):  # pyright: ignore[reportUnknownArgumentType]
                raise ContractError(
                    path,
                    f"differs from {name} in spec/{_spec.KONTRA_TYPES_FILE}; reserved "
                    "definitions must be copied byte for byte",
                )
    return defs  # pyright: ignore[reportUnknownVariableType]


def _check_services(
    doc: Mapping[str, Any], offering: OfferingId, defs: Mapping[str, Any]
) -> tuple[str, str]:
    services = doc["services"]
    if not isinstance(services, dict):
        raise ContractError("services", "must be an object")
    key = offering.service_key
    if list(services) != [key]:  # pyright: ignore[reportUnknownArgumentType]
        found = ", ".join(map(str, services)) or "none"  # pyright: ignore[reportUnknownArgumentType]
        raise ContractError(
            "services",
            f"must hold exactly one service, {key} (fqn {offering.service_name}) for "
            f"{offering.full_name}; found {found}",
        )
    spath = join_path("services", key)
    service = services[key]
    if not isinstance(service, dict):
        raise ContractError(spath, "must be an object")
    for k in service:  # pyright: ignore[reportUnknownVariableType]
        if k not in _SERVICE_KEYS:
            raise ContractError(
                join_path(spath, k),
                f"unknown member; a service has {', '.join(sorted(_SERVICE_KEYS))}",
            )  # pyright: ignore[reportUnknownArgumentType]
    if service.get("fqn") != offering.service_name:  # pyright: ignore[reportUnknownMemberType]
        raise ContractError(
            join_path(spath, "fqn"),
            f"must be {offering.service_name!r}, the Nexus service name on the wire "
            "(a / in a service name breaks Temporal's Nexus routing)",
        )
    if "description" in service:
        _check_text(service["description"], join_path(spath, "description"), single_line=False)
    ops = service.get("operations")  # pyright: ignore[reportUnknownMemberType]
    opath = join_path(spath, "operations")
    if not isinstance(ops, dict) or sorted(ops) != sorted(OPERATIONS):  # pyright: ignore[reportUnknownArgumentType]
        raise ContractError(opath, "must be exactly quote and execute")
    refs: dict[str, tuple[str, str]] = {}
    for name in OPERATIONS:
        op = ops[name]
        path = join_path(opath, name)
        if not isinstance(op, dict):
            raise ContractError(path, "must be an object")
        for k in op:  # pyright: ignore[reportUnknownVariableType]
            if k not in _OPERATION_KEYS:
                raise ContractError(
                    join_path(path, k),
                    f"unknown member; an operation has {', '.join(sorted(_OPERATION_KEYS))}",
                )  # pyright: ignore[reportUnknownArgumentType]
        if op.get("fqn") != name:  # pyright: ignore[reportUnknownMemberType]
            raise ContractError(
                join_path(path, "fqn"),
                f"must be {name!r}: nexgen would otherwise name the operation "
                f"{name.capitalize()!r} on the wire, which the handler does not serve",
            )
        if "description" in op:
            _check_text(op["description"], join_path(path, "description"), single_line=False)
        for side in ("input", "output"):
            if side not in op:
                raise ContractError(join_path(path, side), "is missing")
        refs[name] = (
            _ref_name(op["input"], join_path(path, "input"), defs),
            _ref_name(op["output"], join_path(path, "output"), defs),
        )
    if refs["quote"][0] != refs["execute"][0]:
        raise ContractError(
            join_path(opath, "quote", "input"),
            "must be the same type as execute's input: a quote prices the exact input a job runs",
        )
    if refs["quote"][1] != "KontraQuote":
        raise ContractError(
            join_path(opath, "quote", "output"), 'must be {"$ref": "#/$defs/KontraQuote"}'
        )
    for side, name in (("input", refs["execute"][0]), ("output", refs["execute"][1])):
        body = defs[name]
        if not isinstance(body, dict) or body.get("type") != "object":  # pyright: ignore[reportUnknownMemberType]
            raise ContractError(
                join_path(opath, "execute", side),
                f"$defs.{name} must be an object type: a Nexus payload is always an object "
                "(wrap a scalar or list in a model with one field)",
            )
    return refs["execute"]


def _check_reserved_collisions(defs: Mapping[str, Any], offering: OfferingId) -> None:
    """Go output adds ``<Service>Client``/``New<Service>Client`` to the package; a type
    with one of those names would not compile (nexgen refuses it at load)."""
    key = offering.service_key
    for name in (key, f"{key}Client", f"New{key}Client"):
        if name in defs:
            raise ContractError(
                join_path("$defs", name),
                f"collides with the generated service name {name}; rename the type",
            )


def _check_metaschema(defs: Mapping[str, Any]) -> None:
    wrapper = {"$schema": DIALECT, "$defs": dict(defs)}
    error = next(
        iter(Draft202012Validator(Draft202012Validator.META_SCHEMA).iter_errors(wrapper)), None
    )  # pyright: ignore[reportUnknownArgumentType]
    if error is not None:
        raise ContractError(
            join_path(ROOT, *error.absolute_path), f"not valid JSON Schema 2020-12: {error.message}"
        )


def validate_document(doc: Mapping[str, Any]) -> ContractInfo:
    """Apply every catalog rule; return what the document says, or raise
    :class:`ContractError` naming the offending path."""
    if not isinstance(doc, Mapping):  # pyright: ignore[reportUnnecessaryIsInstance]
        raise ContractError(ROOT, "a contract document is a JSON object")
    check_json_value(doc, max_depth=MAX_DEPTH)
    size = len(canonical_json(doc))
    if size > MAX_CANONICAL_BYTES:
        raise ContractError(
            ROOT, f"is {size} bytes in canonical form; at most {MAX_CANONICAL_BYTES}"
        )

    for key in doc:
        if key not in DOCUMENT_KEYS:
            raise ContractError(
                join_path(ROOT, key),
                f"unknown top-level member; a contract has exactly {', '.join(DOCUMENT_KEYS)}",
            )
    for key in DOCUMENT_KEYS:
        if key not in doc:
            raise ContractError(join_path(ROOT, key), "is missing")
    if doc["nexusrpc"] != NEXUSRPC:
        raise ContractError("nexusrpc", f'must be the string "{NEXUSRPC}"')
    if doc["$schema"] != DIALECT:
        raise ContractError("$schema", f"must be {DIALECT!r}")

    facts = _check_facts(doc)
    offering = facts.offering_id
    defs = _check_defs(doc)
    input_type, output_type = _check_services(doc, offering, defs)
    _check_reserved_collisions(defs, offering)
    for name, body in defs.items():
        path = join_path("$defs", name)
        check_schema_subset(body, path, defs)
        if not (body.get("type") == "object" or "oneOf" in body or "$ref" in body):
            raise ContractError(
                path,
                "a definition must be an object, a oneOf union or a $ref alias (nexgen); "
                "write enums and constrained scalars inline where they are used",
            )
    _check_ref_cycles(defs)
    _check_metaschema(defs)

    actual = contract_hash(doc)
    if facts.contract_hash != actual:
        raise ContractError(
            "x-kontra.contract_hash",
            f"is {facts.contract_hash} but the document hashes to {actual}: it was edited "
            "after sealing; republish (or bump the version)",
        )
    document = copy.deepcopy(dict(doc))
    return ContractInfo(
        offering=offering,
        facts=facts,
        contract_hash=actual,
        input_schema=_self_contained(defs, input_type),
        output_schema=_self_contained(defs, output_type),
        document=document,
        input_type=input_type,
        output_type=output_type,
    )
