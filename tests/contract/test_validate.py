"""The catalog's rules (§11.3, acceptance 19): one test per rule, each naming its path."""

from __future__ import annotations

import copy
from collections.abc import Callable
from typing import Any

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st
from jsonschema import Draft202012Validator

from kontra.contract import ContractError, seal, validate_document
from kontra.contract.validate import MAX_CANONICAL_BYTES

Doc = dict[str, Any]
Mutation = Callable[[Doc], Any]


def _inp(d: Doc) -> Doc:
    body: Doc = d["$defs"]["EnrichInput"]
    return body


def _urls(d: Doc) -> Doc:
    urls: Doc = _inp(d)["properties"]["urls"]
    return urls


def _out(d: Doc) -> Doc:
    body: Doc = d["$defs"]["EnrichOutput"]
    return body


def _ops(d: Doc) -> Doc:
    ops: Doc = d["services"]["AcmeEnrich"]["operations"]
    return ops


def _set(target: Callable[[Doc], Doc], key: str, value: Any) -> Mutation:
    return lambda d: target(d).__setitem__(key, value)


def _del(target: Callable[[Doc], Doc], key: str) -> Mutation:
    return lambda d: target(d).__delitem__(key)


def _items(value: Any) -> Mutation:
    return _set(_urls, "items", value)


def _deep(d: Doc) -> None:
    node: Doc = {"type": "string"}
    for _ in range(40):
        node = {"type": "object", "additionalProperties": False, "properties": {"x": node}}
    _inp(d)["properties"]["deep"] = node


def _many_defs(d: Doc) -> None:
    for i in range(200):
        d["$defs"][f"D{i}"] = {"type": "object", "additionalProperties": False, "properties": {}}


def _rename_service(d: Doc) -> None:
    d["services"]["AcmeEnrichment"] = d["services"].pop("AcmeEnrich")


def _second_service(d: Doc) -> None:
    d["services"]["Other"] = copy.deepcopy(d["services"]["AcmeEnrich"])


#: A well-formed age recipient, for the rule that a contract may not carry one at all.
_EXAMPLE_RECIPIENT = "age1e0q4lyexsefmh6hurumvgl3cmmsfnz4zyfhkd7lfrjcpqc2e250qs6j4el"


def _prop(name: str, schema: Doc | None = None) -> Mutation:
    return lambda d: _inp(d)["properties"].__setitem__(name, schema or {"type": "string"})


def _required_with_default(d: Doc) -> None:
    _inp(d)["properties"]["s"] = {"type": "string", "default": "x"}
    _inp(d)["required"].append("s")


def _alias_loop(d: Doc) -> None:
    d["$defs"]["A"] = {"$ref": "#/$defs/B"}
    d["$defs"]["B"] = {"oneOf": [{"$ref": "#/$defs/A"}, {"type": "null"}]}
    _inp(d)["properties"]["x"] = {"$ref": "#/$defs/A"}


def _allof_loop(d: Doc) -> None:
    d["$defs"]["A"] = {
        "type": "object",
        "additionalProperties": False,
        "properties": {},
        "allOf": [{"$ref": "#/$defs/A"}],
    }
    _inp(d)["properties"]["x"] = {"$ref": "#/$defs/A"}


#: A property name that ends nexgen's Go struct tag (a raw `...` string) and declares
#: init(): a buyer's `go build` of the generated client would run the seller's code.
GO_TAG_ESCAPE = 'x`}; func init() { panic("pwned") }; type Z struct { Q *string `json:"q'
#: An enum value nexgen writes verbatim into a TypeScript template literal: the buyer's
#: client evaluates it whenever a value fails the enum check.
TS_TEMPLATE_ESCAPE = "${globalThis.process.exit(42)}"


E = "$defs.EnrichInput"
U = "$defs.EnrichInput.properties.urls"
OPS = "services.AcmeEnrich.operations"

RULES: list[tuple[str, Mutation, str, str]] = [
    # exact top-level keys
    (
        "extra top-level key",
        lambda d: d.__setitem__("description", "x"),
        "description",
        "unknown top-level",
    ),
    ("missing top-level key", lambda d: d.__delitem__("$schema"), "$schema", "missing"),
    ("nexusrpc version", lambda d: d.__setitem__("nexusrpc", "1.1.0"), "nexusrpc", "1.0.0"),
    (
        "dialect",
        lambda d: d.__setitem__("$schema", "http://json-schema.org/draft-07/schema#"),
        "$schema",
        "2020-12",
    ),
    # one service, named after the offering
    ("two services", _second_service, "services", "exactly one service"),
    ("service key", _rename_service, "services", "AcmeEnrich"),
    (
        "service named by x-kontra",
        lambda d: d["x-kontra"].__setitem__("name", "acme/other"),
        "services",
        "AcmeOther",
    ),
    (
        "service fqn",
        lambda d: d["services"]["AcmeEnrich"].__setitem__("fqn", "acme/enrich"),
        "services.AcmeEnrich.fqn",
        "acme.enrich",
    ),
    (
        "service member",
        lambda d: d["services"]["AcmeEnrich"].__setitem__("deprecated", True),
        "services.AcmeEnrich.deprecated",
        "unknown member",
    ),
    # exactly quote + execute, same input, KontraQuote out
    (
        "extra operation",
        lambda d: _ops(d).__setitem__("status", copy.deepcopy(_ops(d)["quote"])),
        OPS,
        "quote and execute",
    ),
    ("missing operation", lambda d: _ops(d).__delitem__("quote"), OPS, "quote and execute"),
    ("operation fqn", lambda d: _ops(d)["quote"].__delitem__("fqn"), f"{OPS}.quote.fqn", "'Quote'"),
    (
        "operation member",
        lambda d: _ops(d)["quote"].__setitem__("x-go-name", "Price"),
        f"{OPS}.quote.x-go-name",
        "unknown member",
    ),
    (
        "inline operation input",
        lambda d: _ops(d)["execute"].__setitem__("input", {"type": "object"}),
        f"{OPS}.execute.input",
        "$ref",
    ),
    (
        "missing operation output",
        lambda d: _ops(d)["execute"].__delitem__("output"),
        f"{OPS}.execute.output",
        "missing",
    ),
    (
        "quote input differs",
        lambda d: _ops(d)["quote"].__setitem__("input", {"$ref": "#/$defs/EnrichOutput"}),
        f"{OPS}.quote.input",
        "same type",
    ),
    (
        "quote output",
        lambda d: _ops(d)["quote"].__setitem__("output", {"$ref": "#/$defs/EnrichOutput"}),
        f"{OPS}.quote.output",
        "KontraQuote",
    ),
    (
        "non-object operation output",
        lambda d: (
            d["$defs"].__setitem__("Scalar", {"type": "string"}),
            _ops(d)["execute"].__setitem__("output", {"$ref": "#/$defs/Scalar"}),
        ),
        f"{OPS}.execute.output",
        "object type",
    ),
    # references: local and resolvable, never fetched
    (
        "external $ref in a field",
        _items({"$ref": "https://evil.example/s.json"}),
        f"{U}.items.$ref",
        "external reference",
    ),
    (
        "external $ref on an operation",
        lambda d: _ops(d)["execute"].__setitem__(
            "input", {"$ref": "https://evil.example/s.json#/In"}
        ),
        f"{OPS}.execute.input.$ref",
        "not a local reference",
    ),
    (
        "relative file $ref",
        _items({"$ref": "other.yaml#/$defs/X"}),
        f"{U}.items.$ref",
        "external reference",
    ),
    ("unresolvable $ref", _items({"$ref": "#/$defs/Nope"}), f"{U}.items.$ref", "does not resolve"),
    (
        "deep pointer $ref",
        _items({"$ref": "#/$defs/EnrichOutput/properties/summary"}),
        f"{U}.items.$ref",
        "external reference",
    ),
    (
        "$ref with a newline",
        _items({"$ref": "#/$defs/KontraArtifact\n"}),
        f"{U}.items.$ref",
        "external reference",
    ),
    ("$id", _set(_inp, "$id", "https://evil.example/in.json"), f"{E}.$id", "not allowed"),
    ("$anchor", _set(_inp, "$anchor", "in"), f"{E}.$anchor", "not allowed"),
    ("$dynamicRef", _items({"$dynamicRef": "#x"}), f"{U}.items.$dynamicRef", "not allowed"),
    ("nested $defs", _set(_inp, "$defs", {}), f"{E}.$defs", "not allowed"),
    # additionalProperties present and closed on every object
    (
        "additionalProperties unset",
        _del(_inp, "additionalProperties"),
        f"{E}.additionalProperties",
        "unset",
    ),
    (
        "additionalProperties true",
        _set(_out, "additionalProperties", True),
        "$defs.EnrichOutput.additionalProperties",
        "open",
    ),
    (
        "additionalProperties {}",
        lambda d: _out(d)["properties"]["summary"].__setitem__("additionalProperties", {}),
        "$defs.EnrichOutput.properties.summary.additionalProperties",
        "open",
    ),
    (
        "inline object unset",
        _items({"type": "object", "properties": {}}),
        f"{U}.items.additionalProperties",
        "unset",
    ),
    # reserved definitions
    (
        "KontraQuote edited",
        lambda d: d["$defs"]["KontraQuote"]["properties"]["reason"].__setitem__("maxLength", 1001),
        "$defs.KontraQuote",
        "byte for byte",
    ),
    (
        "KontraArtifact edited",
        lambda d: d["$defs"]["KontraArtifact"].__setitem__("description", "mine"),
        "$defs.KontraArtifact",
        "byte for byte",
    ),
    (
        "seller Kontra* def",
        lambda d: d["$defs"].__setitem__(
            "KontraExtra", {"type": "object", "additionalProperties": False}
        ),
        "$defs.KontraExtra",
        "reserved",
    ),
    (
        "def name",
        lambda d: d["$defs"].__setitem__(
            "Enrich-Input", {"type": "object", "additionalProperties": False}
        ),
        "$defs.Enrich-Input",
        "definition name",
    ),
    (
        "service name collision",
        lambda d: d["$defs"].__setitem__(
            "AcmeEnrichClient", {"type": "object", "additionalProperties": False}
        ),
        "$defs.AcmeEnrichClient",
        "collides",
    ),
    (
        "non-object def",
        lambda d: d["$defs"].__setitem__("Color", {"type": "string", "enum": ["red"]}),
        "$defs.Color",
        "inline",
    ),
    # nexgen's subset
    (
        "anyOf",
        lambda d: _out(d)["properties"].__setitem__(
            "summary", {"anyOf": [{"type": "string"}, {"type": "integer"}]}
        ),
        "$defs.EnrichOutput.properties.summary.anyOf",
        "oneOf",
    ),
    ("array type", _items({"type": ["string", "null"]}), f"{U}.items.type", "one string"),
    ("null type alone", _items({"type": "null"}), f"{U}.items.type", "oneOf branch"),
    ("no type", _items({"format": "uri"}), f"{U}.items", "no type"),
    ("boolean schema", _items(True), f"{U}.items", "schema object"),
    (
        "default null",
        lambda d: _urls(d)["items"].__setitem__("default", None),
        f"{U}.items.default",
        "default: null",
    ),
    ("composite default", _set(_urls, "default", []), f"{U}.default", "scalar defaults"),
    (
        "unknown format",
        _items({"type": "string", "format": "phone"}),
        f"{U}.items.format",
        "outside nexgen",
    ),
    (
        "format on a number",
        _items({"type": "integer", "format": "uuid"}),
        f"{U}.items.format",
        'type: "string"',
    ),
    (
        "lookahead",
        _items({"type": "string", "pattern": "^(?=a).*$"}),
        f"{U}.items.pattern",
        "lookaround",
    ),
    (
        "backreference",
        _items({"type": "string", "pattern": "^(a)\\1$"}),
        f"{U}.items.pattern",
        "backreference",
    ),
    (
        "invalid regex",
        _items({"type": "string", "pattern": "("}),
        f"{U}.items.pattern",
        "unclosed",
    ),
    (
        "fractional multipleOf",
        _items({"type": "number", "multipleOf": 0.5}),
        f"{U}.items.multipleOf",
        "positive integer",
    ),
    (
        "enum with null",
        _items({"type": "string", "enum": ["a", None]}),
        f"{U}.items.enum",
        "non-null",
    ),
    ("object const", _items({"type": "string", "const": {"a": 1}}), f"{U}.items.const", "scalar"),
    ("prefixItems", _set(_urls, "prefixItems", [{"type": "string"}]), f"{U}.prefixItems", "tuples"),
    (
        "patternProperties",
        _set(_inp, "patternProperties", {"^x": {"type": "string"}}),
        f"{E}.patternProperties",
        "typed map",
    ),
    (
        "discriminator",
        _set(_inp, "discriminator", {"propertyName": "k"}),
        f"{E}.discriminator",
        "OpenAPI",
    ),
    ("nullable", _items({"type": "string", "nullable": True}), f"{U}.items.nullable", "OpenAPI"),
    ("unknown keyword", _set(_inp, "x-extra", 1), f"{E}.x-extra", "unknown keyword"),
    ("array without items", _del(_urls, "items"), f"{U}.items", "needs items"),
    (
        "single-branch oneOf",
        _items({"oneOf": [{"type": "string"}]}),
        f"{U}.items.oneOf",
        "at least 2",
    ),
    (
        "required undeclared",
        lambda d: _inp(d)["required"].append("nope"),
        f"{E}.required[1]",
        "not declared",
    ),
    ("required twice", lambda d: _inp(d)["required"].append("urls"), f"{E}.required", "twice"),
    (
        "properties without object type",
        _items({"properties": {}, "additionalProperties": False}),
        f"{U}.items",
        "no type",
    ),
    ("metaschema", _set(_urls, "uniqueItems", "yes"), f"{U}.uniqueItems", "JSON Schema 2020-12"),
    # caps
    ("depth", _deep, "$defs.EnrichInput.properties.deep" + ".properties.x" * 14, "deeper than 32"),
    ("size", _set(_inp, "description", "x" * MAX_CANONICAL_BYTES), "$", "canonical form"),
    ("definition count", _many_defs, "$defs", "at most 200"),
    # strings nexgen writes into generated source (code injection into buyer clients)
    (
        "property name escaping a Go struct tag",
        _prop(GO_TAG_ESCAPE),
        f"{E}.properties[" + '"' + GO_TAG_ESCAPE.replace('"', '\\"') + '"]',
        "property name",
    ),
    ("property name with a dot", _prop("a.b"), f'{E}.properties["a.b"]', "property name"),
    ("property name alias", _prop("@type"), f'{E}.properties["@type"]', "property name"),
    ("property name starting with -", _prop("-"), f'{E}.properties["-"]', "property name"),
    (
        "enum escaping a TS template literal",
        _prop("s", {"type": "string", "enum": ["ok", TS_TEMPLATE_ESCAPE]}),
        f"{E}.properties.s.enum[1]",
        "generated source code",
    ),
    (
        "const with a backtick",
        _prop("s", {"type": "string", "const": "a`b"}),
        f"{E}.properties.s.const",
        "generated source code",
    ),
    (
        "enum with whitespace",
        _prop("s", {"type": "string", "enum": ["in progress"]}),
        f"{E}.properties.s.enum[0]",
        "generated source code",
    ),
    (
        "enum listed twice",
        _prop("s", {"type": "string", "enum": ["a", "a"]}),
        f"{E}.properties.s.enum",
        "twice",
    ),
    (
        "language override that is not an identifier",
        _set(_urls, "x-go-name", "URLs;func init(){}"),
        f"{U}.x-go-name",
        "identifier",
    ),
    (
        "dependentRequired name",
        _set(_inp, "dependentRequired", {"urls": ["a`b"]}),
        f"{E}.dependentRequired.urls[0]",
        "property name",
    ),
    # doc comments (nexgen's annotation rules)
    (
        "control character in a description",
        _set(_inp, "description", "a\x00b"),
        f"{E}.description",
        "U+0000",
    ),
    ("empty description", _set(_inp, "description", "  "), f"{E}.description", "empty"),
    ("multi-line title", _set(_inp, "title", "a\nb"), f"{E}.title", "one line"),
    (
        "service description",
        lambda d: d["services"]["AcmeEnrich"].__setitem__("description", "a\x1bb"),
        "services.AcmeEnrich.description",
        "control character",
    ),
    (
        "operation description",
        lambda d: _ops(d)["quote"].__setitem__("description", ""),
        f"{OPS}.quote.description",
        "empty",
    ),
    # literals must satisfy their own node (nexgen refuses the document otherwise)
    (
        "default against its pattern",
        _prop("s", {"type": "string", "pattern": "^h", "default": "bye"}),
        f"{E}.properties.s.default",
        "does not match",
    ),
    (
        "default against its bounds",
        _prop("n", {"type": "integer", "minimum": 10, "default": 5}),
        f"{E}.properties.n.default",
        "minimum",
    ),
    (
        "default against its type",
        _prop("n", {"type": "integer", "default": "5"}),
        f"{E}.properties.n.default",
        "not of type",
    ),
    (
        "enum member against its length",
        _prop("s", {"type": "string", "maxLength": 2, "enum": ["ab", "abc"]}),
        f"{E}.properties.s.enum[1]",
        "too long",
    ),
    (
        "const against its format",
        _prop("s", {"type": "string", "format": "uuid", "const": "nope"}),
        f"{E}.properties.s.const",
        "uuid",
    ),
    (
        "default on a required member",
        _required_with_default,
        f"{E}.properties.s.default",
        "required member",
    ),
    # reference loops that consume nothing (validators recurse until they crash)
    ("$ref/oneOf loop", _alias_loop, "$defs.A", "A -> B -> A"),
    ("allOf self-loop", _allof_loop, "$defs.A", "A -> A"),
    # patterns: one case per family; tests/contract/test_patterns.py has the rest
    (
        "exponential pattern",
        _items({"type": "string", "pattern": "^(a+)+$"}),
        f"{U}.items.pattern",
        "ambiguous repetition",
    ),
    (
        "pattern a Go client cannot compile",
        _items({"type": "string", "pattern": "a{1001}"}),
        f"{U}.items.pattern",
        "panic",
    ),
    (
        "pattern that crashed the old check",
        _items({"type": "string", "pattern": "(?u)a"}),
        f"{U}.items.pattern",
        "inline flag",
    ),
    # x-kontra
    (
        "x-kontra member",
        lambda d: d["x-kontra"].__setitem__("workflow", "EnrichFlow"),
        "x-kontra",
        "Additional properties",
    ),
    (
        "non-canonical duration",
        lambda d: d["x-kontra"].__setitem__("max_duration", "360m"),
        "x-kontra.max_duration",
        "write '6h'",
    ),
    (
        "non-canonical size",
        lambda d: d["x-kontra"].__setitem__("max_output_bytes", "5000MB"),
        "x-kontra.max_output_bytes",
        "write '5GB'",
    ),
    (
        "duration bound",
        lambda d: d["x-kontra"].__setitem__("max_duration", "31d"),
        "x-kontra.max_duration",
        "outside",
    ),
    (
        "unsorted tags",
        lambda d: d["x-kontra"].__setitem__("tags", ["z", "a"]),
        "x-kontra.tags",
        "sorted",
    ),
    (
        # Spec v2 has no public_key member at all, so one is an unknown member rather than
        # a malformed key: additionalProperties is false (decisions I7).
        "a sealed key is refused outright",
        lambda d: d["x-kontra"].__setitem__("public_key", _EXAMPLE_RECIPIENT),
        "x-kontra",
        "public_key",
    ),
    (
        "null member",
        lambda d: d["x-kontra"].__setitem__("pricing_hint", None),
        "x-kontra.pricing_hint",
        "string",
    ),
]


def _ids() -> list[str]:
    return [r[0] for r in RULES]


@pytest.mark.parametrize(("name", "mutate", "path", "fragment"), RULES, ids=_ids())
def test_rule(enrich_doc: Doc, name: str, mutate: Mutation, path: str, fragment: str) -> None:
    doc = copy.deepcopy(enrich_doc)
    mutate(doc)
    if isinstance(doc.get("x-kontra"), dict):
        doc = seal(doc)  # so each rule, not the hash, is what fails
    with pytest.raises(ContractError) as info:
        validate_document(doc)
    assert info.value.path == path, info.value
    assert fragment in info.value.message, info.value


def test_the_derived_document_is_valid(enrich_doc: Doc) -> None:
    info = validate_document(enrich_doc)
    assert info.offering.key == "acme/enrich@1.2.0"
    assert info.facts.max_concurrent_jobs == 10
    assert info.contract_hash == enrich_doc["x-kontra"]["contract_hash"]
    assert info.document == enrich_doc
    assert info.document is not enrich_doc
    assert (info.input_type, info.output_type) == ("EnrichInput", "EnrichOutput")


def test_io_schemas_are_self_contained(enrich_doc: Doc) -> None:
    info = validate_document(enrich_doc)
    assert info.input_schema["type"] == "object"
    assert sorted(info.input_schema["$defs"]) == ["EnrichInput"]
    assert sorted(info.output_schema["$defs"]) == ["EnrichOutput", "KontraArtifact"]
    Draft202012Validator.check_schema(info.output_schema)
    v = Draft202012Validator(info.output_schema)
    good = {
        "records": {"key": "ab" * 16, "size": 1, "sha256": "0" * 64, "content_type": "x"},
        "summary": {"ok": 1},
    }
    assert v.is_valid(good)
    assert not v.is_valid({**good, "extra": 1})


def test_language_overrides_are_allowed(enrich_doc: Doc) -> None:
    doc = copy.deepcopy(enrich_doc)
    _urls(doc)["x-go-name"] = "URLs"
    validate_document(seal(doc))


def test_missing_hash(enrich_doc: Doc) -> None:
    doc = copy.deepcopy(enrich_doc)
    del doc["x-kontra"]["contract_hash"]
    with pytest.raises(ContractError) as info:
        validate_document(doc)
    assert info.value.path == "x-kontra.contract_hash"
    assert "not sealed" in info.value.message


def test_edited_after_sealing(enrich_doc: Doc) -> None:
    doc = copy.deepcopy(enrich_doc)
    _urls(doc)["maxItems"] = 20_000
    with pytest.raises(ContractError) as info:
        validate_document(doc)
    assert info.value.path == "x-kontra.contract_hash"
    assert "edited after sealing" in info.value.message


@pytest.mark.parametrize("value", [[], "doc", None])
def test_not_an_object(value: Any) -> None:
    with pytest.raises(ContractError) as info:
        validate_document(value)
    assert info.value.path == "$"


def test_x_kontra_must_be_an_object(enrich_doc: Doc) -> None:
    doc = copy.deepcopy(enrich_doc)
    doc["x-kontra"] = []
    with pytest.raises(ContractError) as info:
        validate_document(doc)
    assert info.value.path == "x-kontra"


# --------------------------------------------------------------------------- hostile input

_json = st.recursive(
    st.none()
    | st.booleans()
    | st.integers(-(2**53) + 1, 2**53 - 1)
    | st.floats(allow_nan=False, allow_infinity=False)
    | st.text(max_size=8),
    lambda children: (
        st.lists(children, max_size=3) | st.dictionaries(st.text(max_size=8), children, max_size=3)
    ),
    max_leaves=10,
)


def _paths(node: Any, prefix: tuple[Any, ...] = ()) -> list[tuple[Any, ...]]:
    out = [prefix]
    if isinstance(node, dict):
        for k, v in node.items():
            out += _paths(v, (*prefix, k))
    elif isinstance(node, list):
        for i, v in enumerate(node):
            out += _paths(v, (*prefix, i))
    return out


@settings(
    max_examples=300, deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture]
)
@given(data=st.data(), value=_json)
def test_any_mutation_is_accepted_or_refused_with_a_path(
    enrich_doc: Doc, data: st.DataObject, value: Any
) -> None:
    """The catalog takes documents from strangers: whatever they send, validation either
    accepts it or raises ContractError -- never another exception."""
    doc = copy.deepcopy(enrich_doc)
    paths = _paths(doc)[1:]
    target = data.draw(st.sampled_from(paths))
    parent: Any = doc
    for part in target[:-1]:
        parent = parent[part]
    parent[target[-1]] = value
    if isinstance(doc.get("x-kontra"), dict):
        try:
            doc = seal(doc)
        except ContractError:
            return
    try:
        validate_document(doc)
    except ContractError as exc:
        error = exc
    else:
        return
    assert error.path
