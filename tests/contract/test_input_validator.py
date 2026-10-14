"""Values against a contract: the handler before it starts a workflow, the buyer before it
encrypts (§11.3). Formats are asserted and patterns have ECMA/RE2 semantics, so Python
accepts exactly what nexgen-generated validators in other languages accept."""

from __future__ import annotations

import copy
import dataclasses
import socket
from typing import Any

import pytest
from pydantic import BaseModel, Field

from kontra.contract import (
    ContractError,
    InputValidator,
    KontraQuote,
    OfferingSpec,
    derive_document,
    seal,
    validate_document,
)
from kontra.contract.formats import check_format, compile_pattern, pattern_problem

from .sample_models import EnrichOutput

SHA = "a" * 64
RECORDS = {
    "key": "0123456789abcdef0123456789abcdef",
    "size": 10,
    "sha256": SHA,
    "content_type": "application/x-ndjson",
}


@pytest.fixture
def validator(enrich_doc: dict[str, Any]) -> InputValidator:
    return InputValidator(enrich_doc)


def test_valid_input(validator: InputValidator) -> None:
    validator.validate({"urls": ["https://acme.com/a", "http://[::1]:8080/x?y=1#z"]})


@pytest.mark.parametrize(
    ("value", "path", "fragment"),
    [
        ({"urls": ["https://a.com"], "x": 1}, "input", "Additional properties"),
        ({}, "input", "'urls' is a required property"),
        ({"urls": []}, "input.urls", "should be non-empty"),
        ({"urls": [1]}, "input.urls[0]", "not of type 'string'"),
        ({"urls": ["not a url"]}, "input.urls[0]", "is not a 'uri'"),
        ({"urls": ["https://exämple.com"]}, "input.urls[0]", "is not a 'uri'"),
        ({"urls": ["https://a.com"] * 10_001}, "input.urls", "too long"),
        ("urls", "input", "not of type 'object'"),
    ],
)
def test_invalid_input_names_the_path(
    validator: InputValidator, value: Any, path: str, fragment: str
) -> None:
    with pytest.raises(ContractError) as info:
        validator.validate(value)
    assert info.value.path == path
    assert fragment in info.value.message


def test_all_violations_are_listed(validator: InputValidator) -> None:
    with pytest.raises(ContractError) as info:
        validator.validate({"urls": ["bad one", 3], "extra": True})
    assert sorted(p for p, _ in info.value.errors) == ["input", "input.urls[0]", "input.urls[1]"]


def test_output(validator: InputValidator) -> None:
    validator.validate_output({"records": RECORDS, "summary": {"ok": 3, "failed": 0}})
    with pytest.raises(ContractError) as info:
        validator.validate_output({"records": {**RECORDS, "sha256": "x"}, "summary": {}})
    assert info.value.path == "output.records.sha256"
    with pytest.raises(ContractError) as info:
        validator.validate_output({"records": RECORDS, "summary": {"not found": "3"}})
    assert info.value.path == 'output.summary["not found"]'


def test_quote(validator: InputValidator) -> None:
    validator.validate_quote(
        KontraQuote(
            status="ok", amount_minor=1, currency="USD", max_duration_seconds=60
        ).model_dump(mode="json")
    )
    with pytest.raises(ContractError) as info:
        validator.validate_quote({"status": "ok", "amount_minor": None})
    assert info.value.path == "quote.amount_minor"


def test_dollar_means_end_of_input(validator: InputValidator) -> None:
    """Python's ``$`` also matches before a final newline; ECMA's and RE2's do not."""
    with pytest.raises(ContractError) as info:
        validator.validate_output(
            {"records": {**RECORDS, "key": "0123456789abcdef0123456789abcdef\n"}, "summary": {}}
        )
    assert info.value.path == "output.records.key"


class Digits(BaseModel):
    code: str = Field(pattern=r"^\d+$")


def test_backslash_d_is_ascii(enrich_spec: OfferingSpec) -> None:
    v = InputValidator(derive_document(enrich_spec, Digits, EnrichOutput))
    v.validate({"code": "0123"})
    with pytest.raises(ContractError):
        v.validate({"code": "٣٤"})  # Arabic-Indic digits: \d in Python, not in RE2


class Node(BaseModel):
    child: Node | None = None
    tag: str = "x"


def test_deep_values_are_refused_not_crashed(enrich_spec: OfferingSpec) -> None:
    v = InputValidator(derive_document(enrich_spec, Node, Node))
    value: dict[str, Any] = {}
    for _ in range(60):
        value = {"child": value}
    v.validate(value)
    for _ in range(1000):
        value = {"child": value}
    with pytest.raises(ContractError) as info:
        v.validate(value)
    assert info.value.path.startswith("input.child.child")
    assert "deeper than 64" in info.value.message


def test_non_json_values_are_refused(validator: InputValidator) -> None:
    with pytest.raises(ContractError) as info:
        validator.validate({"urls": ("https://a.com",)})
    assert info.value.path == "input.urls"
    with pytest.raises(ContractError) as info:
        validator.validate([float("nan")])
    assert info.value.path == "input[0]"


def test_validator_refuses_an_invalid_document(enrich_doc: dict[str, Any]) -> None:
    doc = copy.deepcopy(enrich_doc)
    doc["x-kontra"]["review_window"] = "2d"  # edited after sealing
    with pytest.raises(ContractError) as info:
        InputValidator(doc)
    assert info.value.path == "x-kontra.contract_hash"


def test_nothing_is_ever_fetched(
    enrich_doc: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Even a document that slipped past validation cannot make the validator open a socket."""
    info = validate_document(enrich_doc)
    forged = copy.deepcopy(info.document)
    forged["$defs"]["EnrichInput"]["properties"]["urls"]["items"] = {
        "$ref": "https://evil.example/x.json"
    }

    def no_network(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("the validator tried to open a socket")

    monkeypatch.setattr(socket, "socket", no_network)
    validator = InputValidator(dataclasses.replace(info, document=forged))
    with pytest.raises(Exception) as exc:  # noqa: PT011 -- referencing's own error type
        validator.validate({"urls": ["https://a.com"]})
    assert "AssertionError" not in type(exc.value).__name__
    assert "evil.example" in str(exc.value)


def _with_loop(doc: dict[str, Any]) -> dict[str, Any]:
    looped = copy.deepcopy(doc)
    looped["$defs"]["A"] = {"$ref": "#/$defs/B"}
    looped["$defs"]["B"] = {"oneOf": [{"$ref": "#/$defs/A"}, {"type": "null"}]}
    looped["$defs"]["EnrichInput"]["properties"]["x"] = {"$ref": "#/$defs/A"}
    return looped


def test_a_reference_loop_is_refused_when_the_validator_is_built(
    enrich_doc: dict[str, Any],
) -> None:
    """It used to pass the catalog and then crash every validation with RecursionError:
    a seller's handler on any input, a buyer's SDK on any delivery."""
    with pytest.raises(ContractError) as info:
        InputValidator(seal(_with_loop(enrich_doc)))
    assert info.value.path == "$defs.A"


def test_a_loop_that_slipped_past_validation_is_a_refusal_not_a_crash(
    enrich_doc: dict[str, Any],
) -> None:
    info = validate_document(enrich_doc)
    validator = InputValidator(dataclasses.replace(info, document=_with_loop(info.document)))
    with pytest.raises(ContractError) as refused:
        validator.validate({"urls": ["https://a.com"], "x": 1})
    assert refused.value.path == "input"
    assert "too deeply nested" in refused.value.message


# --------------------------------------------------------------------------- formats


@pytest.mark.parametrize(
    ("fmt", "good", "bad"),
    [
        (
            "uri",
            [
                "https://a.com",
                "urn:isbn:0451450523",
                "http://[::1]/",
                "mailto:a@b.c",
                "http://u@h:80/p?q#f",
            ],
            [
                "a.com",
                "http://[zz]/",
                "https://a b",
                "https://exämple.com",
                "1http://a",
                "",
                "http://a.com/\n",
            ],
        ),
        (
            "uri-reference",
            ["/a/b", "a/b", "?q", "#f", "https://a.com", ""],
            ["a b", "http://[x]/", ":a"],
        ),
        (
            "hostname",
            ["acme.com", "a-b.c", "localhost"],
            ["-a.com", "a..com", "a_b.com", "a" * 64 + ".com", "x" * 254],
        ),
        ("email", ["a.b+c@acme.com"], ["a@", "@a.com", "a b@c.com", "a..b@c.com", '"q"@c.com']),
        ("ipv4", ["127.0.0.1", "255.255.255.255"], ["01.1.1.1", "256.1.1.1", "1.1.1", "1.1.1.1\n"]),
        ("ipv6", ["::1", "fe80::1", "::ffff:1.2.3.4"], ["fe80::1%eth0", ":::", "1.2.3.4"]),
        (
            "uuid",
            ["123e4567-e89b-12d3-a456-426614174000"],
            ["123e4567e89b12d3a456426614174000", "x" * 36],
        ),
        (
            "date",
            ["2024-02-29", "2026-10-11"],
            ["2023-02-29", "2026-13-01", "2026-1-1", "20261011"],
        ),
        (
            "time",
            ["10:00:00Z", "23:59:59.5+18:00", "00:00:00z"],
            ["24:00:00Z", "10:00:00", "10:00:00+19:00", "10:60:00Z"],
        ),
        (
            "date-time",
            ["2026-10-11T10:00:00Z", "2026-10-11t10:00:00.123-05:30"],
            ["2026-10-11 10:00:00Z", "2026-10-11T10:00Z"],
        ),
        ("duration", ["P1D", "PT1H", "P1Y2M3DT4H5M6S", "P2W"], ["P", "PT", "P1DT", "1D", "P1H"]),
    ],
)
def test_formats(fmt: str, good: list[str], bad: list[str]) -> None:
    for value in good:
        assert check_format(fmt, value), (fmt, value)
    for value in bad:
        assert not check_format(fmt, value), (fmt, value)


@pytest.mark.parametrize(
    ("pattern", "fragment"),
    [
        ("(?=a)", "lookaround"),
        ("(?<!a)b", "lookaround"),
        ("(a)\\1", "backreference"),
        ("(?P<x>a)(?P=x)", "named group"),
        ("(", "unclosed"),
        ("a" * 1001, "longer than"),
    ],
)
def test_unportable_patterns(pattern: str, fragment: str) -> None:
    problem = pattern_problem(pattern)
    assert problem is not None
    assert fragment in problem


def test_portable_patterns() -> None:
    assert pattern_problem(r"^[a-z0-9][a-z0-9/._-]*$") is None
    assert pattern_problem(r"^a\\1$") is None  # an escaped backslash, then 1
    assert compile_pattern(r"^a$").search("a\n") is None
    assert compile_pattern(r"^[$]$").search("$") is not None  # $ inside a class is literal
    assert compile_pattern(r"^\$$").search("$") is not None
