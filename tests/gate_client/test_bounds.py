"""Seam ②: every list, dict and free string of the wire models has a ceiling, and nothing a
body carries can make validating it cost more than a handful of errors.

The gate parses a request body into these models before anything else looks at it, and a
validation error costs far more memory than the bytes that caused it: an authenticated
1 MiB ``complete_artifact`` body of bad ETags was ~540 MiB at the gate, 4 MiB of stray keys
on a quote ~300 MiB. Each ceiling sits above the largest legitimate value, which is
exercised here model by model."""

from __future__ import annotations

import base64
import gc
import json
import re
import tracemalloc
from collections.abc import Callable
from functools import partial
from pathlib import Path
from typing import Any

import pytest
from pydantic import BaseModel, ValidationError

from kontra.contract import agent_card, from_yaml
from kontra.contract.names import HANDLE_MAX
from kontra.contract.offering import DESCRIPTION_MAX, PRICING_HINT_MAX
from kontra.contract.quote import KontraQuote
from kontra.contract.validate import MAX_CANONICAL_BYTES
from kontra.gate_client import models as m

from ._support import NOW

MiB = 1024 * 1024
SHA = "0" * 64
HANDLE = "a" * HANDLE_MAX
#: The longest offering reference a person can type (names.py: _REF_MAX).
REF = f"{HANDLE}/{'b' * HANDLE_MAX}@999999999.999999999.999999999"
#: An S3 multipart part's ETag: a quoted MD5.
ETAG = '"' + "0123456789abcdef" * 2 + '"'
#: An AWS multipart upload id is about 150 characters; leave plenty.
UPLOAD_ID = "u" * 1024
AGE_KEY = "age1" + "q" * 58
API_KEY = m.API_KEY_PREFIX + "k" * 43
#: The longest address the gate takes: a 64-character local part, 254 in all.
EMAIL = "e" * 64 + "@" + "d" * 61 + "." + "d" * 61 + "." + "d" * 57 + ".example"
#: The gates' relayed-payload limit (2 MiB) and heartbeat limit (64 KiB), as base64.
PAYLOAD_B64 = base64.b64encode(b"\0" * (2 * MiB)).decode()
HEARTBEAT_B64 = base64.b64encode(b"\0" * (64 * 1024)).decode()
#: A presigned URL with a session token runs past 2 KiB.
URL = "https://bucket.example/" + "p" * 4096
JWT = "e" * 4096
CONTRACT = from_yaml(
    (Path(__file__).resolve().parents[2] / "examples/enrich/contract.yaml").read_text()
)


def _models() -> list[type[BaseModel]]:
    return [
        obj
        for name in m.__all__
        if isinstance(obj := getattr(m, name), type) and issubclass(obj, BaseModel)
    ]


#: A fixed-length anchored pattern bounds a string as well as ``maxLength`` does.
_FIXED = re.compile(r"\^\[[^\]]+\]\{\d+\}\$")


def _unbounded(schema: dict[str, Any], where: str) -> list[str]:
    """Paths in ``schema`` (one model's, nested models by ``$ref``) to a string, array or
    open object without a ceiling. Date-times, enums and constants are bounded by what
    they accept."""
    if "anyOf" in schema:
        return [g for option in schema["anyOf"] for g in _unbounded(option, where)]
    kind = schema.get("type")
    if "$ref" in schema or "enum" in schema or "const" in schema:
        return []
    if kind == "string":
        fixed = _FIXED.fullmatch(schema.get("pattern", "")) is not None
        bounded = "maxLength" in schema or fixed or schema.get("format") == "date-time"
        return [] if bounded else [where]
    if kind == "array":
        gaps = [] if "maxItems" in schema else [where]
        return gaps + _unbounded(schema.get("items", {}), f"{where}[]")
    if kind == "object":
        if "properties" in schema:
            return [g for name, sub in schema["properties"].items() for g in _unbounded(sub, name)]
        return [] if "maxProperties" in schema else [where]
    return []


def test_every_list_dict_and_string_has_a_ceiling() -> None:
    gaps = {model.__name__: _unbounded(model.model_json_schema(), "") for model in _models()}
    assert {name: g for name, g in gaps.items() if g} == {}
    # The check itself: an unbounded field of each kind is found.
    loose = {"type": "object", "properties": {"s": {"type": "string"}, "l": {"type": "array"}}}
    assert _unbounded(loose, "") == ["s", "l"]
    assert _unbounded({"type": "object", "additionalProperties": True}, "d") == ["d"]


def test_every_ceiling_is_in_the_schema_the_gate_publishes() -> None:
    """The OpenAPI document (seam ②) carries each ceiling: ``maxLength``, ``maxItems``,
    ``maxProperties``."""
    schema = m.ArtifactComplete.model_json_schema()
    etags = schema["properties"]["etags"]
    assert etags["maxItems"] == m.MAX_PARTS == 10_000
    assert etags["items"] == {"maxLength": 256, "pattern": r"^[\x20-\x7e]*$", "type": "string"}
    assert m.PublishRequest.model_json_schema()["properties"]["document"]["maxProperties"] == 64


# --------------------------------------------------------------------------- the attacks


def _cost(validate: Callable[[], object]) -> tuple[list[Any], int]:
    """The errors ``validate`` raises and the peak memory it allocates doing so."""
    gc.collect()
    tracemalloc.start()
    try:
        validate()
    except ValidationError as e:
        errors: list[Any] = e.errors(include_input=False)
    else:
        raise AssertionError("accepted")
    finally:
        peak = tracemalloc.get_traced_memory()[1]
        tracemalloc.stop()
    return errors, peak


def test_a_megabyte_of_bad_etags_is_one_error() -> None:
    body = {"upload_id": None, "etags": [1] * (MiB // 2), "ciphertext_sha256": SHA}
    raw = json.dumps(body)
    assert len(raw) >= MiB  # the gate's complete_artifact body limit
    for validate in (
        lambda: m.ArtifactComplete.model_validate(body),  # FastAPI parses, then validates
        lambda: m.ArtifactComplete.model_validate_json(raw),
    ):
        errors, peak = _cost(validate)
        assert [(e["type"], e["loc"]) for e in errors] == [("too_long", ("etags",))]
        assert peak < 8 * MiB, peak
    # Under the cap, every item may be bad -- of the wrong type, too long, or not printable
    # ASCII (a trailing newline included): still one error, naming the first. Reported item
    # by item, 10 000 of them were 10 000 errors, ~25 MiB at a gate for one 1 MiB body.
    for bad in (["\n"] * m.MAX_PARTS, [1] * m.MAX_PARTS, [ETAG] * 9 + ["e" * 257], [ETAG + "\n"]):
        under: dict[str, Any] = {**body, "etags": bad}
        for validate in (
            partial(m.ArtifactComplete.model_validate, under),
            partial(m.ArtifactComplete.model_validate_json, json.dumps(under)),
        ):
            errors, peak = _cost(validate)
            first = next(i for i, e in enumerate(bad) if e != ETAG)
            assert [(e["type"], e["loc"]) for e in errors] == [("value_error", ("etags",))]
            assert errors[0]["msg"].startswith(f"Value error, etags[{first}]: ")
            assert peak < 8 * MiB, peak
    # A legitimate list passes untouched, an empty ETag (a single PUT) included.
    for good in ([ETAG] * m.MAX_PARTS, [""]):
        assert m.ArtifactComplete.model_validate({**body, "etags": good}).etags == good


def test_megabytes_of_stray_keys_are_one_error() -> None:
    body: dict[str, Any] = {"offering": REF, "input": {"b64": "AAAA"}}
    body.update((f"k{i}", 0) for i in range(400_000))
    assert len(json.dumps(body)) >= 4 * MiB  # the gate's quote body limit
    errors, peak = _cost(lambda: m.QuoteRequest.model_validate(body))
    assert len(errors) == 1
    assert errors[0]["type"] == "value_error"
    assert errors[0]["msg"] == ("Value error, unknown field(s): 'k0', 'k1', 'k2' and 399997 more")
    assert peak < 16 * MiB, peak
    # Nested models too, and a long key is named only by its start: a key is in the
    # error's location otherwise, and from there in the gate's message.
    nested = {"offering": REF, "input": {"b64": "AAAA", "x" * MiB: 1}}
    errors, _ = _cost(lambda: m.QuoteRequest.model_validate(nested))
    assert [(e["loc"], e["msg"]) for e in errors] == [
        (("input",), f"Value error, unknown field(s): '{'x' * 40}'")
    ]


def test_a_few_short_stray_keys_are_reported_where_they_are() -> None:
    """The honest mistake keeps pydantic's own report: each key, at its own location."""
    few = {"offering": REF, "input": {"b64": "AAAA"}, **{f"k{i}": 0 for i in range(8)}}
    with pytest.raises(ValidationError) as e:
        m.QuoteRequest.model_validate(few)
    assert [(err["type"], err["loc"]) for err in e.value.errors()] == [
        ("extra_forbidden", (f"k{i}",)) for i in range(8)
    ]
    with pytest.raises(ValidationError) as e:
        m.QuoteRequest.model_validate({**few, "k8": 0})
    assert [err["type"] for err in e.value.errors()] == ["value_error"]
    with pytest.raises(ValidationError) as e:
        m.QuoteRequest.model_validate({"offering": REF, "input": {"b64": "AAAA"}, "k" * 65: 0})
    assert [err["type"] for err in e.value.errors()] == ["value_error"]


@pytest.mark.parametrize(
    ("model", "field", "ceiling"),
    [
        (m.TokenRequest, "refresh_token", 16 * 1024),
        (m.TokenRequest, "handle", 255),
        (m.DevLogin, "handle", 255),
        (m.SetKey, "public_key", 255),
        (m.QuoteRequest, "offering", 255),
        (m.HireRequest, "quote_id", 255),
        (m.ResolveRequest, "note", 8 * 1024),
        (m.ProgressPost, "activity", 1000),
        (m.ArtifactCreate, "content_type", 255),
        (m.ArtifactComplete, "ciphertext_sha256", 255),
        (m.Topup, "currency", 255),
    ],
)
def test_a_request_string_past_its_ceiling_is_refused(
    model: type[BaseModel], field: str, ceiling: int
) -> None:
    base = _largest_request(model)
    model.model_validate({**base, field: "x" * ceiling})
    with pytest.raises(ValidationError) as e:
        model.model_validate({**base, field: "x" * (ceiling + 1)})
    assert [err["type"] for err in e.value.errors()] == ["string_too_long"]


# --------------------------------------------------------------------------- legitimate maxima


def _largest_request(model: type[BaseModel]) -> dict[str, Any]:
    """The largest body a legitimate client sends to the route that takes ``model``."""
    return {
        m.TokenRequest: {
            "grant_type": "refresh_token",
            "refresh_token": "r" * 43,
            "device_code": "d" * 43,
            "handle": HANDLE,
            "api_key": API_KEY,
        },
        m.MagicLinkRequest: {"email": EMAIL, "device_user_code": "BCDF-GHJK"},
        m.ApiKeyCreate: {"label": "l" * 64},
        m.DevLogin: {"handle": HANDLE},
        m.SetKey: {"public_key": AGE_KEY},
        m.PublishRequest: {"document": CONTRACT},
        m.QuoteRequest: {"offering": REF, "input": {"b64": PAYLOAD_B64}},
        m.HireRequest: {"quote_id": "0" * 32, "max_price_minor": 2**53},
        m.StartRequest: {"input": {"b64": PAYLOAD_B64}},
        m.RejectRequest: {"reason": "r" * 2000},
        m.ResolveRequest: {"buyer_minor": 1, "seller_minor": 1, "note": "n" * 2000},
        m.ProgressPost: {"at": NOW, "activity": "a" * 255, "ciphertext_b64": HEARTBEAT_B64},
        m.ArtifactCreate: {"size": 5 * 1024**4, "content_type": "c" * 255, "job_id": "0" * 32},
        m.ArtifactComplete: {
            "upload_id": UPLOAD_ID,
            "etags": [ETAG] * m.MAX_PARTS,
            "ciphertext_sha256": SHA,
        },
        m.BlobCreate: {"sha256": SHA, "size": 2**40},
        m.Topup: {"amount_minor": 10**12, "currency": "USD"},
        m.TemporalTokenRequest: {"role": "worker"},
        m.AcceptRequest: {},
    }[model]


#: route body limit (gate/app.py) per request model
_BODY_LIMIT = {
    m.QuoteRequest: 4 * MiB,
    m.StartRequest: 4 * MiB,
    m.PublishRequest: 2 * MiB,
    m.ArtifactComplete: 1 * MiB,
    m.ProgressPost: 128 * 1024,
}


@pytest.mark.parametrize(
    "model",
    [
        m.TokenRequest,
        m.MagicLinkRequest,
        m.ApiKeyCreate,
        m.DevLogin,
        m.SetKey,
        m.PublishRequest,
        m.QuoteRequest,
        m.HireRequest,
        m.StartRequest,
        m.RejectRequest,
        m.ResolveRequest,
        m.ProgressPost,
        m.ArtifactCreate,
        m.ArtifactComplete,
        m.BlobCreate,
        m.Topup,
        m.TemporalTokenRequest,
        m.AcceptRequest,
    ],
)
def test_the_largest_legitimate_request_is_accepted(model: type[BaseModel]) -> None:
    body = _largest_request(model)
    parsed = model.model_validate_json(json.dumps(body))
    assert parsed.model_dump(mode="json", exclude_unset=True) == body
    # ... and it fits the body limit the gate reads on its route.
    assert len(json.dumps(body)) <= _BODY_LIMIT.get(model, 64 * 1024)


def test_every_request_model_has_its_largest_request() -> None:
    from kontra.gate_client.routes import ROUTES

    bodies = {r.request for r in ROUTES if r.request is not None}
    for model in bodies:
        _largest_request(model)


def test_the_largest_legitimate_responses_are_accepted() -> None:
    """What a gate answers at its most: a contract error naming a path as long as the
    256 KiB canonical document, catalog entries at the contract's limits, a dispute
    reason, a seller's quote reason, a 10 000-part upload, a page of 200, a run with
    Temporal's default maximum of pending activities."""
    path = "$defs." + "d" * MAX_CANONICAL_BYTES
    m.ErrorBody(code="invalid_contract", message=f"{path}: {'m' * 200}", detail={"path": path})
    card = agent_card(CONTRACT, url=URL)
    entry = m.CatalogEntry(
        offering=REF,
        party=HANDLE,
        name=REF.split("@")[0],
        version="999999999.999999999.999999999",
        description="d" * DESCRIPTION_MAX,
        pricing_hint="p" * PRICING_HINT_MAX,
        contract_hash="sha256:" + SHA,
        document=CONTRACT,
        card=card,
        endpoint="k" + "0" * 32,
        service=f"{HANDLE}.{'b' * HANDLE_MAX}",
        task_queue=REF,
        public_key=AGE_KEY,
        published_at=NOW,  # type: ignore[arg-type]
    )
    assert len(card) < 64
    summary = m.CatalogSummary.model_validate(
        {
            **entry.model_dump(include=set(m.CatalogSummary.model_fields) - {"seller"}),
            "seller": {"party": HANDLE, "public_key": AGE_KEY},
        }
    )
    m.CatalogList(items=[summary] * 200, next_cursor="c" * 200)
    reason = "q" * 1000
    KontraQuote.model_validate({"status": "rejected", "reason": reason})  # the seller's limit
    m.QuoteView.model_validate(
        {
            "quote_id": "0" * 32,
            "offering": REF,
            "status": "rejected",
            "reason": reason,
            "contract_hash": "sha256:" + SHA,
            "endpoint": "k" + "0" * 32,
            "service": "s",
            "seller_public_key": AGE_KEY,
        }
    )
    job = {
        "job_id": "0" * 32,
        "offering": REF,
        "buyer": HANDLE,
        "seller": HANDLE,
        "state": "completed",
        "escrow": "disputed",
        "amount_minor": 2**53,
        "currency": "USD",
        "price": "90071992547409.92",
        "created_at": NOW,
        "deadline": NOW,
        "reason": "r" * 2000,
        "operation_id": "0" * 32,
        "workflow_id": "job-" + "0" * 32,
    }
    m.JobList.model_validate({"items": [job] * 200})
    part = {"number": 1, "url": URL, "size": 16 * MiB}
    m.ArtifactUpload.model_validate(
        {
            "key": "0" * 32,
            "upload_id": UPLOAD_ID,
            "part_size": 16 * MiB,
            "parts": [part] * m.MAX_PARTS,
            "expires_at": NOW,
        }
    )
    m.ProgressEvent.model_validate(
        {
            "seq": 1,
            "at": NOW,
            "source": "history",
            "last_event": "WorkflowExecutionContinuedAsNew",
            "activities_running": ["a" * 1000] * 2000,
        }
    )
    m.ProgressEvent.model_validate(
        {
            "seq": 2,
            "at": NOW,
            "source": "heartbeat",
            "activity": "a" * 255,
            "ciphertext_b64": HEARTBEAT_B64,
        }
    )
    m.ResultView.model_validate(
        {
            "job_id": "0" * 32,
            "payload": {"b64": PAYLOAD_B64},
            "sha256": SHA,
            "size": 2 * MiB,
            "delivered_at": NOW,
        }
    )
    m.HireView.model_validate(
        {
            "job_id": "0" * 32,
            "hold_token": JWT,
            "offering": REF,
            "endpoint": "k" + "0" * 32,
            "service": "s",
            "workflow_id": "job-" + "0" * 32,
            "operation_id": "0" * 32,
            "contract_hash": "sha256:" + SHA,
            "amount_minor": 1,
            "currency": "USD",
            "max_duration_seconds": 30 * 86_400,
            "seller_public_key": AGE_KEY,
            "expires_at": NOW,
        }
    )
    m.TokenResponse.model_validate(
        {
            "access_token": JWT,
            "expires_in": 900,
            "refresh_token": "r" * 43,
            "party": HANDLE,
            "auth_time": NOW,
        }
    )
    m.BlobUrl.model_validate({"sha256": SHA, "url": URL, "expires_at": NOW})


def test_bounds_follow_the_limits_they_name() -> None:
    """The ceilings that restate a limit defined elsewhere in the package stay in step."""
    assert m._DESCRIPTION_MAX == DESCRIPTION_MAX  # pyright: ignore[reportPrivateUsage]
    assert m._PRICING_HINT_MAX == PRICING_HINT_MAX  # pyright: ignore[reportPrivateUsage]
    assert len(REF) <= m._NAME_MAX  # pyright: ignore[reportPrivateUsage]
    assert len(AGE_KEY) <= m._NAME_MAX  # pyright: ignore[reportPrivateUsage]
