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

from kontra import _jobkeys as jk
from kontra.codec import age
from kontra.contract import agent_card, from_yaml
from kontra.contract.names import HANDLE_MAX
from kontra.contract.offering import DESCRIPTION_MAX, PRICING_HINT_MAX, STREAMS_MAX
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
#: The gates' relayed-payload limit (2 MiB) and relayed stream item limit (256 KiB), base64.
PAYLOAD_B64 = base64.b64encode(b"\0" * (2 * MiB)).decode()
ITEM_B64 = base64.b64encode(b"\0" * m.STREAM_ITEM_MAX).decode()
#: A presigned URL with a session token runs past 2 KiB.
URL = "https://bucket.example/" + "p" * 4096
JWT = "e" * 4096
#: A WebAuthn credential id at its largest (L3: 1023 bytes), as base64url.
CREDENTIAL_ID = base64.urlsafe_b64encode(b"\0" * 1023).rstrip(b"=").decode()
#: A wrap as the clients make one (decisions I25): a job key file, age-encrypted to a device.
WRAP_B64 = jk.wrap_job_key("0" * 32, age.generate_identity()[0], age.generate_identity()[1])
#: The largest wrap a gate keeps, as base64.
WRAP_MAX_B64 = base64.b64encode(b"w" * m.WRAP_MAX).decode()
TRANSPORTS = ["usb", "nfc", "ble", "smart-card", "hybrid", "internal"]
#: The browser's answer to a registration at its largest: the longest credential id, an RSA
#: key, and an attestation with a certificate chain (``none`` is what the gates ask for; a
#: browser may still send more).
REGISTRATION = {
    "id": CREDENTIAL_ID,
    "rawId": CREDENTIAL_ID,
    "type": "public-key",
    "response": {
        "clientDataJSON": "c" * 512,
        "attestationObject": "a" * 12 * 1024,
        "authenticatorData": "d" * 2048,
        "transports": TRANSPORTS,
        "publicKeyAlgorithm": -257,
        "publicKey": "p" * 512,
    },
    "authenticatorAttachment": "cross-platform",
    "clientExtensionResults": {"credProps": {"rk": True}},
}
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
        (m.AddKey, "public_key", 255),
        (m.AddKey, "label", 255),
        (m.AddDevice, "public_key", 255),
        (m.QuoteRequest, "offering", 255),
        (m.HireRequest, "quote_id", 255),
        (m.ResolveRequest, "note", 8 * 1024),
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
        m.AddKey: {"label": "l" * 255, "public_key": AGE_KEY},
        m.PublishRequest: {
            "document": CONTRACT,
            "streams": sorted(f"t{i}." + "x" * 60 for i in range(STREAMS_MAX)),
        },
        m.QuoteRequest: {"offering": REF, "input": {"b64": PAYLOAD_B64}},
        m.HireRequest: {"quote_id": "0" * 32, "max_price_minor": 2**53, "buyer_key": AGE_KEY},
        m.StartRequest: {"input": {"b64": PAYLOAD_B64}},
        m.RejectRequest: {"reason": "r" * 2000},
        m.ResolveRequest: {"buyer_minor": 1, "seller_minor": 1, "note": "n" * 2000},
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
        m.PasskeyAdd: {
            "ceremony_id": "wac_" + "0" * 32,
            "label": "l" * 64,
            "credential": REGISTRATION,
        },
        m.AddDevice: {"label": "l" * 64, "public_key": AGE_KEY, "kind": "browser"},
        # A catch-up's page: a hundred jobs, each wrapped to the device being served.
        m.ShareJobKeys: {
            "items": [
                {
                    "job_id": f"{i:032x}",
                    "proof": "p" * 43,
                    "wraps": [{"recipient": AGE_KEY, "wrap": WRAP_B64}],
                }
                for i in range(m.SHARE_ITEMS_MAX)
            ]
        },
        m.PasskeyConfirm: {
            "ceremony_id": "wac_" + "0" * 32,
            "credential": {
                "id": CREDENTIAL_ID,
                "rawId": CREDENTIAL_ID,
                "type": "public-key",
                "response": {
                    "clientDataJSON": "c" * 512,
                    "authenticatorData": "d" * 512,
                    "signature": "s" * 512,
                    "userHandle": "u" * 43,
                },
                "authenticatorAttachment": "platform",
                "clientExtensionResults": {},
            },
        },
    }[model]


#: route body limit (gate/app.py) per request model
_BODY_LIMIT = {
    m.QuoteRequest: 4 * MiB,
    m.StartRequest: 4 * MiB,
    m.PublishRequest: 2 * MiB,
    m.ArtifactComplete: 1 * MiB,
    # A hundred jobs, or one job wrapped to thirty-two devices, with room.
    m.ShareJobKeys: 256 * 1024,
}


@pytest.mark.parametrize(
    "model",
    [
        m.TokenRequest,
        m.MagicLinkRequest,
        m.ApiKeyCreate,
        m.DevLogin,
        m.AddKey,
        m.PublishRequest,
        m.QuoteRequest,
        m.HireRequest,
        m.StartRequest,
        m.RejectRequest,
        m.ResolveRequest,
        m.ArtifactCreate,
        m.ArtifactComplete,
        m.BlobCreate,
        m.Topup,
        m.TemporalTokenRequest,
        m.AcceptRequest,
        m.PasskeyAdd,
        m.PasskeyConfirm,
        m.AddDevice,
        m.ShareJobKeys,
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
        recipients=[AGE_KEY] * m.RECIPIENTS_MAX,
        published_at=NOW,  # type: ignore[arg-type]
        streams=sorted(f"t{i}." + "x" * 60 for i in range(STREAMS_MAX)),
    )
    assert len(card) < 64
    summary = m.CatalogSummary.model_validate(
        {
            **entry.model_dump(include=set(m.CatalogSummary.model_fields) - {"seller"}),
            "seller": {"party": HANDLE, "recipients": [AGE_KEY] * m.RECIPIENTS_MAX},
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
            "seller_recipients": [AGE_KEY] * m.RECIPIENTS_MAX,
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
    m.StreamEvent.model_validate(
        {
            "cursor": 1,
            "kind": "history",
            "at": NOW,
            "state": "working",
            "last_event": "WorkflowExecutionContinuedAsNew",
            "activities_running": ["a" * 1000] * 2000,
        }
    )
    m.StreamEvent.model_validate(
        {
            "cursor": 2,
            "kind": "item",
            "at": NOW,
            "topic": "t" + "x" * 63,
            "offset": 10**18,
            "ciphertext_b64": ITEM_B64,
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
            "seller_recipients": [AGE_KEY] * m.RECIPIENTS_MAX,
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
    # Adding an eleventh passkey's options exclude ten, at the longest credential id each.
    excluded = {"id": CREDENTIAL_ID, "type": "public-key", "transports": TRANSPORTS}
    m.PasskeyCeremony.model_validate(
        {
            "ceremony_id": "wac_" + "0" * 32,
            "rp_id": "r" * 253,
            "options": {
                "rp": {"name": "n" * 64, "id": "r" * 253},
                "user": {"id": "u" * 43, "name": HANDLE, "displayName": HANDLE},
                "challenge": "c" * 43,
                "pubKeyCredParams": [{"type": "public-key", "alg": a} for a in (-8, -7, -257)],
                "timeout": 300_000,
                "excludeCredentials": [excluded] * m.MAX_PASSKEYS,
                "authenticatorSelection": {
                    "residentKey": "preferred",
                    "requireResidentKey": False,
                    "userVerification": "required",
                },
                "attestation": "none",
            },
            "expires_in": 360,
        }
    )
    view = {
        "id": "pky_" + "0" * 32,
        "label": "l" * 64,
        "created_at": NOW,
        "added_by": "0" * 12,
        "last_used_at": NOW,
        "backed_up": True,
        "transports": TRANSPORTS,
        "revoked_at": NOW,
        "revoked_by": "operator",
    }
    m.PasskeyList.model_validate({"rp_id": "r" * 253, "passkeys": [view] * 50})
    # Devices at the cap; a page of job keys, each wrapped to every device at the largest
    # size a gate keeps, with its challenge (decisions I25).
    device = {
        "public_key": AGE_KEY,
        "label": "l" * 64,
        "kind": "browser",
        "added_at": NOW,
        "added_by": "0" * 12,
        "last_used_at": NOW,
    }
    m.DeviceList.model_validate({"devices": [device] * m.DEVICES_MAX})
    wrap = {"recipient": AGE_KEY, "wrap": WRAP_MAX_B64, "wrapped_at": NOW}
    challenge = base64.b64encode(b"c" * m.CHALLENGE_MAX).decode()
    item = {
        "job_id": "0" * 32,
        "buyer_key": AGE_KEY,
        "wraps": [wrap] * m.DEVICES_MAX,
        "challenge": challenge,
    }
    m.JobKeyList.model_validate({"items": [item] * 200, "next_cursor": "0" * 32})
    refused = [{"job_id": "0" * 32, "code": "bad_proof"}] * m.SHARE_ITEMS_MAX
    m.SharedJobKeys.model_validate({"stored": 0, "skipped": 0, "refused": refused})
    # A version's task queue at its largest: every type with a full listing of pollers,
    # each at Temporal's default identity limit and its build id limit, and an SDK name and
    # version as long as Control lets through (kontra._control fits a heartbeat's to 255).
    poller = {
        "identity": "i" * 1000,
        "last_access_time": NOW,
        "rate_per_second": 100_000.0,
        "build_id": "b" * 255,
        "deployment_name": "d" * 1000,
        "sdk_name": "n" * 255,
        "sdk_version": "v" * 255,
    }
    info = {
        "pollers": [poller] * m.POLLERS_MAX,
        "stats": {
            "approximate_backlog_count": 2**63 - 1,
            "approximate_backlog_age": 30 * 86_400.0,
            "tasks_add_rate": 1e6,
            "tasks_dispatch_rate": 0.0,
            "backlog_increase_rate": 1e6,
        },
    }
    m.WorkersView.model_validate(
        {
            "offering": REF,
            "task_queue": REF,
            "types_info": {kind.value: info for kind in m.TaskQueueType},
            "verdict": f"activity backlog growing for 30 d, {m.POLLERS_MAX} pollers -- scale up",
            "stale": False,
        }
    )


def test_task_queue_stats_refuse_what_temporal_never_reports() -> None:
    """Counts, ages and rates are never below zero (the backlog's growth may be) and never
    infinite or NaN; a task-queue type is one of Temporal's three."""
    m.TaskQueueStats(backlog_increase_rate=-2.5)
    for bad in (
        {"approximate_backlog_count": -1},
        {"approximate_backlog_age": -1.0},
        {"tasks_add_rate": float("inf")},
        {"tasks_dispatch_rate": float("nan")},
        {"backlog_increase_rate": float("-inf")},
    ):
        with pytest.raises(ValidationError):
            m.TaskQueueStats.model_validate(bad)
    with pytest.raises(ValidationError):
        m.WorkersView.model_validate(
            {
                "offering": REF,
                "task_queue": REF,
                "types_info": {"sticky": {}},
                "verdict": "idle, 1 poller",
                "stale": False,
            }
        )


def test_bounds_follow_the_limits_they_name() -> None:
    """The ceilings that restate a limit defined elsewhere in the package stay in step."""
    assert m._DESCRIPTION_MAX == DESCRIPTION_MAX  # pyright: ignore[reportPrivateUsage]
    assert m._PRICING_HINT_MAX == PRICING_HINT_MAX  # pyright: ignore[reportPrivateUsage]
    assert len(REF) <= m._NAME_MAX  # pyright: ignore[reportPrivateUsage]
    assert len(AGE_KEY) <= m._NAME_MAX  # pyright: ignore[reportPrivateUsage]


# --------------------------------------------------------------------------- streams (I31)

_AT = "2026-10-20T00:00:00Z"


@pytest.mark.parametrize(
    "event",
    [
        {"kind": "item", "topic": "turn_events", "offset": 0, "ciphertext_b64": "QQ=="},
        {"kind": "item", "topic": "kontra.progress", "offset": 5, "ciphertext_b64": "QQ==", "state": "working"},
        {"kind": "gap", "topic": "t", "offset": 3, "size": 300_000, "reason": "too_large"},
        {"kind": "gap", "topic": "t", "offset": 3, "reason": "topic_budget"},
        {"kind": "gap", "reason": "history_budget"},
        {"kind": "gap", "reason": "relay_failed", "state": "working"},
        {"kind": "history", "state": "working", "activities_completed": 2},
    ],
)  # fmt: skip
def test_a_stream_event_of_each_kind(event: dict[str, Any]) -> None:
    m.StreamEvent.model_validate({"cursor": 1, "at": _AT, **event})


@pytest.mark.parametrize(
    "event",
    [
        {"kind": "item", "topic": "t", "offset": 0},  # no ciphertext
        {"kind": "item", "offset": 0, "ciphertext_b64": "QQ=="},  # no topic
        {"kind": "item", "topic": "t", "ciphertext_b64": "QQ=="},  # no offset
        {"kind": "item", "topic": "t", "offset": 0, "ciphertext_b64": "QQ==", "reason": "invalid"},
        {"kind": "item", "topic": "t", "offset": 0, "ciphertext_b64": "QQ==", "last_event": "x"},
        {"kind": "item", "topic": "bad topic", "offset": 0, "ciphertext_b64": "QQ=="},
        {"kind": "item", "topic": "t", "offset": -1, "ciphertext_b64": "QQ=="},
        {"kind": "gap"},  # no reason
        {"kind": "gap", "reason": "too_large", "ciphertext_b64": "QQ=="},
        {"kind": "gap", "reason": "too_large", "topic": "t"},  # topic without offset
        {"kind": "gap", "reason": "history_budget", "size": 3},  # a size is an item's
        {"kind": "gap", "reason": "nope"},
        {"kind": "history"},  # no state
        {"kind": "history", "state": "working", "topic": "t"},
        {"kind": "other", "state": "working"},
    ],
)  # fmt: skip
def test_a_stream_event_that_breaks_its_kinds_rules(event: dict[str, Any]) -> None:
    with pytest.raises(ValidationError):
        m.StreamEvent.model_validate({"cursor": 1, "at": _AT, **event})


def test_a_stream_event_has_a_cursor_and_a_bounded_ciphertext() -> None:
    item = {"kind": "item", "topic": "t", "offset": 0, "at": _AT}
    with pytest.raises(ValidationError):
        m.StreamEvent.model_validate({**item, "cursor": 0, "ciphertext_b64": "QQ=="})
    m.StreamEvent.model_validate({**item, "cursor": 1, "ciphertext_b64": ITEM_B64})
    with pytest.raises(ValidationError) as e:
        m.StreamEvent.model_validate({**item, "cursor": 1, "ciphertext_b64": ITEM_B64 + "AAAA"})
    assert [err["type"] for err in e.value.errors()] == ["string_too_long"]


def test_streams_are_bounded_and_canonical_on_both_catalog_models() -> None:
    req = m.PublishRequest.model_validate({"document": {}, "streams": ["b", "a"]})
    assert req.streams == ["a", "b"]
    assert m.PublishRequest.model_validate({"document": {}}).streams == []
    for bad in (["kontra.progress"], ["a", "a"], ["bad topic"], [f"t{i}" for i in range(9)]):
        with pytest.raises(ValidationError):
            m.PublishRequest.model_validate({"document": {}, "streams": bad})
    assert "streams" in m.CatalogEntry.model_fields
    assert m.CatalogEntry.model_fields["streams"].default_factory is not None
