"""Unit tests of the sandbox-safe parts of ``kontra.hire``: Records built from a contract's
output schema, how a failed ``execute`` is classified, inputs, prices, and the hire errors'
round trip through a Temporal failure. No Temporal server needed."""

from __future__ import annotations

import copy
import json
import pickle
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import nexusrpc
import pydantic_core
import pytest
from pydantic import BaseModel
from temporalio.api.failure.v1 import Failure
from temporalio.converter import DataConverter
from temporalio.exceptions import (
    ApplicationError,
    CancelledError,
    NexusOperationError,
    TimeoutError,
    TimeoutType,
)

from kontra.codec.age import generate_identity
from kontra.contract import derive_document, parse_offering, validate_document
from kontra.gate_client import GateClient
from kontra.gate_client import models as m
from kontra.hire import (
    HireRefused,
    InsufficientFunds,
    InvalidDelivery,
    InvalidInput,
    JobFailed,
    KontraError,
    PriceAboveMax,
    QuoteRejected,
    Record,
    SellerBusy,
)
from kontra.hire._client import (
    Progress,
    _BuyerArtifact,  # pyright: ignore[reportPrivateUsage]
    _heartbeat_fields,  # pyright: ignore[reportPrivateUsage]
)
from kontra.hire._errors import from_failure
from kontra.hire._record import wrap
from kontra.hire._service import input_value, outcome_of, price_text
from kontra.types import Artifact

from .models import TextJob, TextOut

ART = {"key": "a" * 32, "size": 3, "sha256": "b" * 64, "content_type": "text/plain"}


def _schema() -> dict[str, Any]:
    spec = parse_offering(
        {
            "name": "acme/text",
            "version": "1.0.0",
            "description": "x",
            "max_duration": "1m",
            "review_window": "1h",
            "max_output_bytes": "1MB",
            "artifact_retention": "1d",
        }
    )
    doc = derive_document(spec, TextJob, TextOut)
    return validate_document(doc).output_schema


def _delivery(**changes: Any) -> dict[str, Any]:
    out: dict[str, Any] = {
        "text": "HI",
        "length": 2,
        "summary": {"chars": 2},
        "parts": [{"index": 0, "file": None}, {"index": 1, "file": ART}],
        "file": ART,
        "files": {"extra": ART},
    }
    out.update(changes)
    return out


def test_artifacts_are_found_by_the_schema() -> None:
    rec = wrap(_delivery(), _schema())
    assert isinstance(rec, Record)
    assert isinstance(rec.file, Artifact)  # oneOf [KontraArtifact, null]
    assert isinstance(rec.parts[1].file, Artifact)  # inside an array of objects
    assert rec.parts[0].file is None
    assert isinstance(rec.files["extra"], Artifact)  # a typed map
    assert isinstance(rec.summary, Record)
    assert rec.summary.chars == 2
    assert rec.to_dict() == _delivery()
    json.dumps(rec.to_dict())  # plain JSON


def test_a_value_shaped_like_an_artifact_is_not_one_unless_the_schema_says_so() -> None:
    rec = wrap({"summary": ART}, {"type": "object", "properties": {"summary": {"type": "object"}}})
    assert isinstance(rec.summary, Record)
    assert not isinstance(rec.summary, Artifact)
    plain = wrap(_delivery(), None)  # no schema: objects become Records, nothing else
    assert isinstance(plain.file, Record)


def test_a_bad_artifact_is_an_invalid_delivery_not_a_crash() -> None:
    with pytest.raises(InvalidDelivery):
        wrap(_delivery(file={"key": "../../etc"}), _schema())


def test_a_factory_binds_every_artifact() -> None:
    made: list[Artifact] = []

    def bind(a: Artifact) -> Artifact:
        made.append(a)
        return a

    wrap(_delivery(), _schema(), artifact=bind)
    assert len(made) == 3


def test_record_access_and_errors() -> None:
    rec = Record({"items": [1], "name": "x", "nested": Record({"a": 1})})
    assert rec["items"] == [1]  # a mapping method name: item access
    assert rec.name == rec["name"] == "x"
    assert "name" in rec
    assert len(rec) == 3
    assert rec == {"items": [1], "name": "x", "nested": {"a": 1}}
    with pytest.raises(AttributeError, match="no field 'missing'"):
        _ = rec.missing
    with pytest.raises(AttributeError, match="read-only"):
        rec.name = "y"  # type: ignore[misc]
    with pytest.raises(TypeError):
        hash(rec)
    assert "name" in dir(rec)
    assert copy.deepcopy(rec) == rec


def test_records_serialise_through_pydantic() -> None:
    class Out(BaseModel):
        rec: Record

    rec = wrap(_delivery(), _schema())
    dumped = pydantic_core.to_jsonable_python(Out(rec=rec))
    assert dumped == {"rec": _delivery()}
    assert Out.model_validate({"rec": {"a": {"b": 1}}}).rec.a.b == 1


def test_outcome_of_a_failed_execute() -> None:
    def wrapped(cause: BaseException) -> NexusOperationError:
        err = NexusOperationError(
            "nexus operation failed",
            scheduled_event_id=1,
            endpoint="k" + "0" * 32,
            service="acme.text",
            operation="execute",
            operation_token="",
        )
        err.__cause__ = cause
        return err

    refused = outcome_of(
        wrapped(
            nexusrpc.HandlerError("hold token refused", type=nexusrpc.HandlerErrorType.BAD_REQUEST)
        )
    )
    assert (refused.kind, refused.state) == ("refused", "failed")
    assert "hold token refused" in refused.reason
    canceled = outcome_of(wrapped(CancelledError("operation canceled")))
    assert (canceled.kind, canceled.state) == ("canceled", "canceled")
    timed_out = outcome_of(
        wrapped(
            TimeoutError("timed out", type=TimeoutType.SCHEDULE_TO_CLOSE, last_heartbeat_details=[])
        )
    )
    assert timed_out.kind == "timed_out"
    failed = outcome_of(wrapped(ApplicationError("the seller says no")))
    assert (failed.kind, failed.reason) == ("failed", "the seller says no")


def test_inputs() -> None:
    assert input_value(None, {"text": "x", "doc": Artifact.model_validate(ART)}) == {
        "text": "x",
        "doc": ART,
    }
    assert input_value(TextJob(text="x"), {}) == {
        "text": "x",
        "mode": "ok",
        "beats": 0,
        "doc": None,
    }
    assert input_value({"a": Decimal("1.5")}, {}) == {"a": "1.5"}
    with pytest.raises(TypeError):
        input_value({"a": 1}, {"b": 2})
    with pytest.raises(TypeError):
        input_value([1, 2], {})


def test_prices() -> None:
    assert price_text(20) == "20"
    assert price_text(0.1) == "0.1"
    assert price_text("1.50") == "1.50"
    for bad in ("abc", -1, float("nan"), True):
        with pytest.raises((ValueError, TypeError)):
            price_text(bad)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "err",
    [
        QuoteRejected("no", offering="acme/x@1.0.0", reason="no"),
        SellerBusy("later", offering="acme/x@1.0.0", eta_seconds=30),
        PriceAboveMax("too much", price="2.00", max_price="1.00", currency="USD"),
        InsufficientFunds("broke"),
        HireRefused("gone", code="offering_not_found", status=404),
        HireRefused("used", code="quote_used", status=409, detail={"job_id": "e" * 32}),
        InvalidInput("bad", path="input.text"),
        JobFailed("boom", state="canceled", job_id="f" * 32),
        KontraError("plain"),
    ],
)
async def test_hire_errors_survive_a_temporal_failure(err: KontraError) -> None:
    """What the hire activity raises is what workflow code catches."""
    converter = DataConverter.default
    failure = Failure()
    converter.failure_converter.to_failure(err, converter.payload_converter, failure)
    decoded = converter.failure_converter.from_failure(failure, converter.payload_converter)
    assert isinstance(decoded, ApplicationError)
    assert decoded.non_retryable
    back = from_failure(decoded)
    assert type(back) is type(err)
    assert back is not None
    assert (str(back), back.fields) == (str(err), err.fields)
    if isinstance(err, PriceAboveMax):
        assert isinstance(back, PriceAboveMax)
        assert (back.price, back.max_price) == (Decimal("2.00"), Decimal("1.00"))
    if isinstance(err, SellerBusy):
        assert isinstance(back, SellerBusy)
        assert back.eta == err.eta
    if isinstance(err, HireRefused):
        assert isinstance(back, HireRefused)
        assert (back.code, back.status, back.detail) == (err.code, err.status, err.detail)
    assert from_failure(ApplicationError("not ours", type="Other")) is None


async def test_a_delivered_record_copies_and_pickles_without_its_binding() -> None:
    """Artifacts in a delivery are bound to the client that received it (its gate and our
    secret key). Copying such a Record works; pickling it keeps plain artifacts, never the
    key or the live client."""
    secret = generate_identity()[0]
    gate = GateClient("http://127.0.0.1:9")
    try:

        def bind(a: Artifact) -> Artifact:
            bound = _BuyerArtifact.model_validate(a.model_dump())
            bound._gate, bound._identity = gate, secret  # pyright: ignore[reportPrivateUsage]
            return bound

        rec = wrap(_delivery(), _schema(), artifact=bind)
        assert isinstance(rec.file, _BuyerArtifact)
        assert copy.deepcopy(rec) == rec
        assert copy.deepcopy(rec).file is rec.file  # frozen: shared, not cloned
        raw = pickle.dumps(rec)
        assert secret.encode() not in raw
        back = pickle.loads(raw)
        assert back == rec
        assert type(back.file) is Artifact
        assert secret not in repr(rec)
    finally:
        await gate.aclose()


def test_heartbeat_details_are_untrusted() -> None:
    """A seller's heartbeat details never end ``progress()``: numbers that do not fit a
    float, or are not finite, are ignored; so is anything that is not a mapping."""
    assert _heartbeat_fields({"percent": 10**400, "message": "x"}) == (None, "x")
    assert _heartbeat_fields({"percent": float("nan")}) == (None, None)
    assert _heartbeat_fields({"percent": float("inf")}) == (None, None)
    assert _heartbeat_fields({"percent": True, "message": 3}) == (None, None)
    assert _heartbeat_fields([1, 2]) == (None, None)
    assert _heartbeat_fields({"percent": 12.5}) == (12.5, None)


def test_progress_renders_without_terminal_control_characters() -> None:
    at = datetime(2026, 1, 1, tzinfo=UTC)
    beat = Progress(
        seq=1,
        at=at,
        source=m.ProgressSource.HEARTBEAT,
        percent=50,
        message="half\x1b]0;pwned\x07 done\r\n",
    )
    assert str(beat) == "50% half]0;pwned done"
    assert beat.message is not None
    assert "\x1b" in beat.message  # the value itself is untouched
    alive = Progress(
        seq=2, at=at, source=m.ProgressSource.HISTORY, activities_running=("fetch\x1b[2J",)
    )
    assert str(alive) == "alive, in fetch[2J, at 2026-01-01T00:00:00+00:00"
