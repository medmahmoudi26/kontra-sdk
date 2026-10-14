"""The generated service, without Temporal: what ``quote`` answers, and what a seller bug
in ``quote`` becomes (never a leak of the input, never a retry loop)."""

from __future__ import annotations

import asyncio
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import nexusrpc
import pytest
from temporalio.api.common.v1 import Payload
from temporalio.common import RawValue

from kontra.codec import CLAIM_CHECK_ENCODING
from kontra.codec.age import generate_identity
from kontra.contract import ContractError, KontraQuote, Quote
from kontra.contract.names import new_job_id
from kontra.gate_client import models as m
from kontra.serve import BoundOffering, HoldVerifier, JobDirectory, OfferingService
from kontra.serve.handler import INPUT_SLACK, service_handler

from . import seller
from .support import write_offering

PK = generate_identity()[1]


class _Nothing:
    async def jwks(self) -> dict[str, Any]:
        raise AssertionError("no gate")

    async def seller_job(self, job_id: str) -> Any:
        raise AssertionError("no gate")


def _service(bound: BoundOffering) -> OfferingService:
    return OfferingService(
        bound=bound,
        namespace="acme",
        party="acme",
        contract_hash=bound.contract_hash(),
        validator=bound.validator(),
        verifier=HoldVerifier(_Nothing()),
        jobs=JobDirectory(_Nothing()),
    )


@pytest.fixture
def service(tmp_path: Path) -> OfferingService:
    return _service(seller.bind(write_offering(tmp_path)))


async def test_quote_answers_on_the_wire(service: OfferingService) -> None:
    ok = await service.quote({"text": "abcd", "copies": 3})
    assert ok.model_dump(mode="json") == {
        "status": "ok",
        "amount_minor": 12,
        "currency": "USD",
        "max_duration_seconds": 600,
    }
    rejected = await service.quote({"text": "reject me"})
    assert (rejected.status, rejected.reason) == ("rejected", "this text is not for sale")
    invalid = await service.quote({"text": 3, "secret": "s3cr3t"})
    assert invalid.status == "rejected"
    assert "2 problem(s)" in (invalid.reason or "")
    assert "s3cr3t" not in (invalid.reason or "")
    assert "secret" not in (invalid.reason or "")


def test_a_refused_input_cannot_forge_seller_log_lines(
    service: OfferingService, caplog: pytest.LogCaptureFixture
) -> None:
    """A refusal's path can hold a map key, i.e. buyer data: it is logged escaped and
    capped, never as a line of its own."""
    caplog.set_level("WARNING", "kontra.serve.handler")
    key = "x\nCRITICAL kontra.serve: seller key leaked" + "y" * 1000
    service._log_invalid("quote", ContractError(f"input.tags.{key}", "not an integer"))  # pyright: ignore[reportPrivateUsage]
    [record] = [r for r in caplog.records if "input refused" in r.getMessage()]
    message = record.getMessage()
    assert "\n" not in message
    assert "input.tags.x" in message
    assert len(message) < 400


async def _raises(service: OfferingService, raw: Any) -> nexusrpc.HandlerError:
    with pytest.raises(nexusrpc.HandlerError) as err:
        await service.quote(raw)
    return err.value


async def test_a_quote_that_raises_is_a_non_retryable_seller_error(
    service: OfferingService, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def boom(order: Any) -> Quote:
        raise RuntimeError(f"cannot price {order.text}")

    monkeypatch.setattr(service.bound, "quote", boom)
    err = await _raises(service, {"text": "private words"})
    assert err.type is nexusrpc.HandlerErrorType.INTERNAL
    assert err.retryable is False
    assert "private words" not in str(err)

    async def not_a_quote(order: Any) -> Any:
        return {"price": 1}

    monkeypatch.setattr(service.bound, "quote", not_a_quote)
    assert "returned no Quote" in str(await _raises(service, {"text": "x"}))

    async def too_long(order: Any) -> Quote:
        return Quote(price=1, max_duration=timedelta(days=1))

    monkeypatch.setattr(service.bound, "quote", too_long)
    assert "exceeds the offering's max_duration" in str(await _raises(service, {"text": "x"}))


def test_the_service_handler_is_named_after_the_offering(service: OfferingService) -> None:
    handler = service_handler(service)
    defn = nexusrpc.get_service_definition(type(handler))
    assert defn is not None
    assert defn.name == "acme.upper"
    assert set(defn.operation_definitions) == {"quote", "execute"}


async def test_an_unexpected_failure_says_nothing_about_the_sellers_machine(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A failure the handler did not anticipate (a Temporal RPC error while starting the
    job, say) reaches the caller -- in plaintext -- as a generic retryable error: no
    exception text, no traceback. The seller's log has the details."""
    from kontra.serve.handler import NO_TRACE, _guarded  # pyright: ignore[reportPrivateUsage]

    async def boom() -> None:
        raise RuntimeError("cannot reach /home/acme/.kontra/key")

    caplog.set_level("ERROR", "kontra.serve.handler")
    with pytest.raises(nexusrpc.HandlerError) as err:
        await _guarded("acme/upper@1.0.0", "execute", boom())
    assert err.value.type is nexusrpc.HandlerErrorType.INTERNAL
    assert err.value.retryable  # as temporalio would answer it
    assert "/home/acme" not in str(err.value)
    assert err.value.stack_trace == NO_TRACE
    assert err.value.__cause__ is None
    assert "/home/acme/.kontra/key" in caplog.text

    async def refuse() -> None:
        raise nexusrpc.HandlerError("no", type=nexusrpc.HandlerErrorType.BAD_REQUEST)

    with pytest.raises(nexusrpc.HandlerError) as err:
        await _guarded("acme/upper@1.0.0", "execute", refuse())
    assert (str(err.value), err.value.stack_trace) == ("no", NO_TRACE)


# --------------------------------------------------------------------------- decisions I16

NOW = 1_800_000_000.0


def _claims(**changes: Any) -> m.HoldClaims:
    job = new_job_id()
    base: dict[str, Any] = {
        "iss": "gate",
        "aud": "kontra:hold",
        "sub": job,
        "iat": int(NOW) - 60,
        "exp": int(NOW) + 3600,
        "job_id": job,
        "offering": "acme/upper@1.0.0",
        "seller": "acme",
        "seller_ns": "acme",
        "buyer": "bob",
        "buyer_ns": "bob",
        "buyer_key": PK,
        "amount_minor": 100,
        "currency": "USD",
        "contract_hash": "sha256:" + "0" * 64,
        "max_duration_seconds": 600,
    }
    return m.HoldClaims.model_validate({**base, **changes})


def _priced(seconds: int) -> KontraQuote:
    return KontraQuote(status="ok", amount_minor=1, currency="USD", max_duration_seconds=seconds)


def _refusal(err: pytest.ExceptionInfo[nexusrpc.HandlerError]) -> str:
    assert err.value.type is nexusrpc.HandlerErrorType.BAD_REQUEST
    assert err.value.retryable is False
    return str(err.value)


def test_a_late_start_runs_only_until_the_hold_expires(service: OfferingService) -> None:
    """The hold's max_duration counts from the hire: a run started late gets what is left
    before the expiry plus the escrow grace, never a full max_duration from its start."""
    service.clock = lambda: NOW
    assert service.run_timeout(_claims(), _priced(600)) == timedelta(seconds=600)
    # Started at once after the hire: the whole max_duration, though a moment has passed.
    just_hired = _claims(exp=int(NOW) + 599)
    assert service.run_timeout(just_hired, _priced(600)) == timedelta(seconds=600)
    late = _claims(exp=int(NOW) + 300)
    assert service.run_timeout(late, _priced(60)) == timedelta(seconds=300) + service.grace
    assert service.run_timeout(late, _priced(330)) == timedelta(seconds=300) + service.grace


def test_a_late_start_ends_by_the_gates_own_deadline(service: OfferingService) -> None:
    """The run must end by the gate's refund deadline, which the hold carries: a gate with a
    shorter grace than this handler's default (``kontra.testing`` uses one second) would
    otherwise refund at its deadline while the run goes on for up to 29 s and delivers to
    the buyer's call. The refusal keeps its tolerance for the moment between hire and start:
    a job quoted for its whole max_duration still starts, cut to the deadline."""
    service.clock = lambda: NOW
    exp = int(NOW) + 300
    short = _claims(exp=exp, deadline=exp + 1)
    assert service.run_timeout(short, _priced(60)) == timedelta(seconds=301)
    assert service.run_timeout(short, _priced(330)) == timedelta(seconds=301)
    with pytest.raises(nexusrpc.HandlerError) as err:
        service.run_timeout(short, _priced(331))
    assert f"must end by {datetime.fromtimestamp(exp + 1, UTC).isoformat()}" in _refusal(err)
    just_hired = _claims(exp=int(NOW) + 599, deadline=int(NOW) + 600)
    assert service.run_timeout(just_hired, _priced(600)) == timedelta(seconds=600)
    # A longer grace at the gate: the run may use it, and the refusal is that much later.
    long = _claims(exp=exp, deadline=exp + 120)
    assert service.run_timeout(long, _priced(420)) == timedelta(seconds=420)
    # A deadline the gate set at or before now leaves no time at all: refused.
    with pytest.raises(nexusrpc.HandlerError):
        service.run_timeout(_claims(exp=exp, deadline=int(NOW)), _priced(1))


def test_an_unset_bound_or_deadline_stays_out_of_the_signed_claims() -> None:
    """A gate that sets neither optional claim re-signs a hold to the same bytes (the hold
    digest of decisions I15 stays the same), so both are left out when unset."""
    dumped = _claims().model_dump()
    assert "input_size" not in dumped
    assert "deadline" not in dumped
    full = _claims(input_size=10, deadline=int(NOW)).model_dump()
    assert (full["input_size"], full["deadline"]) == (10, int(NOW))


def test_a_start_too_late_for_the_input_is_refused(service: OfferingService) -> None:
    """The seller would be blamed for a deadline it never had: refused, and the re-quoted
    duration (it describes the input) stays out of the plaintext message."""
    service.clock = lambda: NOW
    claims = _claims(exp=int(NOW) + 300)
    service.run_timeout(claims, _priced(330))  # what is left, grace included
    with pytest.raises(nexusrpc.HandlerError) as err:
        service.run_timeout(claims, _priced(331))
    text = _refusal(err)
    at = datetime.fromtimestamp(claims.exp, UTC).isoformat()
    assert at in text
    assert "sooner than this input needs" in text
    assert "331" not in text


def _reference(size: int) -> RawValue:
    data = b'{"sha256":"' + b"a" * 64 + b'","size":' + str(size).encode() + b"}"
    return RawValue(
        Payload(metadata={"encoding": CLAIM_CHECK_ENCODING, "kontra-v": b"1"}, data=data)
    )


def test_an_input_larger_than_the_quoted_one_is_refused_before_any_fetch(
    service: OfferingService,
) -> None:
    """A claim-check is judged by the size its reference names, an inline input by its
    own: above the hold's bound (plus a little slack for encodings), refused at once,
    naming the bound -- never fetched, decrypted or validated."""
    bound = 300
    claims = _claims(input_size=bound)
    service.check_input_size(claims, _reference(bound + INPUT_SLACK))  # within the slack
    with pytest.raises(nexusrpc.HandlerError) as err:
        service.check_input_size(claims, _reference(64 * 1024 * 1024))
    text = _refusal(err)
    assert f"larger than the input this hold was quoted for ({bound} bytes)" in text
    inline = RawValue(Payload(metadata={"encoding": b"json/plain"}, data=b"x" * 10_000))
    with pytest.raises(nexusrpc.HandlerError) as err:
        service.check_input_size(claims, inline)
    assert f"({bound} bytes)" in _refusal(err)
    malformed = RawValue(Payload(metadata={"encoding": CLAIM_CHECK_ENCODING}, data=b"{"))
    with pytest.raises(nexusrpc.HandlerError) as err:
        service.check_input_size(claims, malformed)
    assert "malformed claim-check reference" in _refusal(err)
    # A token from a gate that sets no bound, and a direct call, are not bounded here.
    service.check_input_size(_claims(), _reference(64 * 1024 * 1024))
    service.check_input_size(claims, {"text": "x" * 10_000})


async def test_a_fetch_that_would_outlast_the_attempt_is_refused_in_time(
    service: OfferingService,
) -> None:
    """A fetch still running when the Nexus request is about to time out is abandoned and
    refused: answering late is recorded as a seller that never answered."""
    fetching: list[float] = []

    async def slow(payload: Payload) -> Payload:
        fetching.append(time.monotonic())
        await asyncio.sleep(30)
        return payload

    service.open_deferred = slow
    deadline = datetime.now(UTC) + timedelta(seconds=2.5)
    began = time.monotonic()
    with pytest.raises(nexusrpc.HandlerError) as err:
        await service._read_input(_reference(1000), deadline)  # pyright: ignore[reportPrivateUsage]
    assert "could not be fetched within one Nexus attempt" in _refusal(err)
    assert fetching
    assert time.monotonic() - began < 1.5  # the deadline less the reserve for the answer


async def test_the_fetch_budget_never_trusts_a_skewed_clock(
    service: OfferingService, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The deadline is the server's: one already past on the seller's clock (a skewed
    clock) leaves the fixed budget, never an instant refusal; no deadline, the same."""
    from kontra.serve import handler as module

    monkeypatch.setattr(module, "_FETCH_BUDGET", 0.5)

    async def quick(payload: Payload) -> Payload:
        await asyncio.sleep(0.1)
        return Payload(metadata={"encoding": b"json/plain"}, data=b'{"text": "x"}')

    service.open_deferred = quick
    past = datetime.now(UTC) - timedelta(seconds=30)
    for deadline in (past, None):
        assert await service._read_input(_reference(10), deadline) == {"text": "x"}  # pyright: ignore[reportPrivateUsage]

    async def stuck(payload: Payload) -> Payload:
        await asyncio.sleep(30)
        return payload

    service.open_deferred = stuck
    began = time.monotonic()
    with pytest.raises(nexusrpc.HandlerError):
        await service._read_input(_reference(10), None)  # pyright: ignore[reportPrivateUsage]
    assert time.monotonic() - began < 1.5
