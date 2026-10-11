"""The generated service, without Temporal: what ``quote`` answers, and what a seller bug
in ``quote`` becomes (never a leak of the input, never a retry loop)."""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path
from typing import Any

import nexusrpc
import pytest

from kontra.codec.age import generate_identity
from kontra.contract import ContractError, Quote
from kontra.serve import BoundOffering, HoldVerifier, JobDirectory, OfferingService
from kontra.serve.handler import service_handler

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
        contract_hash=bound.contract_hash(PK),
        validator=bound.validator(PK),
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
