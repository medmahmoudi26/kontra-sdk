"""Hiring from inside a buyer's own workflow (PRD §7.2): ``service(offering).execute(...)``.

Workflow code cannot do I/O, so a hire is two steps. The activity ``kontra.hire.prepare``
(registered by :func:`kontra.hire.worker`) resolves the offering in the catalog, validates
the input against the published contract, quotes it, checks ``max_price`` and locks the
hold, and primes the worker's seller-key cache so the codec can encrypt the input to the
seller. Then the workflow itself is the Nexus caller: ``execute`` with the hold token in the
``kontra-hold`` header and ``schedule_to_close_timeout = max_duration`` (decisions I4).

When ``execute`` ends without a result (refused, failed, timed out), the gate's record
decides what the workflow sees, as in a script (decisions I6): the activity
``kontra.hire.settle`` waits for the gate to close the job and hands back the delivery if
the gate counted one -- the run the workflow's own call started can complete after the
operation timed out but before the gate's deadline, and the gate releases such a delivery to
the seller when the review window closes (a run the seller started itself is never a
delivery: decisions I15). Otherwise the workflow gets
:class:`~kontra.hire.JobFailed` (a non-retryable ``ApplicationError``) with the seller's
reason when it gave one. A start the seller's handler refused, and a cancellation that lands
before the seller started, run ``kontra.hire.cancel`` first so the hold refunds at once
instead of at its deadline. Once the job runs, cancelling the awaiting task (or the whole
buyer workflow) cancels the Nexus operation, which cancels the seller's workflow; the gate
observes that and refunds (§8.3).

This module runs inside workflow code: it imports only the standard library, temporalio,
nexusrpc, pydantic and the sandbox-safe parts of :mod:`kontra.hire`, and it is deterministic.
"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import timedelta
from decimal import Decimal, InvalidOperation
from typing import Any, Literal, cast

import nexusrpc
import pydantic_core
from pydantic import BaseModel, ConfigDict
from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import (
    ActivityError,
    ApplicationError,
    CancelledError,
    NexusOperationError,
    TimeoutError,
)

from kontra.hire._errors import InvalidDelivery, JobFailed, from_failure
from kontra.hire._record import to_json, wrap

__all__ = [
    "CANCEL_ACTIVITY",
    "EXECUTE_OPERATION",
    "HOLD_HEADER",
    "PREPARE_ACTIVITY",
    "SETTLE_ACTIVITY",
    "Outcome",
    "PrepareRequest",
    "Prepared",
    "ServiceClient",
    "Settled",
    "input_value",
    "outcome_of",
    "price_text",
    "service",
]

PREPARE_ACTIVITY = "kontra.hire.prepare"
CANCEL_ACTIVITY = "kontra.hire.cancel"
SETTLE_ACTIVITY = "kontra.hire.settle"
#: The Nexus header carrying the hold token (D10).
HOLD_HEADER = "kontra-hold"
EXECUTE_OPERATION = "execute"

#: A2A states a job ends in (``kontra.gate_client.models.JobState``, not importable here).
_CLOSED_STATES = frozenset({"completed", "failed", "canceled"})

_PREPARE_TIMEOUT = timedelta(seconds=60)
_PREPARE_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    maximum_interval=timedelta(seconds=10),
    maximum_attempts=5,
)
_CANCEL_TIMEOUT = timedelta(seconds=30)
_CANCEL_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    maximum_interval=timedelta(seconds=10),
    maximum_attempts=10,
)
#: The gate closes every job by its deadline (lock + max_duration + grace); when ``execute``
#: has ended, that is at most the grace away. The activity heartbeats while it waits.
_SETTLE_TIMEOUT = timedelta(minutes=15)
_SETTLE_HEARTBEAT = timedelta(seconds=30)
_SETTLE_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    maximum_interval=timedelta(seconds=30),
    maximum_attempts=5,
)


class PrepareRequest(BaseModel):
    """The input of ``kontra.hire.prepare``."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    offering: str
    input: dict[str, Any]
    #: A decimal string in the quote's currency, or None for no cap.
    max_price: str | None = None


class Prepared(BaseModel):
    """What ``kontra.hire.prepare`` hands the workflow: a locked hold, ready to execute."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    job_id: str
    offering: str
    hold_token: str
    endpoint: str
    service: str
    max_duration_seconds: int
    seller_public_key: str
    price: str
    currency: str
    #: The contract's self-contained output schema, to find artifacts in the delivery.
    output_schema: dict[str, Any]


class Settled(BaseModel):
    """What ``kontra.hire.settle`` reports: the gate's verdict on a job whose ``execute``
    ended without a result. ``delivery`` is the decrypted delivery when the gate counted one
    (checked against the contract); ``state`` is the A2A state the gate closed the job in
    (still open only if the gate never closed it in time)."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    state: str
    delivery: dict[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class Outcome:
    """Why a Nexus ``execute`` ended without a result."""

    kind: Literal["refused", "failed", "timed_out", "canceled"]
    reason: str

    @property
    def state(self) -> str:
        """The A2A state the job ends in (§8.1)."""
        return "canceled" if self.kind == "canceled" else "failed"


def outcome_of(err: BaseException) -> Outcome:
    """Classify a failed ``execute`` from its cause chain (client or workflow side):

    * a Nexus handler error: the seller's handler refused to start the job (a hold token or
      input it would not take). Nothing runs; the caller refunds at once.
    * a cancellation, a timeout (max_duration with no delivery), or the seller workflow's
      own failure, whose message is the seller's (decrypted: failures are encoded).
    """
    seen: set[int] = set()
    cur: BaseException | None = err.__cause__
    while cur is not None and id(cur) not in seen:
        seen.add(id(cur))
        if isinstance(cur, nexusrpc.HandlerError):
            return Outcome("refused", f"the seller refused to start the job: {cur}")
        if isinstance(cur, CancelledError):
            return Outcome("canceled", "the job was canceled")
        if isinstance(cur, TimeoutError):
            return Outcome("timed_out", "the seller did not deliver within max_duration")
        if isinstance(cur, ApplicationError) and not isinstance(cur, NexusOperationError):
            return Outcome("failed", cur.message or "the seller's job failed")
        cur = cur.__cause__
    return Outcome("failed", str(err) or "the seller's job failed")


def input_value(value: Any, fields: Mapping[str, Any]) -> dict[str, Any]:
    """The job input as JSON: a pydantic model, a mapping, or keyword arguments (not both).
    Models (an ``Artifact`` from ``Kontra.upload`` included) become their JSON form."""
    if value is not None and fields:
        raise TypeError("pass the input as one object or as keyword arguments, not both")
    raw = dict(fields) if value is None else value
    out = pydantic_core.to_jsonable_python(to_json(raw), by_alias=True)
    if not isinstance(out, dict):
        raise TypeError("an offering's input is a JSON object (a model, a dict or keywords)")
    return cast("dict[str, Any]", out)


def price_text(value: Decimal | int | float | str) -> str:
    """A max price as an exact decimal string (floats through ``str``, as the ledger does)."""
    if isinstance(value, bool):
        raise TypeError("max_price must be a number")
    try:
        d = Decimal(repr(value)) if isinstance(value, float) else Decimal(value)
    except (InvalidOperation, TypeError, ValueError):
        raise ValueError(f"max_price {value!r} is not a number") from None
    if not d.is_finite() or d < 0:
        raise ValueError(f"max_price {value!r} must be a finite number >= 0")
    return str(d)


class ServiceClient:
    """``service(offering, max_price=...)``: hire ``offering`` from a buyer's workflow."""

    def __init__(
        self, offering: str, *, max_price: Decimal | int | float | str | None = None
    ) -> None:
        self.offering = offering
        self.max_price = None if max_price is None else price_text(max_price)

    async def execute(self, input: Any = None, /, **kwargs: Any) -> Any:
        """Quote, hold and run the job; the seller's output as a :class:`~kontra.hire.Record`.

        Raises :class:`~kontra.hire.JobFailed` when the job ends without a delivery, and
        the hire errors (:class:`~kontra.hire.QuoteRejected`, ``SellerBusy``,
        ``PriceAboveMax``, ...) when nothing was locked."""
        value = input_value(input, kwargs)
        prepared = await self._prepare(value)
        return await self._run(prepared, value)

    async def _prepare(self, value: dict[str, Any]) -> Prepared:
        handle = workflow.start_activity(
            PREPARE_ACTIVITY,
            PrepareRequest(offering=self.offering, input=value, max_price=self.max_price),
            result_type=Prepared,
            start_to_close_timeout=_PREPARE_TIMEOUT,
            retry_policy=_PREPARE_RETRY,
        )
        try:
            return await asyncio.shield(handle)
        except asyncio.CancelledError:
            # The hold may be locked already: let the activity finish, refund what it locked.
            try:
                prepared = await handle
            except BaseException:  # it failed: nothing was locked
                raise asyncio.CancelledError() from None
            await _refund(prepared.job_id)
            raise
        except ActivityError as err:
            mapped = from_failure(err.cause) if err.cause is not None else None
            if mapped is not None:
                raise mapped from err
            raise

    async def _run(self, prepared: Prepared, value: dict[str, Any]) -> Any:
        nexus = workflow.create_nexus_client(service=prepared.service, endpoint=prepared.endpoint)
        try:
            handle = await nexus.start_operation(
                EXECUTE_OPERATION,
                value,
                headers={HOLD_HEADER: prepared.hold_token},
                schedule_to_close_timeout=timedelta(seconds=prepared.max_duration_seconds),
            )
        except asyncio.CancelledError:
            await _refund(prepared.job_id)  # never started: the gate would wait for the deadline
            raise
        except NexusOperationError as err:
            outcome = outcome_of(err)
            if outcome.kind == "canceled":
                await _refund(prepared.job_id)
                raise _failure(outcome, prepared.job_id) from err
            return await _settle(prepared, outcome, err)
        try:
            # Shielded: a cancellation of this task reaches the except clause at once,
            # instead of waiting for whatever the seller does with a Nexus cancel.
            out = await asyncio.shield(handle)
        except asyncio.CancelledError:
            # Cancel at the gate first, as Job.cancel() does: the hold refunds now and a
            # delivery after this is ignored. Nexus cancellation alone would let a seller
            # that meets the cancel by completing with a result be paid for a job the buyer
            # cancelled. Then cancel the operation, so the seller's handler is told too.
            await _refund(prepared.job_id)
            handle.cancel()
            raise
        except NexusOperationError as err:
            outcome = outcome_of(err)
            if outcome.kind == "canceled":
                raise _failure(outcome, prepared.job_id) from err
            return await _settle(prepared, outcome, err)
        return wrap(out, prepared.output_schema)


async def _settle(prepared: Prepared, outcome: Outcome, err: NexusOperationError) -> Any:
    """``execute`` ended without a result: the gate's verdict decides (module docstring).
    A refused start is cancelled first, so its hold refunds now. If the buyer's workflow is
    cancelled meanwhile, the job is cancelled at the gate (refunded unless delivered)."""
    job_id = prepared.job_id
    if outcome.kind == "refused":
        await _refund(job_id)
    try:
        settled = await workflow.execute_activity(
            SETTLE_ACTIVITY,
            job_id,
            result_type=Settled,
            start_to_close_timeout=_SETTLE_TIMEOUT,
            heartbeat_timeout=_SETTLE_HEARTBEAT,
            retry_policy=_SETTLE_RETRY,
        )
    except asyncio.CancelledError:
        await _refund(job_id)
        raise
    except ActivityError as activity_err:
        mapped = from_failure(activity_err.cause) if activity_err.cause is not None else None
        if isinstance(mapped, InvalidDelivery):
            raise mapped from err
        workflow.logger.warning("job %s: no verdict from the gate: %s", job_id, activity_err)
        raise JobFailed(outcome.reason, state=outcome.state, job_id=job_id) from err
    if settled.delivery is not None:
        workflow.logger.warning(  # %r: the reason is the seller's text
            "job %s: %r, yet the gate recorded a delivery; returning it", job_id, outcome.reason
        )
        return wrap(settled.delivery, prepared.output_schema)
    state = settled.state if settled.state in _CLOSED_STATES else outcome.state
    raise JobFailed(outcome.reason, state=state, job_id=job_id) from err


def _failure(outcome: Outcome, job_id: str) -> BaseException:
    """``JobFailed``, except that a job cancelled *because this task was cancelled* ends the
    task as cancelled (so a cancelled buyer workflow ends ``CANCELED``, not ``FAILED``)."""
    task = asyncio.current_task()
    if outcome.kind == "canceled" and task is not None and task.cancelling():
        return asyncio.CancelledError()
    return JobFailed(outcome.reason, state=outcome.state, job_id=job_id)


async def _refund(job_id: str) -> None:
    """Cancel the job at the gate (the hold refunds now), even if this task is cancelled
    again meanwhile. Best effort: if it cannot be done, the gate refunds at the deadline."""
    task = workflow.start_activity(
        CANCEL_ACTIVITY,
        job_id,
        start_to_close_timeout=_CANCEL_TIMEOUT,
        retry_policy=_CANCEL_RETRY,
    )
    try:
        await asyncio.shield(task)
    except asyncio.CancelledError:
        try:
            await task
        except Exception:
            workflow.logger.warning("could not cancel job %s; it refunds at its deadline", job_id)
        raise
    except Exception:
        workflow.logger.warning("could not cancel job %s; it refunds at its deadline", job_id)


def service(
    offering: str, *, max_price: Decimal | int | float | str | None = None
) -> ServiceClient:
    """A Nexus client with endpoint resolution, for use inside a buyer's workflow (§7.2).

    ``offering`` is a catalog reference (``acme/enrich``, ``acme/enrich@1``, exact).
    ``max_price`` is in the quote's currency; a higher quote locks nothing and raises
    :class:`~kontra.hire.PriceAboveMax`. The workflow's worker must come from
    :func:`kontra.hire.worker` (it runs the hire activities and carries the buyer codec).
    """
    return ServiceClient(offering, max_price=max_price)
