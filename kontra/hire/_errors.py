"""What can go wrong when hiring, as exceptions shared by every door (PRD §7: one semantics).

Every error is a :class:`KontraError`, which is a non-retryable Temporal
``ApplicationError``: raised inside a buyer's workflow it fails the workflow (not just a
workflow task, which Temporal would retry forever), and raised from the hire activity it
reaches the workflow as a failure of type ``kontra.<Name>`` that :func:`from_failure` turns
back into the same class. In a script they are ordinary exceptions.

This module runs inside workflow code: it imports only the standard library and temporalio.
"""

from __future__ import annotations

from datetime import timedelta
from decimal import Decimal, InvalidOperation
from typing import Any, ClassVar

from temporalio.exceptions import ApplicationError

__all__ = [
    "HireRefused",
    "InsufficientFunds",
    "InvalidDelivery",
    "InvalidInput",
    "JobFailed",
    "KontraError",
    "PriceAboveMax",
    "QuoteRejected",
    "SellerBusy",
    "from_failure",
]

_PREFIX = "kontra."


class KontraError(ApplicationError):
    """Base of every hire error. ``str(e)`` is the human-readable message."""

    #: Errors of this class travel through Temporal as an ``ApplicationError`` of this type.
    wire_type: ClassVar[str] = _PREFIX + "KontraError"

    def __init__(self, message: str, **fields: Any) -> None:
        super().__init__(message, fields, type=self.wire_type, non_retryable=True)
        self.fields: dict[str, Any] = fields

    def __str__(self) -> str:
        return self.message

    @classmethod
    def _restore(cls, message: str, fields: dict[str, Any]) -> KontraError:
        return cls(message, **fields)


class QuoteRejected(KontraError):
    """The seller will not take this input (its ``quote`` said ``rejected``)."""

    wire_type = _PREFIX + "QuoteRejected"

    def __init__(self, message: str, *, offering: str = "", reason: str = "") -> None:
        super().__init__(message, offering=offering, reason=reason)
        self.offering, self.reason = offering, reason


class SellerBusy(KontraError):
    """The offering is at its concurrency limit, or the seller asked to come back later."""

    wire_type = _PREFIX + "SellerBusy"

    def __init__(self, message: str, *, offering: str = "", eta_seconds: int | None = None) -> None:
        super().__init__(message, offering=offering, eta_seconds=eta_seconds)
        self.offering = offering
        self.eta: timedelta | None = (
            timedelta(seconds=eta_seconds) if eta_seconds is not None else None
        )


class PriceAboveMax(KontraError):
    """The quote is above the buyer's ``max_price``: nothing was locked."""

    wire_type = _PREFIX + "PriceAboveMax"

    def __init__(
        self,
        message: str,
        *,
        offering: str = "",
        price: str = "0",
        max_price: str = "0",
        currency: str = "",
    ) -> None:
        super().__init__(
            message, offering=offering, price=price, max_price=max_price, currency=currency
        )
        self.offering, self.currency = offering, currency
        self.price, self.max_price = _decimal(price), _decimal(max_price)


class InsufficientFunds(KontraError):
    """The buyer's available balance does not cover the price."""

    wire_type = _PREFIX + "InsufficientFunds"

    def __init__(self, message: str, *, offering: str = "") -> None:
        super().__init__(message, offering=offering)
        self.offering = offering


class HireRefused(KontraError):
    """The gate refused a quote or hire for a reason the buyer must act on (no such
    offering, the quote expired, the offering was retired...). ``code`` is the gate's."""

    wire_type = _PREFIX + "HireRefused"

    def __init__(self, message: str, *, code: str = "", status: int = 0) -> None:
        super().__init__(message, code=code, status=status)
        self.code, self.status = code, status


class InvalidInput(KontraError):
    """The input does not match the offering's published contract; nothing was sent.
    ``path`` names the first offending place (``input.urls[3]``)."""

    wire_type = _PREFIX + "InvalidInput"

    def __init__(self, message: str, *, offering: str = "", path: str = "") -> None:
        super().__init__(message, offering=offering, path=path)
        self.offering, self.path = offering, path


class JobFailed(KontraError):
    """A hired job ended without a delivery: the seller refused to start it, its workflow
    failed or timed out, or it was canceled. It is raised once the gate has closed the job
    without counting a delivery, so its hold is refunded (§8.3); when the gate could not be
    asked, the gate still refunds by the job's deadline unless it saw a delivery.

    ``state`` is the A2A state (``failed`` or ``canceled``); ``reason`` says why, in the
    seller's words when the seller gave any (failure messages are encrypted to the buyer).
    """

    wire_type = _PREFIX + "JobFailed"

    def __init__(self, reason: str, *, state: str = "failed", job_id: str | None = None) -> None:
        super().__init__(reason, state=state, job_id=job_id)
        self.reason, self.state, self.job_id = reason, state, job_id

    @classmethod
    def _restore(cls, message: str, fields: dict[str, Any]) -> KontraError:
        return cls(
            message,
            state=str(fields.get("state", "failed")),
            job_id=fields.get("job_id") if isinstance(fields.get("job_id"), str) else None,
        )


class InvalidDelivery(KontraError):
    """The delivery does not match the offering's published output schema. The raw value
    is on ``value`` (a script can still read it, and ``job.reject(...)`` it)."""

    wire_type = _PREFIX + "InvalidDelivery"

    def __init__(self, message: str, *, job_id: str | None = None, value: Any = None) -> None:
        super().__init__(message, job_id=job_id)
        self.job_id, self.value = job_id, value


_BY_TYPE: dict[str, type[KontraError]] = {
    c.wire_type: c
    for c in (
        KontraError,
        QuoteRejected,
        SellerBusy,
        PriceAboveMax,
        InsufficientFunds,
        HireRefused,
        InvalidInput,
        JobFailed,
        InvalidDelivery,
    )
}


def _decimal(value: object) -> Decimal:
    try:
        return Decimal(str(value))
    except InvalidOperation:
        return Decimal(0)


def from_failure(err: BaseException) -> KontraError | None:
    """The :class:`KontraError` an ``ApplicationError`` carries across Temporal (an activity
    failure's cause, a child or Nexus failure), or None if it is not one of ours."""
    if isinstance(err, KontraError):
        return err
    if not isinstance(err, ApplicationError) or err.type not in _BY_TYPE:
        return None
    raw = err.details[0] if err.details else {}
    fields: dict[str, Any] = (
        {str(k): v for k, v in raw.items()}  # pyright: ignore[reportUnknownVariableType]
        if isinstance(raw, dict)
        else {}
    )
    cls = _BY_TYPE[err.type]
    try:
        return cls._restore(err.message, fields)
    except TypeError:  # fields from a newer SDK: keep the class and the message
        return cls._restore(err.message, {})
