"""Quotes: the seller-facing :class:`Quote` and the wire model :class:`KontraQuote`.

A seller's ``quote`` function returns ``Quote(price=12.4)``, ``Quote.reject(reason)`` or
``Quote.busy(eta)``. The handler converts it with :meth:`Quote.to_wire` into a
``KontraQuote`` -- the body of ``#/$defs/KontraQuote`` in ``spec/kontra-types.v1.json`` --
which is what crosses Nexus, what the gate reads to lock a hold, and what nexgen-generated
clients in other languages decode. Amounts are integer minor units (§9.4): the float a
seller writes never reaches the wire.
"""

from __future__ import annotations

import math
from datetime import timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    SerializationInfo,
    SerializerFunctionWrapHandler,
    model_serializer,
    model_validator,
)

from kontra.contract.canonical import MAX_SAFE_INTEGER
from kontra.ledger import CURRENCIES, InvalidAmount, Money

if TYPE_CHECKING:
    from kontra.contract.offering import OfferingSpec

__all__ = ["MAX_ETA_SECONDS", "KontraQuote", "Quote", "QuoteError", "QuoteStatus"]

QuoteStatus = Literal["ok", "rejected", "busy"]
#: Mirrors ``maximum`` in the spec: 30 days.
MAX_ETA_SECONDS = 30 * 86_400
_MAX_DURATION_SECONDS = 30 * 86_400
_REASON_MAX = 1000


class QuoteError(ValueError):
    """A quote that cannot be put on the wire (negative price, longer than the offering
    allows, an unsupported currency, ...). A seller bug, reported to the seller."""


class KontraQuote(BaseModel):
    """``#/$defs/KontraQuote``. Absent members are omitted on the wire, never ``null``:
    the spec declares them optional, not nullable, as every nexgen target expects."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    status: QuoteStatus
    #: At most 2^53-1 (the spec's maximum): the gate locks this amount, and JSON numbers
    #: beyond it lose precision in JavaScript and overflow a Postgres bigint downstream.
    amount_minor: int | None = Field(default=None, ge=0, le=MAX_SAFE_INTEGER, strict=True)
    currency: str | None = Field(default=None, pattern=r"^[A-Z][A-Z0-9]{2,7}$")
    max_duration_seconds: int | None = Field(
        default=None, ge=1, le=_MAX_DURATION_SECONDS, strict=True
    )
    reason: str | None = Field(default=None, min_length=1, max_length=_REASON_MAX)
    eta_seconds: int | None = Field(default=None, ge=0, le=MAX_ETA_SECONDS, strict=True)

    @model_validator(mode="after")
    def _consistent(self) -> KontraQuote:
        """JSON Schema cannot say "amount when ok" without ``if``/``then``, which nexgen
        refuses; the rule lives here and in the spec's description."""
        priced = (self.amount_minor, self.currency, self.max_duration_seconds)
        if self.status == "ok":
            if any(v is None for v in priced):
                raise ValueError(
                    "an ok quote needs amount_minor, currency and max_duration_seconds"
                )
            if self.currency not in CURRENCIES:
                # The quote comes from the seller's process; the gate must be able to
                # lock it, so a currency the ledger does not hold is not a price.
                raise ValueError(
                    f"currency {self.currency!r} is not one the ledger holds "
                    f"({', '.join(sorted(CURRENCIES))})"
                )
            if self.reason is not None or self.eta_seconds is not None:
                raise ValueError("an ok quote carries no reason or eta_seconds")
        else:
            if any(v is not None for v in priced):
                raise ValueError(f"a {self.status} quote carries no price")
            if self.status == "rejected":
                if self.reason is None:
                    raise ValueError("a rejected quote needs a reason")
                if self.eta_seconds is not None:
                    raise ValueError("a rejected quote carries no eta_seconds")
            elif self.eta_seconds is None:
                raise ValueError("a busy quote needs eta_seconds")
        return self

    @model_serializer(mode="wrap")
    def _omit_absent(
        self, handler: SerializerFunctionWrapHandler, info: SerializationInfo
    ) -> dict[str, Any]:
        data: dict[str, Any] = handler(self)
        return {k: v for k, v in data.items() if v is not None}

    @property
    def money(self) -> Money | None:
        if self.amount_minor is None or self.currency is None:
            return None
        return Money(self.amount_minor, self.currency)


class Quote:
    """What a seller's ``quote`` function returns.

    ``Quote(price=12.4)`` -- a fixed price in ``currency`` (default USD), rounded half-up
    to the minor unit. ``max_duration`` caps this job below the offering's limit (it can
    never exceed it). ``Quote.reject(reason)`` refuses the input; ``Quote.busy(eta)`` asks
    the buyer to come back later.
    """

    __slots__ = ("_eta", "_max_duration", "_money", "_reason", "_status")

    _status: QuoteStatus
    _money: Money | None
    _max_duration: timedelta | None
    _reason: str | None
    _eta: timedelta | None

    def __init__(
        self,
        price: Decimal | int | float | str,
        currency: str = "USD",
        max_duration: timedelta | None = None,
    ) -> None:
        if isinstance(price, bool):
            raise QuoteError("price must be a number, not a bool")
        try:
            money = Money.from_decimal(price, currency)
        except (InvalidAmount, ArithmeticError, ValueError) as exc:
            raise QuoteError(f"price {price!r} {currency}: {exc}") from None
        if money.amount_minor < 0:
            raise QuoteError(f"price must not be negative, got {price!r}")
        if money.amount_minor > MAX_SAFE_INTEGER:
            raise QuoteError(f"price {price!r} {currency} is too large to quote")
        if max_duration is not None:
            if not isinstance(max_duration, timedelta):  # pyright: ignore[reportUnnecessaryIsInstance]
                raise QuoteError("max_duration must be a timedelta")
            if max_duration <= timedelta(0):
                raise QuoteError("max_duration must be positive")
        self._init("ok", money=money, max_duration=max_duration)

    def _init(
        self,
        status: QuoteStatus,
        *,
        money: Money | None = None,
        max_duration: timedelta | None = None,
        reason: str | None = None,
        eta: timedelta | None = None,
    ) -> None:
        object.__setattr__(self, "_status", status)
        object.__setattr__(self, "_money", money)
        object.__setattr__(self, "_max_duration", max_duration)
        object.__setattr__(self, "_reason", reason)
        object.__setattr__(self, "_eta", eta)

    def __setattr__(self, name: str, value: object) -> None:
        raise AttributeError("Quote is immutable")

    @classmethod
    def reject(cls, reason: str) -> Quote:
        reason = reason.strip() if isinstance(reason, str) else reason  # pyright: ignore[reportUnnecessaryIsInstance]
        if not isinstance(reason, str) or not reason:  # pyright: ignore[reportUnnecessaryIsInstance]
            raise QuoteError("a rejection needs a reason the buyer can act on")
        q = cls.__new__(cls)
        q._init("rejected", reason=reason[:_REASON_MAX])
        return q

    @classmethod
    def busy(cls, eta: timedelta | float, reason: str | None = None) -> Quote:
        """``eta``: when to try again, as a timedelta or seconds."""
        if isinstance(eta, bool) or not isinstance(eta, timedelta | int | float):  # pyright: ignore[reportUnnecessaryIsInstance]
            raise QuoteError("eta must be a timedelta or a number of seconds")
        if isinstance(eta, timedelta):
            td = eta
        elif not math.isfinite(eta):
            raise QuoteError(f"eta must be a finite number of seconds, got {eta!r}")
        else:
            td = timedelta(seconds=min(eta, MAX_ETA_SECONDS + 1))
        if td < timedelta(0):
            raise QuoteError("eta must not be negative")
        text = reason.strip()[:_REASON_MAX] if isinstance(reason, str) else None
        q = cls.__new__(cls)
        q._init("busy", eta=td, reason=text or None)
        return q

    @property
    def status(self) -> QuoteStatus:
        return self._status

    @property
    def money(self) -> Money | None:
        return self._money

    @property
    def price(self) -> Decimal | None:
        return self._money.to_decimal() if self._money else None

    @property
    def currency(self) -> str | None:
        return self._money.currency if self._money else None

    @property
    def max_duration(self) -> timedelta | None:
        return self._max_duration

    @property
    def reason(self) -> str | None:
        return self._reason

    @property
    def eta(self) -> timedelta | None:
        return self._eta

    def to_wire(self, spec: OfferingSpec) -> KontraQuote:
        """The wire form for this offering. ``max_duration`` defaults to the offering's
        and may not exceed it: the hold and the Nexus timeout are both cut from it."""
        if self._status == "rejected":
            return KontraQuote(status="rejected", reason=self._reason)
        if self._status == "busy":
            assert self._eta is not None
            eta = min(math.ceil(self._eta.total_seconds()), MAX_ETA_SECONDS)
            return KontraQuote(status="busy", eta_seconds=eta, reason=self._reason)
        assert self._money is not None
        limit = spec.max_duration
        chosen = self._max_duration if self._max_duration is not None else limit
        if chosen > limit:
            raise QuoteError(
                f"quote max_duration {chosen} exceeds the offering's max_duration {limit}"
            )
        seconds = max(1, math.ceil(chosen.total_seconds()))
        return KontraQuote(
            status="ok",
            amount_minor=self._money.amount_minor,
            currency=self._money.currency,
            max_duration_seconds=seconds,
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Quote):
            return NotImplemented
        return (self._status, self._money, self._max_duration, self._reason, self._eta) == (
            other._status,
            other._money,
            other._max_duration,
            other._reason,
            other._eta,
        )

    def __hash__(self) -> int:
        return hash((self._status, self._money, self._max_duration, self._reason, self._eta))

    def __repr__(self) -> str:
        if self._status == "ok":
            assert self._money is not None
            extra = f", max_duration={self._max_duration!r}" if self._max_duration else ""
            return f"Quote(price={self._money.display()!r}{extra})"
        if self._status == "rejected":
            return f"Quote.reject({self._reason!r})"
        return f"Quote.busy({self._eta!r})"
