"""The ledger seam (PRD §9, seam ④).

One Protocol, several adapters: ``kontra.testing.fake_ledger()`` (in memory, here) and
the Postgres adapter in the platform; Stripe top-ups and a Solana adapter later. Every
adapter must pass ``kontra.testing.ledger_contract`` -- the §9.3 invariants -- unchanged.

The three migration-safe properties (§9.4) are fixed here, on day one:

1. Parties are keyed by ``party_id``; a payment address is an attribute, never the key.
2. Amounts are :class:`Money` -- ``(amount_minor: int, currency: str)``. Never a float.
3. Every call returns a :class:`Handle` that may be ``pending``; callers never assume
   synchronous confirmation.

Accounting is double-entry over two accounts per (party, currency): ``available`` and
``held``. Every movement is a set of :class:`Posting` rows whose amounts sum to zero, and a
balance is exactly the sum of its postings -- which is the §9.3 invariant
``sum(ledger_events) == sum(balances)``. Money enters only through :meth:`Ledger.deposit`
(a top-up, posted against the external ``world`` account) and leaves only through
:meth:`Ledger.withdraw`.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from decimal import ROUND_HALF_UP, Decimal
from enum import StrEnum
from typing import Protocol, runtime_checkable

__all__ = [
    "CURRENCIES",
    "MAX_MINOR",
    "PLATFORM_PARTY",
    "WORLD_PARTY",
    "Account",
    "Balance",
    "CurrencyMismatch",
    "Handle",
    "HandleState",
    "Hold",
    "HoldConflict",
    "HoldState",
    "IllegalTransition",
    "InsufficientFunds",
    "InvalidAmount",
    "InvalidParty",
    "InvalidSplit",
    "Ledger",
    "LedgerError",
    "Money",
    "Op",
    "Posting",
    "Split",
    "UnknownHold",
    "fee_for",
    "next_state",
]

#: ISO-4217-style code -> number of minor-unit digits. ``USDC`` is listed for the chain
#: adapter (§9.1); v1 settles only ``USD``.
CURRENCIES: Mapping[str, int] = {"USD": 2, "USDC": 6}

#: The largest ``amount_minor`` a call may carry: §9.1 stores amounts as a Postgres
#: ``bigint``. A price is the seller's to choose (its quote), so an adapter refuses anything
#: larger with ``InvalidAmount`` rather than failing in its database driver, and every
#: adapter refuses the same amounts (acceptance criterion 9).
MAX_MINOR = 2**63 - 1

#: Receives the platform fee at release (§9.5). A reserved handle; never a buyer or seller.
PLATFORM_PARTY = "kontra"
#: The outside world: the counter-account of deposits and withdrawals. Its balance is
#: negative by exactly the money inside the system, so all accounts sum to zero.
WORLD_PARTY = "kontra-world"


class LedgerError(Exception):
    """Base class. Every refusal is one of the subclasses below."""


class IllegalTransition(LedgerError):
    def __init__(self, job_id: str, state: HoldState | None, op: Op) -> None:
        self.job_id, self.state, self.op = job_id, state, op
        super().__init__(
            f"hold {job_id}: cannot {op.value} from {state.value if state else 'no hold'}"
        )


class InsufficientFunds(LedgerError):
    pass


class HoldConflict(LedgerError):
    """An idempotency key reused with different arguments: ``lock`` repeated for a job id
    with different parties or amount, ``resolve`` with a different split, or a deposit /
    withdrawal ``ref`` reused for a different movement."""


class UnknownHold(LedgerError):
    pass


class InvalidSplit(LedgerError):
    pass


class InvalidAmount(LedgerError):
    pass


class InvalidParty(LedgerError):
    """An empty party id, or a reserved account in a role it cannot take: ``WORLD_PARTY``
    is only ever the counter-account of deposits and withdrawals, and ``PLATFORM_PARTY``
    only receives fees (and may withdraw them), so it is never a hold's buyer or seller."""


class CurrencyMismatch(LedgerError):
    pass


@dataclass(frozen=True, slots=True)
class Money:
    amount_minor: int
    currency: str

    def __post_init__(self) -> None:
        if isinstance(self.amount_minor, bool) or not isinstance(self.amount_minor, int):  # pyright: ignore[reportUnnecessaryIsInstance]
            raise InvalidAmount(
                f"amount_minor must be an int, got {type(self.amount_minor).__name__}"
            )
        if self.currency not in CURRENCIES:
            raise InvalidAmount(f"unsupported currency {self.currency!r}")

    @classmethod
    def from_decimal(cls, amount: Decimal | int | float | str, currency: str) -> Money:
        """Seller-facing prices may be written as ``12.4``; the ledger never sees that.
        Rounds half-up to the currency's minor unit. Floats go through ``str`` so that
        ``0.1 * 3`` becomes ``0.3``, not ``0.30000000000000004``."""
        if currency not in CURRENCIES:
            raise InvalidAmount(f"unsupported currency {currency!r}")
        if isinstance(amount, float):
            amount = repr(amount)
        d = Decimal(amount)
        if not d.is_finite():
            raise InvalidAmount(f"price {amount!r} is not a finite number")
        exp = CURRENCIES[currency]
        minor = (d * (Decimal(10) ** exp)).quantize(Decimal(1), rounding=ROUND_HALF_UP)
        return cls(int(minor), currency)

    def to_decimal(self) -> Decimal:
        return Decimal(self.amount_minor).scaleb(-CURRENCIES[self.currency])

    def display(self) -> str:
        """``12.40 USD``."""
        exp = CURRENCIES[self.currency]
        return f"{self.to_decimal():.{exp}f} {self.currency}"


class HoldState(StrEnum):
    HELD = "held"
    RELEASED = "released"
    REFUNDED = "refunded"
    DISPUTED = "disputed"
    RESOLVED = "resolved"


class Op(StrEnum):
    LOCK = "lock"
    RELEASE = "release"
    REFUND = "refund"
    DISPUTE = "dispute"
    RESOLVE = "resolve"
    #: Money in and out (``Handle.op`` of a deposit or withdrawal). Not hold transitions:
    #: :func:`next_state` refuses them.
    DEPOSIT = "deposit"
    WITHDRAW = "withdraw"


#: The whole hold state machine. Anything not in this table is an IllegalTransition.
_TRANSITIONS: Mapping[tuple[HoldState | None, Op], HoldState] = {
    (None, Op.LOCK): HoldState.HELD,
    (HoldState.HELD, Op.RELEASE): HoldState.RELEASED,
    (HoldState.HELD, Op.REFUND): HoldState.REFUNDED,
    (HoldState.HELD, Op.DISPUTE): HoldState.DISPUTED,
    (HoldState.DISPUTED, Op.RESOLVE): HoldState.RESOLVED,
}


def next_state(state: HoldState | None, op: Op) -> HoldState:
    """The state after ``op``, or :class:`IllegalTransition` (job id filled by the caller)."""
    try:
        return _TRANSITIONS[(state, op)]
    except KeyError:
        raise IllegalTransition("?", state, op) from None


def fee_for(amount_minor: int, fee_bps: int) -> int:
    """Platform fee on an amount paid to a seller: floor(amount * bps / 10_000)."""
    if amount_minor < 0 or fee_bps < 0 or fee_bps > 10_000:
        raise InvalidAmount("fee inputs out of range")
    return amount_minor * fee_bps // 10_000


class HandleState(StrEnum):
    PENDING = "pending"
    CONFIRMED = "confirmed"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class Handle:
    """The result of a ledger call. ``ref`` is opaque (an event id here, a transaction
    signature on a chain). A repeated call returns the original handle, ``ref`` included."""

    job_id: str
    op: Op
    state: HandleState
    ref: str


@dataclass(frozen=True, slots=True)
class Split:
    """How a disputed hold is divided by the resolver. Must sum to the held amount. The
    platform fee applies to the seller's part only."""

    buyer_minor: int
    seller_minor: int

    def __post_init__(self) -> None:
        for v in (self.buyer_minor, self.seller_minor):
            if isinstance(v, bool) or not isinstance(v, int) or v < 0:  # pyright: ignore[reportUnnecessaryIsInstance]
                raise InvalidSplit("split parts must be non-negative ints")


class Account(StrEnum):
    AVAILABLE = "available"
    HELD = "held"


@dataclass(frozen=True, slots=True)
class Posting:
    """One leg of a ledger event (the PRD's ``ledger_events`` row, with the party and
    account it moves). The legs of one event sum to zero."""

    event_id: int
    job_id: str | None
    kind: str  # lock | release | fee | refund | dispute | resolve | deposit | withdraw
    party_id: str
    account: Account
    amount_minor: int  # signed
    currency: str
    at: datetime
    actor: str
    ref: str


@dataclass(frozen=True, slots=True)
class Balance:
    party_id: str
    currency: str
    available_minor: int
    held_minor: int


@dataclass(frozen=True, slots=True)
class Hold:
    job_id: str
    buyer: str
    seller: str
    amount: Money
    state: HoldState
    created_at: datetime
    expires_at: datetime
    handles: Mapping[Op, Handle] = field(default_factory=dict[Op, Handle])


@runtime_checkable
class Ledger(Protocol):
    """§9.2. Every call is keyed by ``job_id`` (or ``ref`` for deposits) and idempotent:
    a repeat is a no-op that returns the original :class:`Handle`. Every transition is
    checked against :func:`next_state`. No call can move funds between two parties who
    are not the hold's buyer and seller, except the fee to :data:`PLATFORM_PARTY`."""

    async def lock(
        self, job_id: str, buyer: str, seller: str, amount: Money, *, expires_at: datetime
    ) -> Handle: ...

    async def release(self, job_id: str) -> Handle: ...

    async def refund(self, job_id: str) -> Handle: ...

    async def dispute(self, job_id: str) -> Handle: ...

    async def resolve(self, job_id: str, split: Split) -> Handle: ...

    # Beyond §9.2: money in and out, and reads. Same idempotency rule, keyed by ``ref``.
    async def deposit(self, party_id: str, amount: Money, *, ref: str) -> Handle: ...

    async def withdraw(self, party_id: str, amount: Money, *, ref: str) -> Handle: ...

    async def balance(self, party_id: str, currency: str) -> Balance: ...

    async def get_hold(self, job_id: str) -> Hold | None: ...

    async def postings(self) -> list[Posting]:
        """Every posting, in event order. For audits and the §9.3 property test."""
        ...

    async def balances(self) -> list[Balance]:
        """Every (party, currency) balance row."""
        ...
