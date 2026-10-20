"""The Nexus service Kontra generates for each bound offering (PRD §6.5 items 1-2, D2).

Service ``acme.enrich`` (:attr:`OfferingId.service_name`), two operations:

* ``quote`` (synchronous): validate the input against the contract, run the seller's
  ``quote``, answer a :class:`~kontra.contract.KontraQuote`. The answer is plaintext by
  design: it is a price the gate must read to lock it (decisions I4). An input the contract
  refuses gets a *rejected* quote rather than a handler error, so the buyer sees why and the
  gate does not count it as the seller failing.
* ``execute`` (workflow-run): verify the hold token offline, check every claim against what
  this worker serves, refuse an input larger than the one the hold was quoted for before
  fetching any of it (decisions I16), validate the input, re-run the seller's ``quote`` and
  refuse if the price went up or the currency changed (decisions I10) -- or if this input
  needs longer than the hold's max_duration, or than is left before the hold expires --,
  record the buyer's key, then start the bound workflow (for a bound handler: the turn
  wrapper, :mod:`kontra.serve.turn`) as ``job-<id>`` with a timeout that
  ends by the gate's refund deadline (the hold's ``deadline``; without one, its expiry plus
  the escrow grace). Every refusal happens before any workflow
  exists, as a non-retryable ``BAD_REQUEST`` whose message names what failed -- never an
  input value or the token (refusals travel in plaintext; the codec deliberately leaves
  this operation's replies unencrypted so a buyer can read them).

One Nexus attempt (about 10 s on server 1.32) is all a handler has: one that has not
answered by then is recorded exactly like a seller with no worker (``UPSTREAM_TIMEOUT``) and
the seller is blamed. So ``execute`` answers within the attempt for any input: what it
fetches and checks is bounded by what the seller already answered a quote for within the
gate's quote timeout, and a fetch that would outlast the attempt is abandoned with a
refusal.

The input is validated as raw JSON: validating the raw value is what refuses an extra field
that a lenient pydantic model would silently drop (§11.3). ``quote`` takes it as ``Any``.
``execute`` takes the payload itself (``RawValue``) and converts it only after the hold
check: the codec leaves a claim-checked input unfetched there
(:meth:`kontra.codec.KontraCodec.open_deferred`), so a forged ``execute`` naming a huge blob
makes this worker download nothing.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any, TypeVar

import nexusrpc
import nexusrpc.handler
from nexusrpc.handler import StartOperationContext
from pydantic import BaseModel, ValidationError
from temporalio import nexus
from temporalio.api.common.v1 import Payload
from temporalio.common import RawValue, WorkflowIDConflictPolicy, WorkflowIDReusePolicy
from temporalio.contrib.pydantic import PydanticPayloadConverter
from temporalio.converter import PayloadConverter
from temporalio.exceptions import WorkflowAlreadyStartedError
from temporalio.nexus import WorkflowRunOperationContext

from kontra._control import input_size
from kontra.codec import InvalidPayload, age
from kontra.contract import ContractError, InputValidator, KontraQuote, Quote, QuoteError
from kontra.contract.names import is_job_id, job_workflow_id
from kontra.escrow import DEFAULT_GRACE
from kontra.gate_client import models as m
from kontra.ledger import Money
from kontra.serve.hold import HOLD_HEADER, HoldRefused, HoldVerifier
from kontra.serve.jobs import JobDirectory
from kontra.serve.offering import BoundOffering

__all__ = ["INPUT_SLACK", "OfferingService", "service_handler"]

log = logging.getLogger("kontra.serve.handler")
_T = TypeVar("_T")
_LOGGED_PATH = 200
#: Bytes an ``execute`` input may exceed the hold's ``input_size`` by. The same input
#: encrypted again for ``execute`` has exactly the quoted size (age's overhead is fixed for
#: the same recipients); this absorbs a client that encodes it slightly differently (another
#: recipient stanza, metadata), far below anything that changes how long a check takes.
INPUT_SLACK = 4096
#: What a fetch leaves of the Nexus request's deadline for the rest of ``execute`` (decrypt,
#: validate, re-quote, start the workflow, answer).
_ANSWER_RESERVE = 2.0
#: The longest a claim-checked input's fetch may take, whatever the request's deadline says:
#: an attempt is about 10 s, and the deadline is the server's clock read on the seller's.
_FETCH_BUDGET = 6.0


#: What a handler error carries instead of a Python stack trace. The reply travels in
#: plaintext to the caller's history (and the platform's): a formatted traceback would hand
#: both the seller's file system layout (home directory, user, Python install).
NO_TRACE = "(no stack trace: the seller's worker logs the details)"


def _refuse(message: str) -> nexusrpc.HandlerError:
    return nexusrpc.HandlerError(
        message,
        type=nexusrpc.HandlerErrorType.BAD_REQUEST,
        retryable_override=False,
        stack_trace=NO_TRACE,
    )


def _seller_bug(message: str) -> nexusrpc.HandlerError:
    """The seller's own code is wrong: retrying the same input will not help."""
    return nexusrpc.HandlerError(
        message,
        type=nexusrpc.HandlerErrorType.INTERNAL,
        retryable_override=False,
        stack_trace=NO_TRACE,
    )


async def _guarded(key: str, op: str, call: Awaitable[_T]) -> _T:
    """``call``, with every failure turned into a handler error that says nothing about the
    seller's machine. Ours keep their message; anything else (a Temporal RPC error while
    starting the job, say) is logged here and answered with a generic, retryable error, as
    temporalio would, minus the exception text and the traceback."""
    try:
        return await call
    except nexusrpc.HandlerError as err:
        if not err.stack_trace:
            err.stack_trace = NO_TRACE
        raise
    except nexusrpc.OperationError:
        raise  # an operation outcome (failed / canceled), not a handler failure
    except Exception:
        log.exception("%s %s failed", key, op)
        raise nexusrpc.HandlerError(
            f"{key} could not handle {op} just now; it may succeed if retried",
            type=nexusrpc.HandlerErrorType.INTERNAL,
            stack_trace=NO_TRACE,
        ) from None


def _fetch_budget(deadline: datetime | None) -> float:
    """Seconds a fetch may take: :data:`_FETCH_BUDGET`, or less when the request's deadline
    is nearer. A deadline already past on this machine's clock says the clocks disagree
    (it is the server's), not that the time is up: the fixed budget then stands."""
    budget = _FETCH_BUDGET
    if deadline is not None:
        left = (deadline - datetime.now(UTC)).total_seconds() - _ANSWER_RESERVE
        if left > 0:
            budget = min(budget, left)
    return budget


@dataclass
class OfferingService:
    """Everything the two operations need for one bound offering, as served by this worker."""

    bound: BoundOffering
    #: The worker's namespace and party: a hold token must name them as the seller.
    namespace: str
    party: str
    #: The hash of the contract this worker serves (§11.2).
    contract_hash: str
    validator: InputValidator
    verifier: HoldVerifier
    jobs: JobDirectory
    clock: Callable[[], float] = time.time
    #: Opens the claim-check the codec left in an ``execute`` input until the hold token was
    #: verified (:meth:`kontra.codec.KontraCodec.open_deferred`). None: no codec, so a
    #: claim-checked input cannot be read and is refused.
    open_deferred: Callable[[Payload], Awaitable[Payload]] | None = None
    #: Turns the opened ``execute`` input into its JSON value, as temporalio would have for
    #: an ``Any`` input (the worker passes its data converter's).
    payload_converter: PayloadConverter = field(default_factory=PydanticPayloadConverter)
    #: The escrow grace this handler assumes past the hold's expiry. The run's end is the
    #: hold's ``deadline`` claim (the gate's own refund deadline) when the token carries it,
    #: else the expiry plus this. Either way a start may come this long past the expiry
    #: minus the quoted duration: a job quoted for its whole max_duration always starts a
    #: moment after the hire.
    grace: timedelta = DEFAULT_GRACE

    @property
    def key(self) -> str:
        return self.bound.key

    # ------------------------------------------------------------------ shared steps

    def _parse(self, raw: Any) -> BaseModel:
        """The raw JSON input, validated against the contract, as the workflow's model.
        Raises :class:`ContractError` (refusals name paths only, never values)."""
        self.validator.validate(raw)
        try:
            # JSON mode, as the payload converter would read it: strict models accept a
            # date string exactly when the wire would.
            return self.bound.input_type.model_validate_json(json.dumps(raw))
        except ValidationError as exc:
            raise ContractError(
                "input",
                f"the contract accepts it but {self.bound.input_type.__name__} does not "
                f"({exc.error_count()} error(s))",
            ) from None

    def _log_invalid(self, op: str, exc: ContractError) -> None:
        # Paths stay on the seller's machine, which sees the input anyway. A path can hold
        # a map key, i.e. buyer data: repr() and a length cap keep a crafted key from
        # forging log lines or flooding the log.
        log.warning(
            "%s %s: input refused at %r",
            self.key,
            op,
            [path[:_LOGGED_PATH] for path, _ in exc.errors[:10]],
        )

    async def _price(self, inp: BaseModel) -> KontraQuote:
        """The seller's quote on ``inp``, on the wire. A seller bug is a non-retryable
        handler error; its details go to the seller's log, not to the caller."""
        try:
            answer = await self.bound.quote(inp)
        except Exception:
            log.exception("%s: the quote function raised", self.key)
            raise _seller_bug(f"the seller's quote function for {self.key} failed") from None
        if not isinstance(answer, Quote):  # pyright: ignore[reportUnnecessaryIsInstance]
            log.error("%s: quote returned %s, not a kontra.serve.Quote", self.key, type(answer))
            raise _seller_bug(f"the seller's quote function for {self.key} returned no Quote")
        try:
            return answer.to_wire(self.bound.spec)
        except QuoteError as exc:
            log.error("%s: the quote cannot be sent: %s", self.key, exc)
            raise _seller_bug(f"the seller's quote for {self.key} is invalid: {exc}") from None

    # ------------------------------------------------------------------ quote

    async def quote(self, raw: Any) -> KontraQuote:
        try:
            inp = self._parse(raw)
        except ContractError as exc:
            self._log_invalid("quote", exc)
            return KontraQuote(
                status="rejected",
                reason=(
                    f"the input does not match the contract of {self.key} "
                    f"({len(exc.errors)} problem(s)); validate it against the published schema"
                ),
            )
        return await self._price(inp)

    # ------------------------------------------------------------------ execute

    def check_claims(self, claims: m.HoldClaims) -> None:
        """Everything a verified token must say about this worker (decisions I10)."""
        if claims.offering != self.key:
            raise HoldRefused(f"hold token refused: it is for {claims.offering}, not {self.key}")
        if claims.seller_ns != self.namespace or claims.seller != self.party:
            raise HoldRefused(
                f"hold token refused: it is for the seller {claims.seller_ns!r}, "
                f"not {self.namespace!r}"
            )
        if claims.contract_hash != self.contract_hash:
            raise HoldRefused(
                f"hold token refused: it is for contract {claims.contract_hash}, but this "
                f"worker serves {self.contract_hash} for {self.key}"
            )
        now = self.clock()
        if now >= claims.exp:
            at = datetime.fromtimestamp(claims.exp, UTC).isoformat()
            raise HoldRefused(f"hold token refused: it expired at {at}; the hold refunds")
        if not is_job_id(claims.job_id) or claims.sub != claims.job_id:
            raise HoldRefused("hold token refused: it names no valid job")
        limit = self.bound.spec.max_duration.total_seconds()
        if not 0 < claims.max_duration_seconds <= limit:
            raise HoldRefused(
                f"hold token refused: max_duration {claims.max_duration_seconds}s is outside "
                f"this offering's 1..{int(limit)}s"
            )
        if not age.is_recipient(claims.buyer_key):
            raise HoldRefused("hold token refused: the buyer key is not an age recipient")

    def check_price(self, claims: m.HoldClaims, priced: KontraQuote) -> None:
        """The re-quote guard: a buyer cannot quote a small input and execute a large one."""
        if priced.status != "ok" or priced.money is None:
            raise _refuse(
                f"{self.key} does not accept this input now (its quote is {priced.status}); "
                "quote it again"
            )
        price = priced.money
        if price.currency != claims.currency:
            raise _refuse(
                f"{self.key} now prices in {price.currency}, but the hold is in "
                f"{claims.currency}; quote again and hire"
            )
        if price.amount_minor > claims.amount_minor:
            held = Money(claims.amount_minor, claims.currency).display()
            raise _refuse(
                f"this input prices above the held {held}; quote this input and hire again"
            )
        # The same guard for time: the job runs with the hold's max_duration, so an input
        # the seller needs longer for would be killed by that timeout and refunded with the
        # blame on the seller (§8.3). Refused now, it refunds with no blame. Like the price,
        # the re-quoted duration stays out of the (plaintext) message: it describes the input.
        needed = priced.max_duration_seconds
        if needed is not None and needed > claims.max_duration_seconds:
            raise _refuse(
                "this input needs longer than the held max_duration of "
                f"{claims.max_duration_seconds}s; quote this input and hire again"
            )

    def run_timeout(self, claims: m.HoldClaims, priced: KontraQuote) -> timedelta:
        """The job's execution timeout: ``max_duration``, cut so that the run ends by the
        gate's refund deadline -- the hold's ``deadline`` claim, or (a token without one)
        its expiry plus :attr:`grace`. The hold's ``max_duration`` counts from the hire, not
        from this start: a buyer that starts late gets what is left. A full
        ``max_duration`` from a late start would either deliver after the gate refunded the
        job (while the buyer's call still receives the result) or time out with its run
        pinned, counted against the seller. The deadline is the gate's own, not this
        handler's guess at the gate's grace: a gate with a shorter grace (``kontra.testing``
        uses one second) would otherwise see the run outlive its deadline.

        Refused when the re-quoted duration is longer than what is left before the expiry
        plus :attr:`grace` (or the deadline, if later): the seller would be blamed for a
        deadline it never had. The grace counts here even when the gate's is shorter: a job
        quoted for its whole max_duration (the usual case: the quote's duration is a cap)
        is always started a moment after the hire. Like the price, the duration stays out
        of the (plaintext) message: it describes the input."""
        now = self.clock()
        assumed = claims.exp + self.grace.total_seconds()
        end = claims.deadline if claims.deadline is not None else assumed
        left = end - now
        needed = priced.max_duration_seconds or claims.max_duration_seconds
        if needed > max(end, assumed) - now or left <= 0:
            at = datetime.fromtimestamp(claims.exp, UTC).isoformat()
            by = datetime.fromtimestamp(end, UTC).isoformat()
            raise _refuse(
                f"this hold expires at {at} and the job must end by {by}, sooner than this "
                "input needs; it was started too late: quote this input and hire again"
            )
        return timedelta(seconds=min(claims.max_duration_seconds, left))

    def check_input_size(self, claims: m.HoldClaims, raw: Any) -> None:
        """Refuse an ``execute`` input larger than the one the hold was quoted for, before
        a byte of it is fetched (decisions I16): what the handler fetches, decrypts and
        validates is then about what the seller already did within the gate's quote
        timeout, so the answer comes within one Nexus attempt. Without the bound, a buyer
        could quote a small input, execute a huge valid one, and have the slow answer
        recorded as an absent seller. Sizes are not secret (the platform sees every
        payload's size), so the message names the bound."""
        bound = claims.input_size
        if bound is None or not isinstance(raw, RawValue):
            return  # no bound in the token (an older gate), or a direct call: nothing to fetch
        try:
            size = input_size(raw.payload)
        except InvalidPayload:
            raise _refuse(
                f"the input of {self.key} is a malformed claim-check reference; the job was "
                "not started"
            ) from None
        if size > bound + INPUT_SLACK:
            raise _refuse(
                f"the input of {self.key} is larger than the input this hold was quoted for "
                f"({bound} bytes); quote this input and hire again"
            )

    async def _read_input(self, raw: Any, deadline: datetime | None = None) -> Any:
        """The ``execute`` input's JSON value, now that the hold is verified: a deferred
        claim-check is fetched only here. A value that is not a ``RawValue`` (a direct call)
        is already one.

        ``deadline``: the Nexus request's. A fetch still running :data:`_ANSWER_RESERVE`
        seconds before it (and never longer than :data:`_FETCH_BUDGET`) is abandoned and
        refused: answering late is recorded as a seller that never answered, and retrying
        would download the same blob again each time."""
        if not isinstance(raw, RawValue):
            return raw
        payload = raw.payload
        if self.open_deferred is not None:
            try:
                async with asyncio.timeout(_fetch_budget(deadline)):
                    payload = await self.open_deferred(payload)
            except TimeoutError:
                log.warning("%s execute: the input could not be fetched in time", self.key)
                raise _refuse(
                    f"the input of {self.key} could not be fetched within one Nexus attempt; "
                    "the job was not started (try the hire again later)"
                ) from None
            except InvalidPayload as exc:
                # Malformed, above the limit, altered, not encrypted to us: the same input
                # fails the same way on every retry, each one downloading it again. Refused
                # now; what exactly stays in the seller's log (it may quote a library).
                log.warning("%s execute: the input cannot be opened (%s)", self.key, exc)
                raise _refuse(
                    f"the input of {self.key} cannot be opened by its seller (a claim-check "
                    "that is malformed, too large or altered, or a payload not encrypted to "
                    "the seller); the job was not started"
                ) from None
            # Anything else (the bucket or the gate did not answer) goes to _guarded: a
            # generic, retryable error.
        try:
            [value] = self.payload_converter.from_payloads([payload], None)
        except Exception as exc:
            log.warning("%s execute: the input is not a JSON value (%s)", self.key, exc)
            raise _refuse(
                f"the input of {self.key} could not be read as JSON; the job was not started"
            ) from None
        return value

    async def execute(
        self, ctx: WorkflowRunOperationContext, raw: Any
    ) -> nexus.WorkflowHandle[Any]:
        try:
            claims = await self.verifier.verify(ctx.headers.get(HOLD_HEADER))
            self.check_claims(claims)
        except HoldRefused as exc:
            log.info("%s execute: %s", self.key, exc)
            raise _refuse(str(exc)) from None
        self.check_input_size(claims, raw)
        value = await self._read_input(raw, ctx.request_deadline)
        try:
            inp = self._parse(value)
        except ContractError as exc:
            self._log_invalid("execute", exc)
            raise _refuse(
                f"the input does not match the contract of {self.key} "
                f"({len(exc.errors)} problem(s)); the job was not started"
            ) from None
        priced = await self._price(inp)
        self.check_price(claims, priced)
        timeout = self.run_timeout(claims, priced)
        self.jobs.record(claims.job_id, claims.buyer_key, claims.contract_hash)
        try:
            # The bound workflow with the input, or (a bound handler) Kontra's turn wrapper
            # with the handler's message: the only step that depends on the entry's kind.
            return await ctx.start_workflow(
                self.bound.run_fn,
                self.bound.job_input(inp),
                id=job_workflow_id(claims.job_id),
                execution_timeout=timeout,
                # One start per job, closed or running: a call attached to a running
                # job-<id> (USE_EXISTING) is not the call that started it, so escrow would
                # never count what it returns (decisions I15); it is refused instead, like a
                # second start after the run closed. A retry of this very start (the same
                # Nexus request id) still gets its run: the server matches the request id
                # before it applies the policy.
                id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE,
                id_conflict_policy=WorkflowIDConflictPolicy.FAIL,
            )
        except WorkflowAlreadyStartedError:
            raise _refuse(
                f"job {claims.job_id} already ran (or is running); a hold starts at most one "
                "execution"
            ) from None


def service_handler(service: OfferingService) -> Any:
    """The Nexus service handler instance for ``service`` (name: the offering's
    :attr:`~kontra.contract.names.OfferingId.service_name`)."""

    class KontraOffering:
        @nexusrpc.handler.sync_operation
        async def quote(self, ctx: StartOperationContext, input: Any) -> KontraQuote:
            return await _guarded(service.key, "quote", service.quote(input))

        # RawValue: the payload as the codec left it, read only after the hold check.
        @nexus.workflow_run_operation
        async def execute(
            self, ctx: WorkflowRunOperationContext, input: RawValue
        ) -> nexus.WorkflowHandle[Any]:
            return await _guarded(service.key, "execute", service.execute(ctx, input))

    name = service.bound.offering_id.service_name
    return nexusrpc.handler.service_handler(name=name)(KontraOffering)()
