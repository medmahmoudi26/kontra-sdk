"""The Nexus service Kontra generates for each bound offering (PRD §6.5 items 1-2, D2).

Service ``acme.enrich`` (:attr:`OfferingId.service_name`), two operations:

* ``quote`` (synchronous): validate the input against the contract, run the seller's
  ``quote``, answer a :class:`~kontra.contract.KontraQuote`. The answer is plaintext by
  design: it is a price the gate must read to lock it (decisions I4). An input the contract
  refuses gets a *rejected* quote rather than a handler error, so the buyer sees why and the
  gate does not count it as the seller failing.
* ``execute`` (workflow-run): verify the hold token offline, check every claim against what
  this worker serves, validate the input, re-run the seller's ``quote`` and refuse if the
  price went up or the currency changed (decisions I10) -- or if this input needs longer
  than the hold's max_duration --, record the buyer's key, then start the bound workflow as
  ``job-<id>``. Every refusal happens before any workflow exists, as a non-retryable
  ``BAD_REQUEST`` whose message names what failed -- never an input value or the token
  (refusals travel in plaintext; the codec deliberately leaves this operation's replies
  unencrypted so a buyer can read them).

The input arrives as raw JSON (the operations are typed ``Any``): validating the raw value
is what refuses an extra field that a lenient pydantic model would silently drop (§11.3).
"""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, TypeVar

import nexusrpc
import nexusrpc.handler
from nexusrpc.handler import StartOperationContext
from pydantic import BaseModel, ValidationError
from temporalio import nexus
from temporalio.common import WorkflowIDConflictPolicy, WorkflowIDReusePolicy
from temporalio.exceptions import WorkflowAlreadyStartedError
from temporalio.nexus import WorkflowRunOperationContext

from kontra.codec import age
from kontra.contract import ContractError, InputValidator, KontraQuote, Quote, QuoteError
from kontra.contract.names import is_job_id, job_workflow_id
from kontra.gate_client import models as m
from kontra.ledger import Money
from kontra.serve.hold import HOLD_HEADER, HoldRefused, HoldVerifier
from kontra.serve.jobs import JobDirectory
from kontra.serve.offering import BoundOffering

__all__ = ["OfferingService", "service_handler"]

log = logging.getLogger("kontra.serve.handler")
_T = TypeVar("_T")
_LOGGED_PATH = 200


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

    async def execute(
        self, ctx: WorkflowRunOperationContext, raw: Any
    ) -> nexus.WorkflowHandle[Any]:
        try:
            claims = await self.verifier.verify(ctx.headers.get(HOLD_HEADER))
            self.check_claims(claims)
        except HoldRefused as exc:
            log.info("%s execute: %s", self.key, exc)
            raise _refuse(str(exc)) from None
        try:
            inp = self._parse(raw)
        except ContractError as exc:
            self._log_invalid("execute", exc)
            raise _refuse(
                f"the input does not match the contract of {self.key} "
                f"({len(exc.errors)} problem(s)); the job was not started"
            ) from None
        self.check_price(claims, await self._price(inp))
        self.jobs.record(claims.job_id, claims.buyer_key, claims.contract_hash)
        try:
            return await ctx.start_workflow(
                self.bound.run_fn,
                inp,
                id=job_workflow_id(claims.job_id),
                execution_timeout=timedelta(seconds=claims.max_duration_seconds),
                id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE,
                id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
            )
        except WorkflowAlreadyStartedError:
            raise _refuse(
                f"job {claims.job_id} already ran; a hold starts at most one execution"
            ) from None


def service_handler(service: OfferingService) -> Any:
    """The Nexus service handler instance for ``service`` (name: the offering's
    :attr:`~kontra.contract.names.OfferingId.service_name`)."""

    class KontraOffering:
        @nexusrpc.handler.sync_operation
        async def quote(self, ctx: StartOperationContext, input: Any) -> KontraQuote:
            return await _guarded(service.key, "quote", service.quote(input))

        @nexus.workflow_run_operation
        async def execute(
            self, ctx: WorkflowRunOperationContext, input: Any
        ) -> nexus.WorkflowHandle[Any]:
            return await _guarded(service.key, "execute", service.execute(ctx, input))

    name = service.bound.offering_id.service_name
    return nexusrpc.handler.service_handler(name=name)(KontraOffering)()
