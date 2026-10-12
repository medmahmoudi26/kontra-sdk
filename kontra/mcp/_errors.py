"""Failures as structured MCP tool errors.

An agent decides what to do next from a failure: quote again, raise ``max_price``, wait for
``eta_seconds``, or give up. So every failure the buyer can act on becomes a tool result with
``isError: true`` whose structured content is ``{"error": {code, message, retryable,
detail}}``, ``code`` being the gate's own (``quote_expired``, ``job_not_found``...) or the
SDK's name for a refusal it makes before any gate call (``price_above_max``,
``invalid_input``...). Anything else is a bug: it is left to the MCP server, which logs it
and shows the agent only that the tool failed.

Messages and string details are clipped to :data:`TEXT_LIMIT` characters: some carry the
seller's text (a failed job's reason, the value an invalid delivery got wrong), which must
not flood the agent's context or smuggle a long prompt into it.
"""

import json
from typing import Any

import httpx
from mcp.types import CallToolResult, TextContent
from temporalio.service import RPCError, RPCStatusCode

from kontra.artifacts import ArtifactError
from kontra.gate_client import GateError
from kontra.hire import (
    HireRefused,
    InsufficientFunds,
    InvalidDelivery,
    InvalidInput,
    JobFailed,
    KontraError,
    PriceAboveMax,
    QuoteRejected,
    SellerBusy,
    TemporalUnavailable,
)
from kontra.profile import NotLoggedIn, ProfileError

__all__ = ["TEXT_LIMIT", "ToolRefusal", "WithDetail", "clip", "error_result", "tool_error"]

#: The most characters of one message or detail string an error shows.
TEXT_LIMIT = 1000

#: Codes an agent may simply retry later (after ``eta_seconds`` when given).
_RETRYABLE = frozenset(
    {"busy", "gate_unreachable", "temporal_unavailable", "timeout", "seller_error"}
)
_RETRYABLE_STATUS = frozenset({408, 429, 500, 502, 503, 504})


class ToolRefusal(Exception):
    """A refusal this server makes itself (bad argument combination, unknown quote id)."""

    def __init__(self, code: str, message: str, **detail: Any) -> None:
        super().__init__(message)
        self.code, self.message, self.detail = code, message, detail


class WithDetail(Exception):
    """A handled failure plus what this server knows about it: the ``quote_id`` a failed
    hire can be retried with (the gate never hires one quote twice)."""

    def __init__(self, err: BaseException, **detail: Any) -> None:
        super().__init__(str(err))
        self.err, self.detail = err, detail


def clip(text: str, limit: int = TEXT_LIMIT) -> str:
    """``text``, cut to ``limit`` characters with a marker saying how much was left out."""
    if len(text) <= limit:
        return text
    return f"{text[:limit]}... [{len(text) - limit} more characters not shown]"


#: Exceptions :func:`tool_error` turns into a result; anything else propagates.
HANDLED: tuple[type[BaseException], ...] = (
    ToolRefusal,
    WithDetail,
    KontraError,
    GateError,
    httpx.TransportError,
    TemporalUnavailable,
    RPCError,
    ArtifactError,
    ProfileError,
    OSError,  # TimeoutError included; a download directory that cannot be written
)


def _kontra_code(err: KontraError) -> str:
    match err:
        case PriceAboveMax():
            return "price_above_max"
        case QuoteRejected():
            return "quote_rejected"
        case SellerBusy():
            return "busy"
        case InsufficientFunds():
            return "insufficient_funds"
        case HireRefused():
            return err.code or "hire_refused"
        case InvalidInput():
            return "invalid_input"
        case JobFailed():
            return "job_failed"
        case InvalidDelivery():
            return "invalid_delivery"
        case _:
            return "kontra_error"


def tool_error(err: BaseException) -> tuple[str, str, dict[str, Any]]:
    """``(code, message, detail)`` for a failure in :data:`HANDLED`."""
    match err:
        case ToolRefusal():
            return err.code, err.message, err.detail
        case WithDetail():
            code, message, detail = tool_error(err.err)
            return code, message, {**detail, **err.detail}
        case InvalidDelivery():
            # The raw value stays out: it is the seller's, and it may be large.
            return "invalid_delivery", err.message, {"job_id": err.job_id}
        case JobFailed():
            detail = {"job_id": err.job_id, "state": err.state, "reason": err.reason}
            message = (
                f"the job ended without a delivery ({err.state}): {err.reason}; "
                "its escrow is refunded"
            )
            return "job_failed", message, detail
        case KontraError():
            fields = {k: v for k, v in err.fields.items() if k not in ("code", "detail")}
            # The gate's own detail. A HireRefused carries it (``quote_used`` names the job
            # its quote already made: a hire retried after an ambiguous failure finds it);
            # the money refusals (amounts in minor units) keep it on the GateError they were
            # raised from.
            gate = err.detail if isinstance(err, HireRefused) else None
            if not gate and isinstance(err.__cause__, GateError):
                gate = err.__cause__.detail
            return _kontra_code(err), err.message, {**(gate or {}), **fields}
        case GateError():
            message = err.message
            if err.status == 401:
                message += " (log in again: kontra login)"
            return err.code, message, {"status": err.status, **(err.detail or {})}
        case httpx.TransportError():
            return "gate_unreachable", f"the Kontra gate did not answer: {err}", {}
        case TemporalUnavailable():
            return "temporal_unavailable", f"Temporal could not be reached: {err}", {}
        case RPCError():
            code = (
                "temporal_unavailable"
                if err.status == RPCStatusCode.UNAVAILABLE
                else "temporal_error"
            )
            return code, f"Temporal refused the call: {err.message}", {}
        case ArtifactError():
            return "artifact_error", str(err), {}
        case NotLoggedIn():
            return "not_logged_in", str(err), {}
        case ProfileError():
            return "profile_error", str(err), {}
        case TimeoutError():
            return "timeout", "the operation timed out; try again", {}
        case OSError():
            return "local_io_error", str(err), {}
        case _:
            raise TypeError(f"not a handled failure: {type(err).__name__}") from err


def error_result(err: BaseException) -> CallToolResult:
    """The tool result for a handled failure: ``isError`` with the error as structured
    content, and the same JSON as text for hosts that only show text."""
    code, message, detail = tool_error(err)
    detail = {k: clip(v) if isinstance(v, str) else v for k, v in detail.items()}
    body: dict[str, Any] = {
        "error": {
            "code": code,
            "message": clip(message),
            "retryable": code in _RETRYABLE or detail.get("status") in _RETRYABLE_STATUS,
            "detail": detail,
        }
    }
    text = json.dumps(body, ensure_ascii=False, default=str)
    return CallToolResult(
        content=[TextContent(type="text", text=text)],
        structured_content=json.loads(text),
        is_error=True,
    )
