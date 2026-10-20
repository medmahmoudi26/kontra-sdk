"""How far a buyer's call got, read without a server (decisions I40): what
:func:`~kontra._control.call_record` takes from the shapes server 1.32 records (measured),
and how :meth:`~kontra._control.Control.describe_call` maps the frontend's refusals.

Ported from kontra-cloud's ``tests/jobs/test_temporal_seam.py`` with ``describe_operation``
and ``buyer_operation``, which the gate drops. A dev server produces only two of the
refusals (``tests/control/test_provenance.py``), and a refusal read as "no call" lets the
seller escape blame for good -- no call is ``NEVER_DISPATCHED``, which no one is blamed for,
where "cannot say" is asked again -- so each is pinned here."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from temporalio.api.enums.v1 import (
    NexusHandlerErrorRetryBehavior,
    NexusOperationExecutionStatus,
    PendingNexusOperationState,
)
from temporalio.api.failure.v1 import Failure, NexusHandlerFailureInfo
from temporalio.api.nexus.v1 import NexusOperationExecutionInfo
from temporalio.api.workflowservice.v1 import DescribeNexusOperationExecutionResponse
from temporalio.service import RPCError, RPCStatusCode

from kontra._control import (
    CALL_READ_TIMEOUT,
    HANDLER_BAD_REQUEST,
    HANDLER_INTERNAL,
    HANDLER_UPSTREAM_TIMEOUT,
    BuyerLookupError,
    CallFailure,
    CallRecord,
    CallStatus,
    Control,
    TemporalError,
    call_record,
)

S = NexusOperationExecutionStatus
P = PendingNexusOperationState
B = NexusHandlerErrorRetryBehavior
ENDPOINT = "k" + "1" * 32


def _info(
    status: int, state: int = 0, *, failure: Failure | None = None, token: str = ""
) -> NexusOperationExecutionInfo:
    info = NexusOperationExecutionInfo(
        operation_id="job-1",
        endpoint=ENDPOINT,
        service="acme.echo",
        operation="execute",
        status=status,  # pyright: ignore[reportArgumentType]
        state=state,  # pyright: ignore[reportArgumentType]
        operation_token=token,
    )
    info.nexus_header["kontra-hold"] = "tok"
    if failure is not None:
        info.last_attempt_failure.CopyFrom(failure)
        info.last_attempt_complete_time.GetCurrentTime()
    return info


def _handler(
    kind: str, behavior: int = B.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_UNSPECIFIED, message: str = ""
) -> Failure:
    return Failure(
        message=message,
        nexus_handler_failure_info=NexusHandlerFailureInfo(
            type=kind,
            retry_behavior=behavior,  # pyright: ignore[reportArgumentType]
        ),
    )


FINAL = B.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_NON_RETRYABLE
RETRYABLE = B.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_RETRYABLE


@pytest.mark.parametrize(
    ("info", "status", "started", "failure"),
    [
        # No worker polls the endpoint's task queue (or the handler is too slow): measured.
        (
            _info(
                S.NEXUS_OPERATION_EXECUTION_STATUS_RUNNING,
                P.PENDING_NEXUS_OPERATION_STATE_BACKING_OFF,
                failure=_handler(HANDLER_UPSTREAM_TIMEOUT, message="upstream timeout"),
            ),
            CallStatus.RUNNING,
            False,
            CallFailure(HANDLER_UPSTREAM_TIMEOUT),
        ),
        (
            _info(
                S.NEXUS_OPERATION_EXECUTION_STATUS_TIMED_OUT,
                failure=_handler(HANDLER_UPSTREAM_TIMEOUT, message="upstream timeout"),
            ),
            CallStatus.TIMED_OUT,
            False,
            CallFailure(HANDLER_UPSTREAM_TIMEOUT),
        ),
        # kontra.serve's refusal: BAD_REQUEST, non-retryable.
        (
            _info(
                S.NEXUS_OPERATION_EXECUTION_STATUS_FAILED,
                failure=_handler(HANDLER_BAD_REQUEST, FINAL, "hold token refused"),
            ),
            CallStatus.FAILED,
            False,
            CallFailure(HANDLER_BAD_REQUEST, final=True),
        ),
        # A handler that sets no retry behaviour: unspecified, which is not final (the server
        # still fails BAD_REQUEST at once, by its type's default).
        (
            _info(S.NEXUS_OPERATION_EXECUTION_STATUS_FAILED, failure=_handler(HANDLER_BAD_REQUEST)),
            CallStatus.FAILED,
            False,
            CallFailure(HANDLER_BAD_REQUEST),
        ),
        # The seller's own code failed, declared final; and one left retryable, explicitly.
        (
            _info(
                S.NEXUS_OPERATION_EXECUTION_STATUS_FAILED,
                failure=_handler(HANDLER_INTERNAL, FINAL, "quote exploded"),
            ),
            CallStatus.FAILED,
            False,
            CallFailure(HANDLER_INTERNAL, final=True),
        ),
        (
            _info(
                S.NEXUS_OPERATION_EXECUTION_STATUS_RUNNING,
                P.PENDING_NEXUS_OPERATION_STATE_BACKING_OFF,
                failure=_handler(HANDLER_INTERNAL, RETRYABLE, "bucket down"),
            ),
            CallStatus.RUNNING,
            False,
            CallFailure(HANDLER_INTERNAL),
        ),
        # The handler took the call: an operation token, state STARTED.
        (
            _info(
                S.NEXUS_OPERATION_EXECUTION_STATUS_RUNNING,
                P.PENDING_NEXUS_OPERATION_STATE_STARTED,
                token="op-token",
            ),
            CallStatus.RUNNING,
            True,
            None,
        ),
        # ... either is enough ...
        (
            _info(
                S.NEXUS_OPERATION_EXECUTION_STATUS_RUNNING, P.PENDING_NEXUS_OPERATION_STATE_STARTED
            ),
            CallStatus.RUNNING,
            True,
            None,
        ),
        # ... and is still known to have once the operation closed.
        (
            _info(S.NEXUS_OPERATION_EXECUTION_STATUS_TERMINATED, token="op-token"),
            CallStatus.TERMINATED,
            True,
            None,
        ),
        (
            _info(S.NEXUS_OPERATION_EXECUTION_STATUS_COMPLETED, token="op-token"),
            CallStatus.COMPLETED,
            True,
            None,
        ),
        # Terminated by the buyer before any attempt finished.
        (
            _info(S.NEXUS_OPERATION_EXECUTION_STATUS_TERMINATED),
            CallStatus.TERMINATED,
            False,
            None,
        ),
        # A failure that is not a handler error.
        (
            _info(S.NEXUS_OPERATION_EXECUTION_STATUS_RUNNING, failure=Failure(message="x")),
            CallStatus.RUNNING,
            False,
            CallFailure(""),
        ),
        (
            _info(S.NEXUS_OPERATION_EXECUTION_STATUS_CANCELED, failure=Failure(message="x")),
            CallStatus.CANCELED,
            False,
            CallFailure(""),
        ),
        # A status this SDK does not know reads as still running: nothing is concluded.
        (
            _info(S.NEXUS_OPERATION_EXECUTION_STATUS_UNSPECIFIED),
            CallStatus.RUNNING,
            False,
            None,
        ),
    ],
)
def test_call_record_reads_what_the_server_recorded(
    info: NexusOperationExecutionInfo,
    status: CallStatus,
    started: bool,
    failure: CallFailure | None,
) -> None:
    assert call_record(info) == CallRecord(
        endpoint=ENDPOINT,
        service="acme.echo",
        operation="execute",
        headers={"kontra-hold": "tok"},
        status=status,
        started=started,
        failure=failure,
    )


def test_call_record_reads_when_the_call_was_made_and_when_it_ends() -> None:
    """Who is blamed for an unanswered call depends on how long it waited (the gate's
    window): the server's ``schedule_time`` and ``expiration_time``, read as they are."""
    info = _info(S.NEXUS_OPERATION_EXECUTION_STATUS_TIMED_OUT)
    record = call_record(info)
    assert (record.scheduled_at, record.expires_at) == (None, None)
    made = datetime(2026, 10, 1, 12, 0, 1, tzinfo=UTC)
    info.schedule_time.FromDatetime(made)
    info.expiration_time.FromDatetime(made + timedelta(seconds=15))
    record = call_record(info)
    assert (record.scheduled_at, record.expires_at) == (made, made + timedelta(seconds=15))


def test_call_record_reads_whether_the_buyer_asked_to_cancel() -> None:
    """``cancel_requested`` is the server's ``cancellation_info`` (only a cancel request in
    the buyer's namespace writes it), never the status: the seller's handler ends a call
    ``CANCELED`` with none, and a call the buyer cancels before any worker took it stays
    ``RUNNING`` with it (server 1.32, measured)."""
    by_seller = _info(S.NEXUS_OPERATION_EXECUTION_STATUS_CANCELED, failure=Failure(message="x"))
    record = call_record(by_seller)
    assert (record.status, record.started, record.cancel_requested) == (
        CallStatus.CANCELED,
        False,
        False,
    )
    by_buyer = _info(
        S.NEXUS_OPERATION_EXECUTION_STATUS_RUNNING,
        P.PENDING_NEXUS_OPERATION_STATE_BACKING_OFF,
        failure=_handler(HANDLER_UPSTREAM_TIMEOUT, message="upstream timeout"),
    )
    by_buyer.cancellation_info.requested_time.GetCurrentTime()
    record = call_record(by_buyer)
    assert (record.status, record.started, record.cancel_requested) == (
        CallStatus.RUNNING,
        False,
        True,
    )


class _Service:
    def __init__(self, answer: RPCError | DescribeNexusOperationExecutionResponse) -> None:
        self.answer = answer
        self.requests: list[tuple[Any, dict[str, Any]]] = []

    async def describe_nexus_operation_execution(self, req: Any, **kw: Any) -> Any:
        self.requests.append((req, kw))
        if isinstance(self.answer, RPCError):
            raise self.answer
        return self.answer


class _Client:
    def __init__(self, service: _Service) -> None:
        self.workflow_service = service


def _control(
    answer: RPCError | DescribeNexusOperationExecutionResponse,
) -> tuple[Control, _Service]:
    service = _Service(answer)
    return Control(_Client(service), "http://unused"), service  # type: ignore[arg-type]  # pyright: ignore[reportArgumentType]


async def test_describe_call_asks_the_buyers_namespace_once_by_operation_id() -> None:
    """One RPC in the buyer's namespace, by the operation's id (its latest run), not retried
    and cut off at ``CALL_READ_TIMEOUT`` (2 s), not the other calls' 15 s: it is made on
    every observation of a job with no verified run, in a namespace whose load its buyer
    controls."""
    info = _info(S.NEXUS_OPERATION_EXECUTION_STATUS_RUNNING, token="t")
    control, service = _control(DescribeNexusOperationExecutionResponse(info=info))
    record = await control.describe_call("bob", "job-1")
    assert record == call_record(info)
    assert record is not None
    assert record.started
    [(req, kw)] = service.requests
    assert (req.namespace, req.operation_id, req.run_id) == ("bob", "job-1", "")
    assert kw == {"retry": False, "timeout": CALL_READ_TIMEOUT}
    assert timedelta(seconds=2) == CALL_READ_TIMEOUT


@pytest.mark.parametrize(
    ("error", "outcome"),
    [
        # No such operation: the buyer never called (as far as anyone can see).
        (RPCError("operation not found", RPCStatusCode.NOT_FOUND, b""), None),
        # An id the frontend refuses as malformed names nothing.
        (RPCError("invalid operation id", RPCStatusCode.INVALID_ARGUMENT, b""), None),
        # A missing namespace is "cannot say", never "the buyer never called"; like any failed
        # read of the buyer's namespace it is BuyerLookupError (a TemporalError).
        (RPCError("namespace bob not found", RPCStatusCode.NOT_FOUND, b""), BuyerLookupError),
        (RPCError("frontend down", RPCStatusCode.UNAVAILABLE, b""), BuyerLookupError),
        (RPCError("limit", RPCStatusCode.RESOURCE_EXHAUSTED, b""), BuyerLookupError),
        (RPCError("too slow", RPCStatusCode.DEADLINE_EXCEEDED, b""), BuyerLookupError),
        (RPCError("no", RPCStatusCode.PERMISSION_DENIED, b""), BuyerLookupError),
        (RPCError("boom", RPCStatusCode.INTERNAL, b""), BuyerLookupError),
    ],
)
async def test_describe_call_maps_refusals(error: RPCError, outcome: type | None) -> None:
    control, service = _control(error)
    if outcome is None:
        assert await control.describe_call("bob", "job-1") is None
    else:
        with pytest.raises(outcome):
            await control.describe_call("bob", "job-1")
        assert issubclass(outcome, TemporalError)
    assert len(service.requests) == 1  # never retried here
