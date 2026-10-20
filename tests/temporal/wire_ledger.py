"""The wire suite's assertions and the ledger of every cell's outcome.

Every cell a wire test checks is recorded here, and ``conftest.pytest_terminal_summary``
prints the ledger at the end of the session: the table the deny matrix is read from over
the wire. Shared by every test file in ``tests/temporal``.
"""

from __future__ import annotations

from typing import Any

import pytest
from temporalio.service import RPCError, RPCStatusCode

#: (cell, expectation, outcome) of every cell checked this session.
OUTCOMES: list[tuple[str, str, str]] = []


def record(cell: str, expectation: str, outcome: str) -> None:
    OUTCOMES.append((cell, expectation, outcome))


async def assert_denied(cell: str, coro: Any) -> None:
    """A call that must be refused with PERMISSION_DENIED."""
    try:
        await coro
    except RPCError as e:
        if e.status == RPCStatusCode.PERMISSION_DENIED:
            record(cell, "deny", "PERMISSION_DENIED")
            return
        record(cell, "deny", f"other: {e.status.name}")
        pytest.fail(f"{cell}: expected PERMISSION_DENIED, got {e.status.name}: {e.message}")
    record(cell, "deny", "ALLOWED")
    pytest.fail(f"{cell}: the call was allowed")


async def assert_allowed(cell: str, coro: Any) -> Any:
    try:
        result = await coro
    except RPCError as e:
        record(cell, "allow", f"DENIED: {e.status.name}")
        pytest.fail(f"{cell}: expected success, got {e.status.name}: {e.message}")
    record(cell, "allow", "OK")
    return result


async def assert_authorized(cell: str, coro: Any) -> Any:
    """A call the authorizer must let through. Temporal may still refuse it for the
    state it is in (a version that is current cannot be deleted, a workflow that does not
    exist is not found); anything but PERMISSION_DENIED proves the policy allowed it."""
    try:
        result = await coro
    except RPCError as e:
        if e.status == RPCStatusCode.PERMISSION_DENIED:
            record(cell, "auth", "PERMISSION_DENIED")
            pytest.fail(f"{cell}: the authorizer refused it: {e.message}")
        record(cell, "auth", f"authorized, then {e.status.name}")
        return None
    record(cell, "auth", "OK")
    return result


async def assert_refused(cell: str, status: RPCStatusCode, coro: Any) -> None:
    """A call that must fail with exactly ``status`` (a refusal that is not the
    authorizer's: Temporal's own validation after the authorizer allowed the namespace)."""
    try:
        await coro
    except RPCError as e:
        record(cell, "refuse", e.status.name)
        if e.status != status:
            pytest.fail(f"{cell}: expected {status.name}, got {e.status.name}: {e.message}")
        return
    record(cell, "refuse", "ALLOWED")
    pytest.fail(f"{cell}: the call was allowed")
