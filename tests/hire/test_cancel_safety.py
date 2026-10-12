"""Cancel safety of ``Job.result(timeout)``: no Temporal server, no gate.

On Python 3.11 ``asyncio.wait_for`` returns the awaited result when its caller is cancelled
in the loop iteration where that result arrives: the cancellation is lost, and a cancelled
``result()`` (a task group's sibling failed, an agent aborted the tool call) would go on
to decrypt, validate and return the delivery. These tests make exactly that interleaving
happen, so they are red on 3.11 with ``wait_for``; on 3.12 ``wait_for`` itself is built on
``asyncio.timeout``. CI runs them on both; locally, without touching ``.venv``:

    uv run --isolated --python 3.11 pytest tests/hire/test_cancel_safety.py
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from kontra.hire import Job


class _Kontra:
    """The one call ``Job.result`` makes on its client once it has the outcome."""

    def __init__(self) -> None:
        self.delivered: list[Any] = []

    async def _deliver(self, job: Job, value: Any, output_type: Any) -> Any:
        self.delivered.append(value)
        return value


class _Outcome:
    """Stands in for ``Job._outcome`` (Temporal, then the gate): blocks on ``arrival``."""

    def __init__(self) -> None:
        self.arrival: asyncio.Future[Any] = asyncio.get_running_loop().create_future()
        self.waiting = asyncio.Event()
        self.cancelled = False

    async def __call__(self) -> Any:
        self.waiting.set()
        try:
            return await self.arrival
        except asyncio.CancelledError:
            self.cancelled = True
            raise


def _job(outcome: _Outcome) -> tuple[Job, _Kontra]:
    kontra = _Kontra()
    job = Job(
        kontra,  # type: ignore[arg-type]
        job_id="0" * 32,
        offering="acme/words@1.0.0",
        amount_minor=100,
        currency="USD",
        operation_id="op",
    )
    job._outcome = outcome  # type: ignore[method-assign]
    return job, kontra


@pytest.mark.parametrize("deadline", [30.0, None])
async def test_a_cancel_that_lands_as_the_delivery_arrives_is_kept(deadline: float | None) -> None:
    outcome = _Outcome()
    job, kontra = _job(outcome)
    task = asyncio.create_task(job.result(timeout=deadline))
    await outcome.waiting.wait()  # the task now waits for the outcome
    # Both in one loop iteration: the task's next step sees the result and the cancel.
    outcome.arrival.set_result({"upper": "X"})
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert kontra.delivered == []  # it did not go on to open the delivery
    assert not job._delivered


async def test_a_cancel_while_waiting_is_a_cancel_not_a_timeout() -> None:
    outcome = _Outcome()
    job, _ = _job(outcome)
    task = asyncio.create_task(job.result(timeout=30))
    await outcome.waiting.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert outcome.cancelled


async def test_the_timeout_still_ends_the_wait_with_timeout_error() -> None:
    outcome = _Outcome()
    job, kontra = _job(outcome)
    with pytest.raises(TimeoutError):
        await job.result(timeout=0.05)
    assert outcome.cancelled  # the wait for the outcome was stopped, not left running
    assert kontra.delivered == []
    assert asyncio.current_task().cancelling() == 0  # type: ignore[union-attr]


async def test_without_a_timeout_the_delivery_is_returned() -> None:
    outcome = _Outcome()
    job, kontra = _job(outcome)
    task = asyncio.create_task(job.result())
    await outcome.waiting.wait()
    outcome.arrival.set_result({"upper": "X"})
    assert await task == {"upper": "X"}
    assert kontra.delivered == [{"upper": "X"}]
    assert job._delivered
