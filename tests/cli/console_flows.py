"""Workflows the graph page's routes are tested on (``test_console.py``): an importable
module, as the workflow sandbox requires."""

from __future__ import annotations

import asyncio
from datetime import timedelta

from temporalio import activity, workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError

#: How long each attempt of :func:`flaky` runs: long enough for a stream polling every
#: 0.2 s to see it running.
ATTEMPT_SECONDS = 1.0


@activity.defn
async def flaky(text: str) -> str:
    """Fails its first two attempts, after a second each; the third returns ``text``."""
    await asyncio.sleep(ATTEMPT_SECONDS)
    attempt = activity.info().attempt
    if attempt < 3:
        raise ApplicationError(f"attempt {attempt} failed")
    return text


@activity.defn
async def quick(text: str) -> str:
    return text


@workflow.defn
class RetryFlow:
    @workflow.run
    async def run(self, text: str) -> str:
        return await workflow.execute_activity(
            flaky,
            text,
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=RetryPolicy(
                initial_interval=timedelta(seconds=1), backoff_coefficient=1.0, maximum_attempts=5
            ),
        )


@workflow.defn
class QuickFlow:
    @workflow.run
    async def run(self, text: str) -> str:
        return await workflow.execute_activity(
            quick, text, start_to_close_timeout=timedelta(seconds=30)
        )


@workflow.defn
class WaitFlow:
    """Open until it is terminated (or a minute passes)."""

    @workflow.run
    async def run(self, text: str) -> str:
        await workflow.sleep(timedelta(minutes=1))
        return text


#: How long :func:`fails_once` backs off after its first attempt: a test reads the step
#: while it waits, then terminates the run.
BACKOFF = timedelta(minutes=1)


@activity.defn
async def fails_once(text: str) -> str:
    """Fails its first attempt with ``text`` in the message; the second returns it."""
    if activity.info().attempt == 1:
        raise ApplicationError(f"attempt 1 failed on {text}")
    return text


@workflow.defn
class BackoffFlow:
    """:func:`fails_once`, retried after :data:`BACKOFF`."""

    @workflow.run
    async def run(self, text: str) -> str:
        return await workflow.execute_activity(
            fails_once,
            text,
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=RetryPolicy(initial_interval=BACKOFF),
        )
