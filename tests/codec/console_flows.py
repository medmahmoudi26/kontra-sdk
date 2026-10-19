"""The workflow the graph page's seam ⑤ scan runs (``test_codec_console.py``): an importable
module, as the workflow sandbox requires. Every payload carries the text it is given."""

from __future__ import annotations

import asyncio
from datetime import timedelta

from kontra import ApplicationError, RetryPolicy, activity, workflow

#: How long the first activity holds still once it has heartbeated: the page reads the run
#: while the heartbeat is pending in DescribeWorkflowExecution.
HOLD_SECONDS = 1.5


@activity.defn
async def heartbeat_then_fail(text: str) -> str:
    """Heartbeats ``text``, then fails with it in the message on its first attempt and
    returns it, upper-cased, on its second."""
    activity.heartbeat(f"heartbeat {text}")
    await asyncio.sleep(HOLD_SECONDS)
    if activity.info().attempt == 1:
        raise ApplicationError(f"first attempt failed on {text}")
    return f"result {text}".upper()


@activity.defn
async def item(text: str) -> str:
    return f"item {text}"


@workflow.defn
class LeakFlow:
    @workflow.run
    async def run(self, text: str) -> str:
        first = await workflow.execute_activity(
            heartbeat_then_fail,
            text,
            start_to_close_timeout=timedelta(seconds=30),
            heartbeat_timeout=timedelta(seconds=20),
            retry_policy=RetryPolicy(initial_interval=timedelta(milliseconds=200)),
        )
        items = await workflow.execute_activities_in_parallel(
            item,
            items=[f"{text} {i}" for i in range(3)],
            start_to_close_timeout=timedelta(seconds=30),
        )
        return f"{first} and {len(items)} items"


@workflow.defn
class RefuseFlow:
    @workflow.run
    async def run(self, text: str) -> str:
        raise ApplicationError(f"refused {text}", non_retryable=True)


@workflow.defn
class IdleFlow:
    """Never runs: started on a task queue no worker polls, for its input alone."""

    @workflow.run
    async def run(self, text: str) -> str:
        return text
