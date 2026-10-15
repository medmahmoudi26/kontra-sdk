"""The batch tests' parent workflow, through the façade: one case per call."""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any

from kontra import RetryPolicy, workflow

from . import acts
from .models import Tally

_T = timedelta(seconds=30)


@workflow.defn
class Parent:
    @workflow.run
    async def run(self, case: str, n: int) -> Any:
        par = workflow.execute_activities_in_parallel
        if case == "items":
            return await par(acts.double, items=list(range(n)), start_to_close_timeout=_T)
        if case == "offsets":
            return await par(
                acts.double, pages=acts.offsets, reduce=acts.add, init=0, start_to_close_timeout=_T
            )
        if case == "tokens":
            return await par(acts.shout, pages=acts.tokens, start_to_close_timeout=_T)
        if case == "stuck":
            return await par(acts.shout, pages=acts.stuck, start_to_close_timeout=_T)
        if case == "bounded":
            return await par(
                acts.slow, items=list(range(n)), max_concurrent=3, start_to_close_timeout=_T
            )
        if case == "fragile":
            return await par(
                acts.fragile,
                items=list(range(n)),
                start_to_close_timeout=_T,
                retry_policy=RetryPolicy(maximum_attempts=1),
            )
        if case == "twice":
            a, b = await asyncio.gather(
                par(acts.double, items=[1, 2], start_to_close_timeout=_T),
                par(acts.double, items=[10], start_to_close_timeout=_T),
            )
            return [sorted(a), b]
        if case == "tally":
            out = await par(
                acts.probe,
                items=[f"host-{i}" for i in range(n)],
                max_concurrent=4,
                reduce=acts.tally,
                init=Tally(),
                start_to_close_timeout=_T,
                retry_policy=RetryPolicy(
                    initial_interval=timedelta(milliseconds=200), maximum_attempts=3
                ),
            )
            assert isinstance(out, Tally)  # typed in the parent too
            return out
        raise ValueError(case)
