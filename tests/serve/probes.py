"""A test-only activity that reports the Kontra job context it runs in. (Test code may
import kontra; the D1 rule is about sellers' workflows.py and activities.py.)"""

from __future__ import annotations

from temporalio import activity

from kontra import _runtime


@activity.defn(name="where")
async def where() -> str:
    ctx = _runtime.current_job()
    return ctx.job_id if ctx is not None else ""
