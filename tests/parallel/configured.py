"""An activity module that reads its settings as it loads, as a seller's often does
(``examples/sweep`` reads ``os.environ`` at module level). Workflow code imports it passed
through the sandbox, and the batch child must find it the same way: imported afresh inside
the sandbox, where ``os.environ`` is off limits, it fails the child's every workflow task."""

from __future__ import annotations

import os

from kontra import activity

FACTOR = int(os.environ.get("BATCH_TEST_FACTOR", "3"))


@activity.defn
async def scale(x: int) -> int:
    return FACTOR * x
