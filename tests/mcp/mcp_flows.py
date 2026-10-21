"""The test seller's workflow and activities: plain Temporal, in an importable module as
the workflow sandbox requires. Only ``Artifact`` (from ``mcp_models``) touches Kontra (D1)."""

from __future__ import annotations

import tempfile
from collections import Counter
from datetime import timedelta
from pathlib import Path

from temporalio import activity, workflow
from temporalio.exceptions import ApplicationError

with workflow.unsafe.imports_passed_through():
    from mcp_models import Artifact, Count, DocJob, DocOut, WordsJob, WordsOut

_T = timedelta(seconds=30)
FAIL_MESSAGE = "the seller's job failed on purpose"


@activity.defn
async def upper(text: str) -> str:
    return text.upper()


@activity.defn
async def beat(count: int) -> int:
    for i in range(1, count + 1):
        activity.heartbeat({"percent": i * 100 // count, "message": f"beat {i}"})
    return count


@activity.defn
async def halfway() -> None:
    activity.heartbeat({"percent": 50, "message": "halfway there"})


@activity.defn
async def make_file(name: str, lines: int) -> Artifact:
    with tempfile.TemporaryDirectory() as d:
        path = Path(d) / f"{name}.txt"
        path.write_text("".join(f"{name} line {i}\n" for i in range(lines)))
        return await Artifact.from_path(path, content_type="text/plain")


@workflow.defn
class WordsFlow:
    @workflow.run
    async def run(self, job: WordsJob) -> WordsOut:
        if job.mode == "fail":
            raise ApplicationError(FAIL_MESSAGE, non_retryable=True)
        text = await workflow.execute_activity(upper, job.text, start_to_close_timeout=_T)
        if job.mode == "beats":
            # Progress is live (decisions I31): a moment for the buyer to start listening.
            await workflow.sleep(timedelta(seconds=2))
            await workflow.execute_activity(beat, job.beats, start_to_close_timeout=_T)
        if job.mode == "stuck":
            await workflow.execute_activity(halfway, start_to_close_timeout=_T)
            await workflow.wait_condition(lambda: False)
        counts = Counter(job.text.split())
        out = WordsOut(
            upper=text,
            words=sum(counts.values()),
            counts=[Count(word=w, n=n) for w, n in sorted(counts.items())],
        )
        if job.mode == "files":
            report = await workflow.execute_activity(
                make_file, args=["report", 3], start_to_close_timeout=_T
            )
            extra = await workflow.execute_activity(
                make_file, args=["extra", 2], start_to_close_timeout=_T
            )
            out.file = report
            out.files = {"report": report, "extra": extra}  # the report twice, on purpose
        return out


@workflow.defn
class DocFlow:
    @workflow.run
    async def run(self, job: DocJob) -> DocOut:
        return DocOut(size=job.doc.size)
