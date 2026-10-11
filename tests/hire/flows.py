"""The test sellers' workflows and activities: plain Temporal (an importable module, as the
workflow sandbox requires). Only ``Artifact`` (from ``models``) touches Kontra, as in a
seller's code (D1)."""

from __future__ import annotations

import asyncio
import tempfile
from datetime import timedelta
from pathlib import Path

from temporalio import activity, workflow
from temporalio.exceptions import ApplicationError

with workflow.unsafe.imports_passed_through():
    from .models import (
        Artifact,
        EnrichInput,
        EnrichOutput,
        Head,
        Part,
        ProbeInput,
        ProbeOutput,
        TextJob,
        TextOut,
    )

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
async def quiet(seconds: float) -> None:
    """Works for a while and never heartbeats: liveness comes from history alone."""
    await asyncio.sleep(seconds)


@activity.defn
async def make_file(lines: int, prefix: str) -> Artifact:
    with tempfile.TemporaryDirectory() as d:
        path = Path(d) / "lines.jsonl"
        path.write_text("".join(f'{{"line": {i}, "of": "{prefix}"}}\n' for i in range(lines)))
        return await Artifact.from_path(path, content_type="application/x-ndjson")


@activity.defn
async def read_doc(doc: Artifact) -> str:
    return (await doc.read_bytes()).decode()


@workflow.defn
class TextFlow:
    @workflow.run
    async def run(self, job: TextJob) -> TextOut:
        if job.mode == "fail":
            raise ApplicationError(FAIL_MESSAGE, non_retryable=True)
        if job.mode == "slow":
            await workflow.wait_condition(lambda: False)
        if job.mode == "graceful":
            try:
                await workflow.wait_condition(lambda: False)
            except asyncio.CancelledError:  # a common pattern: hand back what there is
                return TextOut(
                    text="partial",
                    length=0,
                    summary={"chars": 0, "beats": 0},
                    parts=[Part(index=0), Part(index=1)],
                )
        text = await workflow.execute_activity(upper, job.text, start_to_close_timeout=_T)
        if job.mode == "beats":
            await workflow.execute_activity(beat, job.beats, start_to_close_timeout=_T)
        if job.mode == "quiet":  # ``beats`` seconds of work, 2 by default
            await workflow.execute_activity(quiet, float(job.beats or 2), start_to_close_timeout=_T)
        out = TextOut(
            text=text,
            length=len(job.text),
            summary={"chars": len(job.text), "beats": job.beats},
            parts=[Part(index=0), Part(index=1)],
        )
        if job.mode == "artifact":
            if job.doc is not None:
                out.text = await workflow.execute_activity(
                    read_doc, job.doc, start_to_close_timeout=_T
                )
            file = await workflow.execute_activity(
                make_file, args=[job.beats, "main"], start_to_close_timeout=_T
            )
            extra = await workflow.execute_activity(
                make_file, args=[2, "extra"], start_to_close_timeout=_T
            )
            out.file = file
            out.parts[1].file = extra
            out.files = {"extra": extra}
        return out


@workflow.defn
class ProbeFlow:
    """Fails (after a moment, so a sibling job is running) when a target mentions ``fail``."""

    @workflow.run
    async def run(self, inp: ProbeInput) -> ProbeOutput:
        if any("fail" in t.url for t in inp.targets):
            await workflow.sleep(timedelta(seconds=3))
            raise ApplicationError("every probe failed", non_retryable=True)
        return ProbeOutput(heads=[Head(url=t.url, status=200) for t in inp.targets])


@workflow.defn
class EnrichFlow:
    """Runs until cancelled when a url mentions ``fail`` or ``slow``."""

    @workflow.run
    async def run(self, inp: EnrichInput) -> EnrichOutput:
        if any("fail" in u or "slow" in u for u in inp.urls):
            await workflow.wait_condition(lambda: False)
        return EnrichOutput(domains=sorted({u.split("/")[2] for u in inp.urls}))
