"""The test sellers' bindings, as a seller's ``quote.py`` and ``worker.py`` would write them:
``acme/text`` (every lifecycle the buyer tests need) and the recon example's ``acme/probe``
and ``acme/enrich``. Each test writes its own versions, so no two tests share a task queue
on the shared dev server."""

from __future__ import annotations

import asyncio
import itertools
import json
import os
import uuid
from decimal import Decimal
from pathlib import Path
from typing import Any

from kontra.serve import BoundOffering, Quote, Worker, offering

from .flows import (
    EnrichFlow,
    FeedFlow,
    ProbeFlow,
    TextFlow,
    beat,
    feed,
    make_file,
    quiet,
    read_doc,
    upper,
)
from .models import EnrichInput, FeedJob, ProbeInput, TextJob

_versions = itertools.count(1)


def unique_version() -> str:
    return f"2.{os.getpid() % 100_000}.{next(_versions)}"


def write_offering(directory: Path, name: str, **facts: Any) -> Path:
    spec: dict[str, Any] = {
        "name": name,
        "version": unique_version(),
        "description": f"The {name} test offering",
        "max_duration": "2m",
        "review_window": "1h",
        "max_output_bytes": "1MB",
        "artifact_retention": "1d",
        **facts,
    }
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"offering-{uuid.uuid4().hex[:8]}.yaml"
    path.write_text("".join(f"{k}: {json.dumps(v)}\n" for k, v in spec.items()))
    return path


#: Cents added to every price from now on: a test raises it between quote and execute, so
#: the handler's re-quote (decisions I10) prices above the hold and refuses the start.
SURCHARGE = {"cents": 0}
#: ``stall-...``, ``late-...`` and ``dearer-...`` texts: how often each was priced. Their
#: second pricing (the handler's re-quote inside ``execute``) is slow (a seller slow to start
#: the job: ``stall`` for longer than a short test's max_duration, ``late`` for a part of it)
#: or higher than the quote (the handler refuses the start).
_PRICED: dict[str, int] = {}
STALL_SECONDS = 6.0
LATE_SECONDS = 3.0


async def quote_text(job: TextJob) -> Quote:
    """One cent per character, at least one cent."""
    if job.text == "reject me":
        return Quote.reject("this text is not for sale")
    if job.text == "busy":
        return Quote.busy(30)
    cents = max(Decimal(len(job.text)), Decimal(1)) + SURCHARGE["cents"]
    if job.text.startswith(("stall", "late", "dearer")):
        _PRICED[job.text] = _PRICED.get(job.text, 0) + 1
        if _PRICED[job.text] > 1 and job.text.startswith("stall"):
            await asyncio.sleep(STALL_SECONDS)
        if _PRICED[job.text] > 1 and job.text.startswith("late"):
            await asyncio.sleep(LATE_SECONDS)
        if _PRICED[job.text] > 1 and job.text.startswith("dearer"):
            cents += 50  # the re-quote inside execute: dearer than the hold
    return Quote(price=cents / 100)


async def quote_probe(inp: ProbeInput) -> Quote:
    return Quote(price=Decimal("0.10"))


async def quote_enrich(inp: EnrichInput) -> Quote:
    return Quote(price=Decimal("1.50"))


def text_offering(directory: Path, **facts: Any) -> BoundOffering:
    return offering(
        write_offering(directory, "acme/text", **facts), workflow=TextFlow, quote=quote_text
    )


def text_worker(*bound: BoundOffering) -> Worker:
    return Worker(offerings=list(bound), activities=[upper, beat, quiet, make_file, read_doc])


def recon_worker(directory: Path) -> Worker:
    probe = offering(write_offering(directory, "acme/probe"), workflow=ProbeFlow, quote=quote_probe)
    enrich = offering(
        write_offering(directory, "acme/enrich"), workflow=EnrichFlow, quote=quote_enrich
    )
    return Worker(offerings=[probe, enrich])


async def quote_feed(job: FeedJob) -> Quote:
    return Quote(price=Decimal("0.05"))


def feed_worker(directory: Path) -> tuple[Worker, BoundOffering]:
    """``acme/feed``, ``streams: [events]``."""
    bound = offering(
        write_offering(directory, "acme/feed", streams=["events"]),
        workflow=FeedFlow,
        quote=quote_feed,
    )
    return Worker(offerings=[bound], activities=[feed]), bound
