"""The test seller's bindings, as its ``quote.py`` and ``worker.py`` would write them:
``acme/words`` at one cent per character, and ``acme/docs``, whose input is a file. Every
module run publishes its own versions, so no two runs share a task queue on the shared dev
server."""

from __future__ import annotations

import json
import uuid
from decimal import Decimal
from pathlib import Path

from mcp_flows import DocFlow, WordsFlow, beat, halfway, make_file, upper
from mcp_models import DocJob, WordsJob

from kontra.serve import BoundOffering, Quote, Worker, offering

NAME = "acme/words"
DOCS = "acme/docs"
REJECTED = "reject me"
REJECT_REASON = "this text is not for sale"


async def quote_words(job: WordsJob) -> Quote:
    """One cent per character."""
    if job.text == REJECTED:
        return Quote.reject(REJECT_REASON)
    return Quote(price=Decimal(len(job.text)) / 100)


def _write(directory: Path, name: str, **facts: object) -> Path:
    spec = {
        "name": name,
        "version": f"3.{uuid.uuid4().int % 100_000}.{uuid.uuid4().int % 100_000}",
        "description": "Upper-cases a text and counts its words",
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


async def quote_doc(job: DocJob) -> Quote:
    return Quote(price=Decimal("0.01"))


def words_offering(directory: Path) -> BoundOffering:
    path = _write(directory, NAME, pricing_hint="1 cent per character", tags=["text", "words"])
    return offering(path, workflow=WordsFlow, quote=quote_words)


def docs_offering(directory: Path) -> BoundOffering:
    path = _write(directory, DOCS, description="Measures a file")
    return offering(path, workflow=DocFlow, quote=quote_doc)


def words_worker(*bound: BoundOffering) -> Worker:
    return Worker(offerings=list(bound), activities=[upper, beat, halfway, make_file])
