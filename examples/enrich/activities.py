"""Plain Temporal activities. Nothing from kontra is imported here (D1): progress is the
heartbeat you would write for Temporal anyway, and the file upload goes through the
`Artifact` type that lives in models.py."""

import html
import json
import re
import tempfile
from pathlib import Path
from urllib.parse import urlsplit

import httpx
from models import Artifact, Batch, Record
from temporalio import activity

_TITLE = re.compile(r"<title[^>]*>(.*?)</title>", re.IGNORECASE | re.DOTALL)


async def _fetch(http: httpx.AsyncClient, url: str) -> Record:
    domain = urlsplit(url).hostname or ""
    try:
        r = await http.get(url)
    except httpx.HTTPError as e:
        return Record(url=url, domain=domain, error=type(e).__name__)
    m = _TITLE.search(r.text[:200_000])
    title = html.unescape(" ".join(m.group(1).split()))[:300] if m else None
    return Record(url=url, domain=domain, status=r.status_code, title=title)


@activity.defn
async def fetch_batch(batch: Batch) -> list[Record]:
    records: list[Record] = []
    async with httpx.AsyncClient(timeout=10, follow_redirects=True) as http:
        for url in batch.urls:
            records.append(await _fetch(http, url))
            n = batch.done + len(records)
            activity.heartbeat(
                {"percent": round(100 * n / batch.total, 1), "message": f"{n} records"}
            )
    return records


@activity.defn
async def write_records(records: list[Record]) -> Artifact:
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "records.jsonl"
        with path.open("w") as f:
            for rec in records:
                f.write(json.dumps(rec.model_dump(mode="json")) + "\n")
        return await Artifact.from_path(path, content_type="application/x-ndjson")
