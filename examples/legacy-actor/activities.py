"""The old `head` method body, as a Temporal activity. `actor.load` (a per-worker opener)
becomes module state; the fleet-wide `global_state` counters are gone — they were accounting,
not output, and a Temporal workflow can count for itself."""

import asyncio
import urllib.error
import urllib.request

from models import Head, Target
from temporalio import activity

_open = urllib.request.build_opener().open


def _head(url: str) -> Head:
    req = urllib.request.Request(url, method="HEAD")
    try:
        with _open(req, None, 15) as r:
            return Head(url=url, status=r.status, server=r.headers.get("Server", ""))
    except urllib.error.HTTPError as e:
        return Head(url=url, status=e.code, server=e.headers.get("Server", "") if e.headers else "")
    except (urllib.error.URLError, TimeoutError, OSError) as e:
        return Head(url=url, error=type(e).__name__)


@activity.defn
async def head_batch(targets: list[Target]) -> list[Head]:
    heads: list[Head] = []
    for i, t in enumerate(targets, 1):
        heads.append(await asyncio.to_thread(_head, t.address()))
        activity.heartbeat(
            {"percent": round(100 * i / len(targets), 1), "message": f"{i}/{len(targets)} targets"}
        )
    return heads
