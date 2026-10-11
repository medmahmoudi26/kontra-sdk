"""The old `head` method, as a Temporal activity. `actor.load` (per-worker state) becomes
module state; the fleet-wide `global_state` counters are gone — they were accounting, not
output, and a Temporal workflow can count for itself.

A target comes from the buyer, so none is trusted: whatever one does (refuse, stall, answer
garbage, redirect in circles, fail to parse as a URL), it becomes that target's `error`. It
never fails the batch: a failed batch is retried, then fails the whole job, and the buyer is
refunded for work that was done.

The old body was blocking `urllib` on a thread per target, and ported as it was it let one
target hold the worker. urllib's timeout applies to each socket operation, so a server that
sends a header byte every few seconds kept its thread as long as it liked (16 of them kept
every buyer's job on the worker waiting); and urllib turns a redirected HEAD into a GET and
reads the whole body of each redirect after that. httpx's async client keeps HEAD through
redirects (a HEAD answer has no body), and an overall deadline per target cancels whatever
the target is in the middle of.
"""

import asyncio

import httpx
from models import Batch, Head, Target
from temporalio import activity

#: Targets probed at once per batch (what a fleet of the old actor's containers gave it).
#: Each one ends within its deadline, so a batch heartbeats at least that often.
CONCURRENCY = 16
#: What one target may take in all: resolving, connecting, TLS, redirects and the headers.
TARGET_DEADLINE_SECONDS = 30
#: The Server header is the target's text: keep a sane prefix of it.
_SERVER_MAX = 256


async def _head(http: httpx.AsyncClient, target: Target) -> Head:
    url = target.address()
    try:
        async with asyncio.timeout(TARGET_DEADLINE_SECONDS):
            r = await http.head(url)
    except Exception as e:  # this target's failure, recorded; never the batch's
        return Head(url=url, error=type(e).__name__)
    return Head(url=url, status=r.status_code, server=r.headers.get("Server", "")[:_SERVER_MAX])


@activity.defn
async def head_batch(batch: Batch) -> list[Head]:
    slots = asyncio.Semaphore(CONCURRENCY)
    done = batch.done

    async def one(http: httpx.AsyncClient, target: Target) -> Head:
        nonlocal done
        async with slots:
            head = await _head(http, target)
        done += 1
        activity.heartbeat(
            {
                "percent": round(100 * done / batch.total, 1),
                "message": f"{done}/{batch.total} targets",
            }
        )
        return head

    async with httpx.AsyncClient(timeout=15, follow_redirects=True) as http:
        return list(await asyncio.gather(*(one(http, t) for t in batch.targets)))
