"""The batch tests' activities and reducers, through the façade as a seller writes them.
Module-level, as ``execute_activities_in_parallel`` requires; importable in the sandbox."""

from __future__ import annotations

import asyncio

from kontra import ApplicationError, Page, activity

from .models import Hit, Tally

#: Items per page of the paged sources.
PAGE = 7
#: Item activities running right now, and the most there ever were (``slow``). Activity
#: side only: workflow code never reads it.
LIVE = {"now": 0, "max": 0}


@activity.defn
async def double(x: int) -> int:
    return 2 * x


@activity.defn
async def shout(text: str) -> str:
    return text.upper()


@activity.defn
async def offsets(cursor: int | None) -> Page[int]:
    """Offset paging over 0..29."""
    start = cursor or 0
    end = min(start + PAGE, 30)
    return Page(items=list(range(start, end)), next=end if end < 30 else None)


@activity.defn
async def tokens(cursor: str | None) -> Page[str]:
    """Token paging: three pages of words; the second one empty, as some APIs return."""
    pages = {
        None: (["alpha", "beta"], "t1"),
        "t1": ([], "t2"),
        "t2": (["gamma"], None),
    }
    words, nxt = pages[cursor]
    return Page(items=words, next=nxt)


@activity.defn
async def stuck(cursor: str | None) -> Page[str]:
    return Page(items=[], next="same")


@activity.defn
async def slow(x: int) -> int:
    LIVE["now"] += 1
    LIVE["max"] = max(LIVE["max"], LIVE["now"])
    try:
        await asyncio.sleep(0.05)
    finally:
        LIVE["now"] -= 1
    return x


@activity.defn
async def fragile(x: int) -> int:
    if x == 3:
        raise ApplicationError(f"item {x} is bad", type="BadItem", non_retryable=True)
    await asyncio.sleep(0.2)
    return x


@activity.defn
async def probe(target: str) -> Hit:
    return Hit(target=target, open=target.endswith(("0", "5")))


def add(acc: int, r: int) -> int:
    return acc + r


def tally(acc: Tally, hit: Hit) -> Tally:
    # Attribute access: fails if either arrived as a dict (after continue-as-new, say).
    acc.seen += 1
    if hit.open:
        acc.open.append(hit.target)
    return acc
