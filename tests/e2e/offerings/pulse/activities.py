"""Vanilla Temporal: temporalio only, no kontra import (decisions I29). `pulse` heartbeats as any Temporal
activity would; `quiet` never does."""

import asyncio

from models import PulseInput
from temporalio import activity

#: Faster than the forwarder's default interval (5 s, latest wins: decisions I31), so the
#: buyer sees the pacing: about one heartbeat in three, in order, and the last one.
SPACING_SECONDS = 1.5


@activity.defn
async def pulse(inp: PulseInput) -> int:
    for beat in range(1, inp.beats + 1):
        activity.heartbeat(
            {"percent": round(100 * beat / inp.beats, 2), "message": f"beat {beat} {inp.tag}"}
        )
        await asyncio.sleep(SPACING_SECONDS)
    return inp.beats


@activity.defn
async def quiet(seconds: int) -> int:
    await asyncio.sleep(seconds)
    return seconds
