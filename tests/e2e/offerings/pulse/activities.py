"""Vanilla Temporal: temporalio only, no kontra import (decisions I29). `pulse` heartbeats as any Temporal
activity would; `quiet` never does."""

import asyncio

from models import PulseInput
from temporalio import activity

#: Slower than the forwarder's default interval (1 s, latest wins: PRD §8.4), with room
#: for a slow post, so that under the settings a seller ships with every heartbeat is its
#: own event; and below the gate's per-job heartbeat rate (5 per second).
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
