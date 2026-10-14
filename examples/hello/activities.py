"""Plain Temporal activities: nothing from kontra is imported here (D1). The greeting is the
whole job; a real offering does its work here, and heartbeats as it would for Temporal."""

from models import HelloOutput
from temporalio import activity


@activity.defn
async def greet(name: str) -> HelloOutput:
    return HelloOutput(greeting=f"Hi {name}")
