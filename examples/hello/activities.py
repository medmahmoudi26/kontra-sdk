"""Temporal activities through the kontra façade (D1): `activity` is Temporal's, and its
`defn` also notes the activity for `serve()` to register. The greeting is the whole job; a
real offering does its work here, and heartbeats as it would for Temporal."""

from models import HelloOutput

from kontra import activity


@activity.defn
async def greet(name: str) -> HelloOutput:
    return HelloOutput(greeting=f"Hi {name}")
