"""A buyer workflow that imports ``kontra.hire`` with no pass-through: the sandbox imports
the hire module itself, so everything ``service()`` touches must be sandbox-safe."""

from __future__ import annotations

from typing import Any

from temporalio import workflow

from kontra.hire import service


@workflow.defn
class HireSandboxed:
    @workflow.run
    async def run(self, offering: str) -> dict[str, Any]:
        out = await service(offering, max_price=1).execute(text="sandboxed")
        return out.to_dict()
