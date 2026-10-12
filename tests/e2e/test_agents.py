"""An agent's door against the real stack (acceptance 12): ``kontra mcp`` on the buyer's
profile, spoken to over stdio as an agent's host would, through search, quote, hire, result
and accept of a seller serving with ``kontra serve``."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import pytest
from mcp import Client, StdioServerParameters
from mcp.client.stdio import stdio_client
from mcp.types import CallToolResult

from kontra.gate_client import models as m

from .support import Stack, scaffold, serving

pytestmark = [pytest.mark.e2e]

TOOLS = {"search", "describe", "quote", "hire", "status", "result", "accept", "reject"}
TEXT = "the quick brown fox jumps over the lazy dog\nthe end\n"


def _text(answer: CallToolResult) -> str:
    return "".join(getattr(c, "text", "") for c in answer.content)


async def _ok(client: Client, tool: str, **arguments: Any) -> dict[str, Any]:
    answer = await client.call_tool(tool, arguments)
    assert not answer.is_error, _text(answer)
    assert answer.structured_content is not None
    assert json.loads(_text(answer)) == answer.structured_content
    return answer.structured_content


@pytest.mark.timeout(600)
async def test_ac12_mcp_search_quote_hire_result_accept_against_kontra_dev(
    stack: Stack, tmp_path: Path
) -> None:
    seller = await stack.party("ac12")
    buyer = await stack.party("ac12-buyer")
    project, key = await scaffold(seller, tmp_path)
    params = StdioServerParameters(
        command=sys.executable, args=["-m", "kontra.cli", "mcp"], env=buyer.env(), cwd=tmp_path
    )
    errors = tmp_path / "mcp-stderr.log"
    async with serving(project, seller, [key], log=tmp_path / "serve.log"):
        with errors.open("w") as errlog:
            try:
                async with Client(stdio_client(params, errlog=errlog)) as client:
                    assert {t.name for t in (await client.list_tools()).tools} == TOOLS
                    found = await _ok(client, "search", query=seller.handle)
                    assert key in {o["offering"] for o in found["offerings"]}, found
                    quoted = await _ok(client, "quote", offering=key, input={"text": TEXT})
                    hired = await _ok(client, "hire", quote_id=quoted["quote_id"], max_price="1.00")
                    got = await _ok(client, "result", job_id=hired["job_id"], wait_seconds=120)
                    accepted = await _ok(client, "accept", job_id=hired["job_id"])
            except BaseException:
                print(errors.read_text(), file=sys.stderr)
                raise
    print(
        f"\n[acceptance 12] search found {key}; quote {quoted['price']} {quoted['currency']}; "
        f"job {hired['job_id']}: output {got['output']}; accept -> {accepted['escrow']}"
    )
    assert (got["output"]["words"], got["output"]["lines"]) == (11, 2)
    assert accepted["escrow"] == m.Escrow.RELEASED.value
    k = buyer.kontra()
    try:
        balance = await k.balance()
    finally:
        await k.aclose()
    assert (balance.available, balance.held_minor) == ("99.99", 0)
