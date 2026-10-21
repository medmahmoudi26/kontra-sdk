"""``examples/buyer-harness-agent`` end to end, on the dev gate (``kontra.testing.Marketplace``)
with a Temporal dev server: a buyer's own harness agent, run as ``python main.py`` with the
buyer's profile, hires ``acme/recon-harness`` -- served in the same test, its own
``worker.py`` as ``acme`` -- as a tool, through the glue of ``tools.py`` and the client
``kontra pull`` wrote (``gen/recon_harness``).

Spend control is the harness's approval policy: under the spending cap the agent's evaluator
approves the spend and the typed report comes back through the tool; above it the spend is put
to the person at the terminal, and a no hires nothing -- no job at the gate, no hold -- while a
yes hires with ``max_price`` equal to the approved quote, so the escrow can never hold more.
Then the pulled client is what ``kontra pull`` writes for the seller's published contract."""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import shutil
import subprocess
import sys
from collections.abc import Generator
from pathlib import Path
from typing import Any

import pytest

from kontra.cli._nexgen import Nexgen
from kontra.cli._pull import DOCUMENT_NAME
from kontra.contract import from_yaml, nexgen_view, to_yaml
from kontra.gate_client import models as m

from .support import EXAMPLES, START, Buyer, Market, serving

BUYER = EXAMPLES / "buyer-harness-agent"
PULLED = BUYER / "gen" / "recon_harness"
SELLER = "recon-harness"
OFFERING = "acme/recon-harness@0.1.0"

asynchronous = pytest.mark.asyncio(loop_scope="package")


@contextlib.contextmanager
def hires(market: Market) -> Generator[list[m.HireRequest]]:
    """Every hire request the gate receives meanwhile: what a buyer's client asked to lock."""
    gate = market.mp.gate
    real = gate.hire
    seen: list[m.HireRequest] = []

    async def spy(who: Any, req: m.HireRequest) -> m.HireView:
        seen.append(req)
        return await real(who, req)

    gate.hire = spy  # type: ignore[method-assign]
    try:
        yield seen
    finally:
        del gate.hire  # the class's own again


async def run_main(
    buyer: Buyer, domains: list[str], *, cap: str | None, answer: str = ""
) -> tuple[dict[str, Any], str]:
    """``python main.py [--cap CAP] DOMAIN...`` with the buyer's profile, ``answer`` typed at its
    terminal; its JSON answer and what it wrote on stderr (the question, if it asked one)."""
    env = {k: v for k, v in os.environ.items() if not k.startswith(("KONTRA_", "LLM_", "OPENAI_"))}
    env.update(KONTRA_HOME=str(buyer.party.home), PYTHONUNBUFFERED="1")
    args = [sys.executable, "main.py", *(["--cap", cap] if cap is not None else []), *domains]
    proc = await asyncio.create_subprocess_exec(
        *args,
        cwd=BUYER,
        env=env,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    out, err = await asyncio.wait_for(proc.communicate(answer.encode()), 300)
    assert proc.returncode == 0, err.decode(errors="replace")[-6000:]
    return json.loads(out), err.decode(errors="replace")


def _question(price: str) -> str:
    return f"agent wants to spend {price} USD hiring {OFFERING} -- approve? [y/N] "


@pytest.mark.temporal
@asynchronous
async def test_the_agent_hires_the_seller_as_a_tool_within_its_spending_policy(
    market: Market, tmp_path: Path
) -> None:
    async with serving(market, SELLER, tmp_path / f"{SELLER}.log"):
        # Under the cap (0.02 against the agent's own 0.05): approved by the policy, no question.
        buyer = await market.buyer()
        with hires(market) as asked:
            reply, said = await run_main(buyer, ["example.com", "example.org"], cap=None)
        assert "approve?" not in said
        assert reply["hired"] is True
        assert reply["targets"] == 2  # the typed SweepReport's, through the tool
        assert reply["answer"] == f"2 hosts swept: {reply['expired']} expired, {reply['weak']} weak"
        [job] = await buyer.k.jobs()
        assert [r.max_price_minor for r in asked] == [2]  # the approved quote, to the cent
        assert (job.offering, job.state, job.escrow) == (
            OFFERING,
            m.JobState.COMPLETED,
            m.Escrow.HELD,
        )
        balance = await buyer.balance()
        assert (balance.available_minor, balance.held_minor) == (START - 2, 2)

        # Above the cap (0.02 against 0.01): put to the person, who says no. Nothing is hired.
        buyer = await market.buyer()
        with hires(market) as asked:
            reply, said = await run_main(
                buyer, ["example.com", "example.org"], cap="0.01", answer="n\n"
            )
        assert _question("0.02") in said
        assert reply["hired"] is False
        assert reply["answer"].startswith(
            "not hired: the spend was not approved (denied at the terminal"
        )
        assert asked == []  # no hire request: no job at the gate, no hold
        assert await buyer.k.jobs() == []
        balance = await buyer.balance()
        assert (balance.available_minor, balance.held_minor) == (START, 0)

        # Above the cap again, and a yes: hired, the hold at most what was approved.
        buyer = await market.buyer()
        domains = ["example.com", "example.org", "example.net"]
        with hires(market) as asked:
            reply, said = await run_main(buyer, domains, cap="0.01", answer="y\n")
        assert _question("0.03") in said
        assert (reply["hired"], reply["targets"]) == (True, 3)
        assert [r.max_price_minor for r in asked] == [3]
        [job] = await buyer.k.jobs()
        assert (job.amount_minor, job.escrow) == (3, m.Escrow.HELD)


# --------------------------------------------------------------------------- the pulled client


def test_the_pulled_client_is_the_sellers_published_contract() -> None:
    """``gen/recon_harness`` is what ``kontra pull acme/recon-harness --lang python`` wrote: the
    document it verified and handed nexgen is the nexgen view of the seller's golden contract
    (the published one: same hash), so the types the glue uses are the contract's."""
    golden = from_yaml((EXAMPLES / SELLER / "contract.yaml").read_text(encoding="utf-8"))
    assert (PULLED / DOCUMENT_NAME).read_text(encoding="utf-8") == to_yaml(nexgen_view(golden))
    assert {p.name for p in PULLED.iterdir() if p.suffix == ".py"} == {
        "__init__.py",
        "_definitions.py",
        "models.py",
        "services.py",
    }


def test_the_pulled_client_is_nexgens_of_it(tmp_path: Path) -> None:
    """And nexgen 0.2.5 (``kontra pull``'s pin) generates exactly these files from it again.
    Needs the binary (``NEXGEN_BIN``), like the other nexgen tests."""
    binary = os.environ.get("NEXGEN_BIN") or shutil.which("nexgen")
    if not binary:
        pytest.skip("set NEXGEN_BIN to a nexgen 0.2.5 linux binary")
    out = tmp_path / "recon_harness"
    out.mkdir()
    shutil.copy(PULLED / DOCUMENT_NAME, out / DOCUMENT_NAME)
    Nexgen(Path(binary)).generate("python", out / DOCUMENT_NAME, out)
    for path in sorted(PULLED.glob("*.py")):
        assert (out / path.name).read_bytes() == path.read_bytes(), path.name


def test_the_glue_sends_and_reads_the_contracts_json() -> None:
    """The pulled types through their own converter, as the tool sends and reads them: the
    input as the contract's JSON (the defaults left out), a delivery read back typed, and one
    that is not the contract's output refused."""
    probe = (
        "import asyncio, json\n"
        "from gen.recon_harness import SweepReport, SweepRequest\n"
        "from tools import from_wire, to_wire\n"
        "print(json.dumps(asyncio.run(to_wire(SweepRequest(domains=['a.example'])))))\n"
        "full = {'domains': 1, 'targets': 1, 'expired': 0, 'weak': 1, 'posted': None,"
        " 'confirmed': None, 'summary': 's'}\n"
        "r = asyncio.run(from_wire(full, SweepReport))\n"
        "print(json.dumps([type(r).__name__, r.weak, r.posted]))\n"
        "del full['summary']\n"
        "try:\n"
        "    asyncio.run(from_wire(full, SweepReport))\n"
        "except Exception as e:\n"
        "    print(json.dumps(type(e).__name__))\n"
    )
    done = subprocess.run(
        [sys.executable, "-c", probe],
        cwd=BUYER,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert done.returncode == 0, done.stderr
    sent, read, refused = (json.loads(line) for line in done.stdout.strip().splitlines()[-3:])
    assert sent == {"domains": ["a.example"]}
    assert read == ["SweepReport", 1, None]
    assert refused == "ApplicationError"  # temporalio's payload validation error
