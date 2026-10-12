"""``examples/recon``: a buyer's workflow composing the two example sellers (``acme/probe``
from ``examples/legacy-actor`` and ``acme/enrich`` from ``examples/enrich``), unchanged, on
``kontra.hire.worker``. The happy path, and acceptance 11: when the probe fails, the
enrichment -- still running -- is cancelled and its hold refunded.

Recon builds ``https://<domain>/`` and ``https://www.<domain>/``; the domain here is
``127.0.0.1:<port>`` of a local HTTPS site, so the ``www.`` URL cannot resolve and is an
error row in both outputs."""

from __future__ import annotations

import json
import uuid
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import main as recon_main  # pyright: ignore[reportMissingImports] -- examples/recon, on sys.path
import pytest
import pytest_asyncio
from recon import Recon  # pyright: ignore[reportMissingImports]
from temporalio.client import WorkflowExecutionStatus, WorkflowFailureError, WorkflowHandle
from temporalio.exceptions import ApplicationError

from kontra.artifacts import download
from kontra.contract.names import job_workflow_id
from kontra.gate_client import models as m
from kontra.hire import HireWorker, worker
from kontra.types import Artifact

from .sites import SERVER
from .support import START, Buyer, Market, until

pytestmark = [pytest.mark.temporal, pytest.mark.asyncio(loop_scope="package")]

PROBE, ENRICH = "acme/probe@1.0.0", "acme/enrich@1.2.0"


@dataclass
class Door:
    market: Market
    buyer: Buyer
    worker: HireWorker
    task_queue: str

    async def start(self, domain: str) -> WorkflowHandle[Any, Any]:
        return await self.worker.client.start_workflow(
            Recon.run, domain, id=f"recon-{uuid.uuid4().hex[:8]}", task_queue=self.task_queue
        )

    async def jobs(self, ok: Callable[[list[m.JobView]], bool]) -> list[m.JobView]:
        return await until(self.buyer.k.jobs, lambda views: len(views) == 2 and ok(views))

    def by_offering(self, views: list[m.JobView]) -> dict[str, m.JobView]:
        return {v.offering: v for v in views}


@pytest_asyncio.fixture(loop_scope="package")
async def door(market: Market) -> AsyncIterator[Door]:
    buyer = await market.buyer()
    task_queue = f"recon-{uuid.uuid4().hex[:8]}"
    w = await worker(
        market.mp.client(buyer.party.namespace),
        task_queue=task_queue,
        workflows=[Recon],
        kontra=buyer.k,
    )
    async with w:
        yield Door(market, buyer, w, task_queue)


async def test_recon_composes_probe_and_enrich(door: Door, tmp_path: Path) -> None:
    domain = door.market.sites.host("secure")
    out = await (await door.start(domain)).result()
    assert out["domain"] == domain
    apex, www = f"https://{domain}/", f"https://www.{domain}/"
    assert out["heads"] == {
        "heads": [
            {"url": apex, "status": 200, "server": SERVER, "error": None},
            {"url": www, "status": None, "server": "", "error": "ConnectError"},
        ]
    }
    assert out["enrich"]["summary"] == {"urls": 2, "ok": 1, "failed": 1}

    # The workflow returns the artifact as JSON; the buyer downloads it with its own key.
    records = Artifact.model_validate(out["enrich"]["records"])
    k, identity = door.buyer.k, door.buyer.party.identity
    path = await download(k.gate, records, identity, tmp_path / "records.jsonl")
    lines = [json.loads(line) for line in path.read_text().splitlines()]
    assert [(r["url"], r["status"], r["title"], r["error"]) for r in lines] == [
        (apex, 200, "Home", None),
        (www, None, None, "ConnectError"),
    ]

    views = door.by_offering(
        await door.jobs(lambda vs: all(v.state is m.JobState.COMPLETED for v in vs))
    )
    assert set(views) == {PROBE, ENRICH}
    assert {v.escrow for v in views.values()} == {m.Escrow.HELD}  # delivered, in review
    bal = await door.buyer.balance()
    assert (bal.available_minor, bal.held_minor) == (START - 2, 2)  # 0.01 + 0.01


async def test_recon_refunds_the_enrichment_when_the_probe_fails(door: Door) -> None:
    """Acceptance 11. Both jobs run against a site that stalls; then the probe's seller fails
    its job (it terminates the workflow, as ``temporal workflow terminate`` would). Recon
    sees ``JobFailed`` and cancels the enrichment while it still runs: its hold refunds and
    the seller's workflow ends cancelled. The probe refunds as a failed job."""
    market, sites = door.market, door.market.sites
    sites.rearm()
    before = sites.stalled
    try:
        handle = await door.start(sites.host("stall"))
        working = await door.jobs(lambda vs: all(v.state is m.JobState.WORKING for v in vs))

        async def stalled() -> int:
            return sites.stalled

        await until(stalled, lambda n: n >= before + 2)  # the probe and the enrichment, mid-request
        probe = door.by_offering(working)[PROBE]
        seller = market.seller_client()
        await seller.get_workflow_handle(job_workflow_id(probe.job_id)).terminate(
            reason="the probe's seller stopped the job"
        )

        with pytest.raises(WorkflowFailureError) as failed:
            await handle.result()
        cause = failed.value.cause
        assert isinstance(cause, ApplicationError)
        assert cause.type == "kontra.JobFailed"

        views = door.by_offering(
            await door.jobs(lambda vs: all(v.escrow is m.Escrow.REFUNDED for v in vs))
        )
        assert (views[PROBE].state, views[ENRICH].state) == (
            m.JobState.FAILED,
            m.JobState.CANCELED,  # cancelled by Recon while it ran, and refunded
        )
        assert (views[PROBE].reason, views[ENRICH].reason) == (
            "seller workflow terminated",
            "canceled by buyer",
        )
        enrich = seller.get_workflow_handle(job_workflow_id(views[ENRICH].job_id))
        assert (await enrich.describe()).status is WorkflowExecutionStatus.TERMINATED
        bal = await door.buyer.balance()
        assert (bal.available_minor, bal.held_minor) == (START, 0)
    finally:
        sites.release()


async def test_main_runs_recon_from_the_profile(
    market: Market, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """``python main.py <domain>``: a worker from the buyer's profile alone, one recon,
    the result printed as JSON."""
    buyer = await market.buyer()
    monkeypatch.setenv("KONTRA_HOME", str(buyer.party.home))
    domain = market.sites.host("secure")
    await recon_main.main(domain)
    out = json.loads(capsys.readouterr().out)
    assert out["domain"] == domain
    assert out["heads"]["heads"][0]["status"] == 200
    assert out["enrich"]["summary"] == {"urls": 2, "ok": 1, "failed": 1}


@pytest.mark.parametrize(
    "domain",
    [
        "",
        "example.com\n",  # a trailing newline: fullmatch, not match
        "example.com/../admin",
        "example.com?x=1",
        "-example.com",
        "user@example.com",
        "example.com:123456",
        "a" * 254,
    ],
)
async def test_main_refuses_what_is_not_a_domain(domain: str) -> None:
    """The domain is typed by a person and lands in URLs and a workflow id: checked whole,
    before anything connects."""
    with pytest.raises(ValueError, match="not a domain"):
        await recon_main.main(domain)


async def test_main_accepts_domains() -> None:
    for domain in ("example.com", "sub.example-1.co.uk", "127.0.0.1:8443", "a" * 253):
        assert recon_main.check_domain(domain) == domain
