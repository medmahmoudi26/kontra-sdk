"""Money that moves with nobody acting, against the real stack: an offline seller refunds by
its deadline (acceptance 4), a silent buyer pays at the end of the review window (5), and a
buyer's workflow composing two offerings is refunded the second when the first fails (11).

The gate's clocks are its own (a sweep every ``KONTRA_SWEEP_INTERVAL``, 2 s, and a grace of
``KONTRA_GRACE_SECONDS``, 30 s, after ``max_duration``); the offerings here are the examples,
copied with short durations into a temporary directory."""

from __future__ import annotations

import importlib.util
import sys
import time
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest
from temporalio.client import WorkflowExecutionStatus, WorkflowFailureError
from temporalio.exceptions import ApplicationError

from kontra.contract.names import job_workflow_id
from kontra.gate_client import models as m
from kontra.hire import JobFailed

from .support import EXAMPLES, Stack, copy_project, serving, until

pytestmark = [pytest.mark.e2e]


def _sites_module() -> ModuleType:
    """``tests/examples/sites.py`` (local web sites, one of which stalls), loaded by path:
    the unit tests' package is not importable from here by name."""
    name = "kontra_e2e_sites"
    if name in sys.modules:
        return sys.modules[name]
    path = Path(__file__).resolve().parents[1] / "examples" / "sites.py"
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


# --------------------------------------------------------------------------- acceptance 4


@pytest.mark.timeout(900)
async def test_ac04_an_offline_seller_refunds_within_max_duration_plus_a_minute(
    stack: Stack, tmp_path: Path
) -> None:
    """``examples/enrich`` with ``max_duration: 2m``: the seller answers the quote and goes
    offline; the buyer hires anyway and does nothing else. The hold refunds within
    max_duration + 1 minute of the hire, and the buyer's call ends failed."""
    seller = await stack.party("ac04")
    buyer = await stack.party("ac04-buyer")
    project = copy_project(
        EXAMPLES / "enrich",
        tmp_path / "enrich",
        name=f"{seller.handle}/enrich",
        max_duration="2m",
    )
    key = f"{seller.handle}/enrich@1.2.0"
    max_duration = 120.0
    k = buyer.kontra()
    try:
        async with serving(project, seller, [key], log=tmp_path / "serve.log"):
            q = await k.quote(key, urls=["https://example.com/"])
            assert q.ok, q.reason
        # Nobody serves the offering any more.
        hired_at = time.monotonic()
        job = await q.hire(max_price="1.00")
        held = await job.status()
        assert (held.state, held.escrow) == (m.JobState.SUBMITTED, m.Escrow.HELD), held
        assert (await k.balance()).held_minor == job.amount_minor

        closed = await until(
            job.status,
            lambda v: v.escrow is not m.Escrow.HELD,
            within=max_duration + 120,
            what="the refund",
        )
        elapsed = time.monotonic() - hired_at
        print(
            f"\n[acceptance 4] max_duration {max_duration:.0f}s: refunded {elapsed:.1f}s after "
            f"the hire ({closed.state.value}: {closed.reason})"
        )
        assert closed.escrow is m.Escrow.REFUNDED, closed
        assert closed.state is m.JobState.FAILED, closed
        assert closed.started_at is None
        assert elapsed <= max_duration + 60
        assert elapsed >= max_duration  # not before the seller's time was up
        balance = await k.balance()
        assert (balance.available, balance.held_minor) == ("100.00", 0)
        with pytest.raises(JobFailed):
            await job.result(timeout=60)
    finally:
        await k.aclose()


# --------------------------------------------------------------------------- acceptance 5


@pytest.mark.timeout(900)
async def test_ac05_a_silent_buyer_releases_at_review_window_expiry(
    stack: Stack, tmp_path: Path
) -> None:
    """``examples/enrich`` with ``review_window: 30s``: the job delivers, the buyer reads the
    result and says nothing. The escrow releases to the seller when the window closes (the
    platform's fee taken), not before."""
    seller = await stack.party("ac05")
    buyer = await stack.party("ac05-buyer")
    project = copy_project(
        EXAMPLES / "enrich",
        tmp_path / "enrich",
        name=f"{seller.handle}/enrich",
        review_window="30s",
        max_duration="5m",
    )
    key = f"{seller.handle}/enrich@1.2.0"
    window = timedelta(seconds=30)
    # 200 URLs (0.20 USD, so the 5% fee is a whole cent) that the seller refuses to fetch
    # (a loopback address): each becomes an error record at once, with no network.
    urls = [f"http://127.0.0.1:9/{i}" for i in range(200)]
    k = buyer.kontra()
    sk = seller.kontra()
    try:
        async with serving(project, seller, [key], log=tmp_path / "serve.log"):
            job = await k.hire(key, max_price="1.00", urls=urls)
            result = await job.result(timeout=180)
            assert result.summary == {"urls": 200, "ok": 0, "failed": 200}
            delivered = await until(
                job.status, lambda v: v.state is m.JobState.COMPLETED, within=60
            )
            assert delivered.escrow is m.Escrow.HELD
            assert delivered.delivery is not None
            assert delivered.review_deadline is not None
            recorded = delivered.review_deadline - delivered.delivery.delivered_at
            assert abs(recorded - window) < timedelta(seconds=1), recorded

            released = await until(
                job.status,
                lambda v: v.escrow is not m.Escrow.HELD,
                within=window.total_seconds() + 120,
                what="the release",
            )
            seen = datetime.now(UTC)
        late = (seen - delivered.review_deadline).total_seconds()
        print(
            f"\n[acceptance 5] review_window {window.total_seconds():.0f}s: released "
            f"{late:.1f}s after it closed (delivered {delivered.delivery.delivered_at.isoformat()})"
        )
        assert released.escrow is m.Escrow.RELEASED, released
        assert released.state is m.JobState.COMPLETED
        assert seen >= delivered.review_deadline
        # "At expiry": within a few of the gate's 2 s sweeps and this test's 1 s polls.
        assert late <= 10, late
        assert job.amount_minor == 20
        paid = await k.balance()
        assert (paid.available_minor, paid.held_minor) == (10_000 - 20, 0)
        earned = await sk.balance()
        assert earned.available_minor == 10_000 + 20 - 1  # 5% to the platform
    finally:
        await k.aclose()
        await sk.aclose()


# --------------------------------------------------------------------------- acceptance 11


@pytest.mark.timeout(900)
async def test_ac11_recon_refunds_the_second_offering_when_the_first_fails(
    stack: Stack, tmp_path: Path
) -> None:
    """``examples/recon``, unchanged, composes ``acme/probe`` and ``acme/enrich`` (the two
    example sellers, unchanged, served by ``acme``) in one workflow in the buyer's namespace.
    Both jobs reach a site that stalls; the probe's seller then stops the probe's workflow
    (``temporal workflow terminate``, as a seller can). Recon sees the probe fail and cancels
    the enrichment while it still runs: the gate terminates the enrichment's run and both
    holds are refunded."""
    from kontra.hire import worker

    sites_mod: Any = _sites_module()
    (tmp_path / "sites").mkdir()
    sites = sites_mod.Sites.start(tmp_path / "sites")
    recon_dir = str(EXAMPLES / "recon")
    if recon_dir not in sys.path:
        sys.path.insert(0, recon_dir)  # the workflow's module, importable by name (sandbox)
    from recon import Recon  # pyright: ignore[reportMissingImports]

    probe_key, enrich_key = "acme/probe@1.0.0", "acme/enrich@1.2.0"
    acme = await stack.login("acme")
    buyer = await stack.party("ac11-buyer")
    env = {
        "SSL_CERT_FILE": str(sites.ca_file),  # the sites' CA
        "ENRICH_ALLOWED_PRIVATE_NETWORKS": "127.0.0.1/32",  # the sites are on loopback
        "PROBE_ALLOWED_PRIVATE_NETWORKS": "127.0.0.1/32",
    }
    probe = copy_project(EXAMPLES / "legacy-actor", tmp_path / "probe")
    enrich = copy_project(EXAMPLES / "enrich", tmp_path / "enrich")
    k = buyer.kontra()
    try:
        async with (
            serving(probe, acme, [probe_key], log=tmp_path / "probe.log", env=env),
            serving(enrich, acme, [enrich_key], log=tmp_path / "enrich.log", env=env),
        ):
            task_queue = f"recon-{uuid.uuid4().hex[:8]}"
            w = await worker(task_queue=task_queue, workflows=[Recon], kontra=k)
            async with w:
                before = sites.stalled
                handle = await w.client.start_workflow(
                    Recon.run,
                    sites.host("stall"),
                    id=f"recon-{uuid.uuid4().hex[:8]}",
                    task_queue=task_queue,
                )
                working = await until(
                    k.jobs,
                    lambda vs: len(vs) == 2 and all(v.state is m.JobState.WORKING for v in vs),
                    within=180,
                    what="both jobs working",
                )

                async def stalled() -> int:
                    return sites.stalled

                await until(stalled, lambda n: n >= before + 2, within=60, interval=0.5)
                jobs = {v.offering: v for v in working}
                async with acme.temporal() as seller:
                    await seller.get_workflow_handle(
                        job_workflow_id(jobs[probe_key].job_id)
                    ).terminate(reason="the probe's seller stopped the job")

                    with pytest.raises(WorkflowFailureError) as failed:
                        await handle.result()
                    cause = failed.value.cause
                    assert isinstance(cause, ApplicationError), cause
                    assert cause.type == "kontra.JobFailed"

                    closed = await until(
                        k.jobs,
                        lambda vs: all(v.escrow is m.Escrow.REFUNDED for v in vs),
                        within=180,
                        what="both refunds",
                    )
                    views = {v.offering: v for v in closed}
                    enrich_run = seller.get_workflow_handle(
                        job_workflow_id(views[enrich_key].job_id)
                    )
                    enrich_status = (await enrich_run.describe()).status
            sites.release()  # nothing may wait on the stalled site while the sellers stop
            print(
                f"\n[acceptance 11] probe {views[probe_key].state.value} "
                f"({views[probe_key].reason}), enrich {views[enrich_key].state.value} "
                f"({views[enrich_key].reason}); the enrichment's run {enrich_status.name}"
            )
            assert (views[probe_key].state, views[enrich_key].state) == (
                m.JobState.FAILED,
                m.JobState.CANCELED,  # cancelled by Recon while it ran
            )
            assert (views[probe_key].reason, views[enrich_key].reason) == (
                "seller workflow terminated",
                "canceled by buyer",
            )
            assert enrich_status is WorkflowExecutionStatus.TERMINATED
            balance = await k.balance()
            assert (balance.available, balance.held_minor) == ("100.00", 0)
    finally:
        sites.release()
        await k.aclose()
        sites.stop()
