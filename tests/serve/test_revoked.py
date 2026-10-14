"""A worker whose credentials are revoked while it serves says so.

Temporal refuses a revoked session's worker tokens within seconds (the gate's revocation
list), and temporalio stops the worker with a bare ``RuntimeError("Workflow worker failed")``
over a PermissionDenied from its core. Seen on staging (2026-10-14): ``kontra serve`` on an
API key, the key revoked, and the seller got that traceback and no reason. The worker now asks
the gate -- never the error's text -- and says what happened and what to do; any other
failure is raised as it came.

A dev server authorizes nothing, so the refusal itself is simulated: ``_serve`` fails the way
temporalio does, once the test has revoked (or not) the credentials at the fake gate."""

from __future__ import annotations

import asyncio
import traceback
from pathlib import Path

import pytest

from kontra.auth import Session
from kontra.serve import ServeError, Worker
from kontra.testing import Marketplace, TestParty
from kontra.testing.devserver import DevServer

from . import seller
from .flows import shout
from .support import write_offering

pytestmark = [pytest.mark.temporal]


class _Refusal:
    """Serving fails as temporalio's does when Temporal refuses the worker, once the test
    calls :meth:`now` (prepare() runs for real before)."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self.serving, self.refuse = asyncio.Event(), asyncio.Event()

        async def serve(worker: Worker) -> None:
            self.serving.set()
            await self.refuse.wait()
            raise RuntimeError("Workflow worker failed")

        monkeypatch.setattr(Worker, "_serve", serve)

    async def serving_now(self, run: asyncio.Task[None]) -> None:
        waiting = asyncio.create_task(self.serving.wait())
        await asyncio.wait([run, waiting], timeout=60, return_when=asyncio.FIRST_COMPLETED)
        waiting.cancel()
        if not self.serving.is_set():
            raise AssertionError(
                f"the worker never served: {run.exception() if run.done() else 'timeout'}"
            )

    def now(self) -> None:
        self.refuse.set()


async def _published(acme: TestParty, tmp_path: Path) -> Worker:
    bound = seller.bind(write_offering(tmp_path))
    await acme.gate.publish(bound.document(acme.public_key))  # a key never publishes
    return Worker(offerings=[bound], activities=[shout])


async def test_a_worker_whose_api_key_is_revoked_says_so(
    dev_server: DevServer, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with Marketplace(dev_server=dev_server) as mp:
        acme = await mp.party("acme")
        worker = await _published(acme, tmp_path)
        made = await acme.gate.create_api_key("worker")
        refusal = _Refusal(monkeypatch)
        async with await Session.from_api_key(mp.base_url, made.key, home=acme.home) as key:
            run = asyncio.create_task(worker.run_async(session=key, handle_signals=False))
            await refusal.serving_now(run)
            await acme.gate.revoke_api_key(made.id)
            refusal.now()
            with pytest.raises(ServeError) as stopped:
                await run
    text = str(stopped.value)
    assert text.startswith("the worker stopped, refused by Temporal: "), text
    assert "API key" in text
    assert "kontra keys create" in text
    assert isinstance(stopped.value.__cause__, RuntimeError)  # temporalio's, kept as the cause
    assert made.key not in "".join(traceback.format_exception(stopped.value))


async def test_a_worker_whose_login_signed_out_says_so(
    dev_server: DevServer, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with Marketplace(dev_server=dev_server) as mp:
        acme = await mp.party("acme")
        worker = await _published(acme, tmp_path)
        refusal = _Refusal(monkeypatch)
        run = asyncio.create_task(worker.run_async(session=acme.session, handle_signals=False))
        await refusal.serving_now(run)
        await acme.gate.revoke()  # the login signs out (POST /v1/auth/revoke)
        refusal.now()
        with pytest.raises(ServeError) as stopped:
            await run
    assert "kontra login" in str(stopped.value)


async def test_any_other_failure_is_raised_as_it_came(
    dev_server: DevServer, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with Marketplace(dev_server=dev_server) as mp:
        acme = await mp.party("acme")
        worker = await _published(acme, tmp_path)
        made = await acme.gate.create_api_key("worker")
        _Refusal(monkeypatch).now()  # the key stays live: Temporal stopped it for another reason
        async with await Session.from_api_key(mp.base_url, made.key, home=acme.home) as key:
            with pytest.raises(RuntimeError, match=r"^Workflow worker failed$") as failed:
                await worker.run_async(session=key, handle_signals=False)
    assert not isinstance(failed.value, ServeError)
