"""Helpers for the ``kontra.hire`` tests: a marketplace with the test seller serving, fresh
funded buyers, waiting on the gate, and a second ledger configuration (acceptance 9)."""

from __future__ import annotations

import asyncio
import contextlib
import os
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass, replace
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, TypeVar

from temporalio.client import WorkflowExecutionStatus

from kontra.contract.names import job_workflow_id
from kontra.gate_client import models as m
from kontra.hire import Kontra
from kontra.ledger import PLATFORM_PARTY, Handle, HandleState, Ledger, Money, Split
from kontra.serve import BoundOffering, Worker
from kontra.serve.progress import INTERVAL_ENV
from kontra.testing import Marketplace, TestParty, fake_ledger
from kontra.testing.devserver import DevServer

from .sellers import text_offering, text_worker

_T = TypeVar("_T")


def handle(prefix: str = "bob") -> str:
    """A party handle no other test uses (namespaces outlive a test on a shared server)."""
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def usd(text: str) -> int:
    return Money.from_decimal(Decimal(text), "USD").amount_minor


async def until(
    probe: Callable[[], Awaitable[_T]], ok: Callable[[_T], bool], within: float = 30.0
) -> _T:
    """Poll ``probe`` until ``ok`` holds for its answer (the gate observes on a sweep)."""
    loop = asyncio.get_running_loop()
    end = loop.time() + within
    while True:
        value = await probe()
        if ok(value):
            return value
        if loop.time() > end:
            raise AssertionError(f"never got there; last: {value!r}")
        await asyncio.sleep(0.1)


class PendingLedger:
    """A second ledger adapter for acceptance 9: a chain-like one. Every call answers
    ``pending`` (confirmation is asynchronous, §9.4 property 3) after an await, over the
    in-memory ledger's books. The gate, and so the SDK, must not care."""

    def __init__(self, inner: Ledger, latency: float = 0.002) -> None:
        self._inner = inner
        self._latency = latency

    async def _pending(self, call: Awaitable[Handle]) -> Handle:
        await asyncio.sleep(self._latency)
        h = await call
        return replace(h, state=HandleState.PENDING) if h.state is HandleState.CONFIRMED else h

    async def lock(
        self, job_id: str, buyer: str, seller: str, amount: Money, *, expires_at: datetime
    ) -> Handle:
        return await self._pending(
            self._inner.lock(job_id, buyer, seller, amount, expires_at=expires_at)
        )

    async def release(self, job_id: str) -> Handle:
        return await self._pending(self._inner.release(job_id))

    async def refund(self, job_id: str) -> Handle:
        return await self._pending(self._inner.refund(job_id))

    async def dispute(self, job_id: str) -> Handle:
        return await self._pending(self._inner.dispute(job_id))

    async def resolve(self, job_id: str, split: Split) -> Handle:
        return await self._pending(self._inner.resolve(job_id, split))

    async def deposit(self, party_id: str, amount: Money, *, ref: str) -> Handle:
        return await self._pending(self._inner.deposit(party_id, amount, ref=ref))

    async def withdraw(self, party_id: str, amount: Money, *, ref: str) -> Handle:
        return await self._pending(self._inner.withdraw(party_id, amount, ref=ref))

    async def balance(self, party_id: str, currency: str) -> Any:
        return await self._inner.balance(party_id, currency)

    async def get_hold(self, job_id: str) -> Any:
        hold = await self._inner.get_hold(job_id)
        if hold is None:
            return None
        handles = {
            op: replace(h, state=HandleState.PENDING) if h.state is HandleState.CONFIRMED else h
            for op, h in hold.handles.items()
        }
        return replace(hold, handles=handles)

    async def postings(self) -> Any:
        return await self._inner.postings()

    async def balances(self) -> Any:
        return await self._inner.balances()


LEDGERS: dict[str, Callable[[], Ledger | None]] = {
    # The gate's own default: the in-memory reference adapter on the gate's clock.
    "fake": lambda: None,
    "pending": lambda: PendingLedger(fake_ledger()),
}


@dataclass
class Buyer:
    party: TestParty
    k: Kontra

    async def balance(self) -> m.Balance:
        return await self.k.balance()


@dataclass
class World:
    mp: Marketplace
    seller: TestParty
    worker: Worker
    bound: dict[str, BoundOffering]
    root: Path

    def key(self, name: str = "main") -> str:
        return self.bound[name].key

    async def buyer(self, balance: str = "100.00", **options: Any) -> Buyer:
        party = await self.mp.party(handle(), balance=balance)
        options.setdefault("client", self.mp.client(party.namespace))
        return Buyer(party, party.kontra(**options))

    async def gate(self, fn: Callable[[Any], _T]) -> _T:
        """``fn(fake_gate)`` on the gate's own loop (its state lives there)."""
        gate = self.mp.gate

        async def call() -> _T:
            return fn(gate)

        return await gate.run(call)

    async def ledger_balance(self, party: str) -> int:
        gate = self.mp.gate

        async def call() -> int:
            return (await gate.ledger.balance(party, "USD")).available_minor

        return await gate.run(call)

    async def platform(self) -> int:
        return await self.ledger_balance(PLATFORM_PARTY)

    async def job(self, buyer: Buyer, job_id: str, ok: Callable[[m.JobView], bool]) -> m.JobView:
        return await until(lambda: buyer.k.gate.job(job_id), ok)

    async def forge(self, job_id: str, text: str, name: str = "main") -> None:
        """Play a seller that starts ``job-<id>`` itself, outside its Nexus handler (it owns
        its namespace; the job id is in its job list), and runs it to completion. The gate
        watches that workflow id (decisions I6), so this counts as the job's delivery."""
        handle = await self.mp.client(self.seller.namespace).start_workflow(
            "TextFlow",
            {"text": text, "mode": "ok", "beats": 0, "doc": None},
            id=job_workflow_id(job_id),
            task_queue=self.bound[name].offering_id.task_queue,
        )

        async def status() -> WorkflowExecutionStatus | None:
            return (await handle.describe()).status

        await until(status, lambda s: s is WorkflowExecutionStatus.COMPLETED)

    def forge_on_hire(self, k: Kontra, text: str) -> None:
        """Make ``k``'s next hires forge a delivery (:meth:`forge`) right after the gate
        locks the hold, before the buyer starts ``execute``."""
        real_hire = k.gate.hire

        async def hire_then_forge(
            quote_id: str, *, max_price_minor: int | None = None
        ) -> m.HireView:
            view = await real_hire(quote_id, max_price_minor=max_price_minor)
            await self.forge(view.job_id, text)
            return view

        k.gate.hire = hire_then_forge  # type: ignore[method-assign]


@contextlib.asynccontextmanager
async def open_world(
    dev_server: DevServer, root: Path, *, ledger: str = "fake", **gate_options: Any
) -> AsyncIterator[World]:
    """A marketplace with ``acme`` serving ``acme/text`` in three versions: ``main``,
    ``review`` (a 2 s review window) and ``limited`` (one job at a time). Heartbeats are
    forwarded one by one (``KONTRA_PROGRESS_INTERVAL=0``, acceptance 13)."""
    previous = os.environ.get(INTERVAL_ENV)
    os.environ[INTERVAL_ENV] = "0"
    made = LEDGERS[ledger]()
    if made is not None:
        gate_options["ledger"] = made
    try:
        async with Marketplace(
            dev_server=dev_server, progress_rate_per_s=10_000, **gate_options
        ) as mp:
            acme = await mp.party("acme")
            bound = {
                "main": text_offering(root),
                "review": text_offering(root, review_window="2s"),
                "limited": text_offering(root, max_concurrent_jobs=1),
            }
            worker = text_worker(*bound.values())
            async with mp.serve(worker, acme):
                yield World(mp, acme, worker, bound, root)
    finally:
        if previous is None:
            os.environ.pop(INTERVAL_ENV, None)
        else:
            os.environ[INTERVAL_ENV] = previous
