"""Helpers for the examples' tests: where the examples are, the documented example key, and
the example sellers run as ``python worker.py`` processes against a marketplace -- what a
seller does after ``kontra login`` (or ``kontra dev``)."""

from __future__ import annotations

import asyncio
import contextlib
import importlib.util
import os
import signal
import subprocess
import sys
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from types import ModuleType
from typing import Any, TypeVar

import temporalio.api.enums.v1 as enums
import temporalio.api.taskqueue.v1 as taskqueue
import temporalio.api.workflowservice.v1 as wsv1
from temporalio.client import Client

from kontra.contract import load_offering
from kontra.gate_client import models as m
from kontra.hire import Kontra
from kontra.testing import Marketplace, TestParty

from .sites import Sites

_T = TypeVar("_T")

REPO = Path(__file__).resolve().parents[2]
EXAMPLES = REPO / "examples"
#: The sellers, by directory. Their offering names start with ``acme/``. ``recon-harness``
#: sells a bound handler (an agent's one typed entry); every other one a workflow.
SELLERS = (
    "enrich",
    "legacy-actor",
    "hello",
    "chef-agents",
    "chef-claude",
    "sweep",
    "recon-harness",
)
#: The ones the package's marketplace serves for every test. The chefs run in their own
#: tests, each with what its worker dials: a local model server, a stand-in ``claude``.
SERVED = ("enrich", "legacy-actor", "hello", "sweep")
#: What a buyer is funded with, in cents.
START = 10_000
#: The example sellers fetch only public addresses unless told otherwise (their READMEs);
#: the test sites are on 127.0.0.1, so the tests allow that address, and no other.
ALLOW_LOCAL_SITES = {
    "ENRICH_ALLOWED_PRIVATE_NETWORKS": "127.0.0.1/32",
    "PROBE_ALLOWED_PRIVATE_NETWORKS": "127.0.0.1/32",
    "SWEEP_ALLOWED_PRIVATE_NETWORKS": "127.0.0.1/32",
}


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


class Seller:
    """One example's ``worker.py``, run as its own process with the seller's profile.

    Each example is a directory of top-level modules (``models``, ``workflows``...), and
    Temporal's sandbox re-imports a workflow's module by name, so two examples cannot share
    a process: each runs as a seller would run it, ``python worker.py``."""

    def __init__(self, name: str, env: Mapping[str, str], log: Path) -> None:
        self.name = name
        self.directory = EXAMPLES / name
        self.offering = load_offering(self.directory / "offering.yaml").offering_id
        self._env = dict(env)
        self.log = log
        self._proc: subprocess.Popen[bytes] | None = None

    def start(self) -> None:
        with self.log.open("wb") as out:
            self._proc = subprocess.Popen(
                [sys.executable, "worker.py"],
                cwd=self.directory,
                env=self._env,
                stdout=out,
                stderr=subprocess.STDOUT,
            )

    def tail(self, chars: int = 4000) -> str:
        try:
            return self.log.read_text(errors="replace")[-chars:]
        except OSError:
            return ""

    async def wait_ready(self, client: Client, within: float = 60.0) -> None:
        """Until the worker polls its task queue for workflows and for Nexus operations."""
        assert self._proc is not None
        loop = asyncio.get_running_loop()
        end = loop.time() + within
        kinds = (
            enums.TaskQueueType.TASK_QUEUE_TYPE_WORKFLOW,
            enums.TaskQueueType.TASK_QUEUE_TYPE_NEXUS,
        )
        while True:
            if self._proc.poll() is not None:
                raise RuntimeError(f"{self.name}'s worker exited:\n{self.tail()}")
            polled = True
            for kind in kinds:
                answer = await client.workflow_service.describe_task_queue(
                    wsv1.DescribeTaskQueueRequest(
                        namespace=client.namespace,
                        task_queue=taskqueue.TaskQueue(name=self.offering.task_queue),
                        task_queue_type=kind,
                    )
                )
                polled = polled and bool(answer.pollers)
            if polled:
                return
            if loop.time() > end:
                raise RuntimeError(f"{self.name}'s worker never polled:\n{self.tail()}")
            await asyncio.sleep(0.2)

    def kill(self) -> None:
        """SIGKILL: the worker crashes, telling nobody, its activities mid-flight."""
        proc, self._proc = self._proc, None
        if proc is not None and proc.poll() is None:
            proc.kill()
            proc.wait(10)

    async def stop(self) -> None:
        """SIGTERM (a graceful shutdown), a second one (stop now), then SIGKILL."""
        proc, self._proc = self._proc, None
        if proc is None or proc.poll() is not None:
            return
        for sig, grace in ((signal.SIGTERM, 15.0), (signal.SIGTERM, 5.0), (signal.SIGKILL, 10.0)):
            proc.send_signal(sig)
            try:
                await asyncio.to_thread(proc.wait, grace)
                return
            except subprocess.TimeoutExpired:
                continue


@dataclass
class Buyer:
    party: TestParty
    k: Kontra

    async def balance(self) -> m.Balance:
        return await self.k.balance()


@dataclass
class Market:
    """A marketplace with ``acme`` serving every example seller, and the sites they fetch."""

    mp: Marketplace
    seller: TestParty
    sites: Sites
    sellers: dict[str, Seller] = field(default_factory=dict[str, Seller])

    async def buyer(self, balance: str = "100.00") -> Buyer:
        """A fresh, funded buyer (handles never repeat: namespaces outlive a test)."""
        party = await self.mp.party(f"buyer-{uuid.uuid4().hex[:8]}", balance=balance)
        return Buyer(party, party.kontra(client=self.mp.client(party.namespace)))

    async def gate(self, fn: Callable[[Any], _T]) -> _T:
        """``fn(fake_gate)`` on the gate's own loop (its state lives there)."""
        gate = self.mp.gate

        async def call() -> _T:
            return fn(gate)

        return await gate.run(call)

    def seller_client(self) -> Client:
        """A raw client on the seller's namespace: the seller looking at its own jobs."""
        return self.mp.client(self.seller.namespace)

    def logs(self) -> str:
        return "\n".join(f"--- {s.name}\n{s.tail()}" for s in self.sellers.values())


def seller_env(home: Path, sites: Sites) -> dict[str, str]:
    """The seller processes' environment: the party's profile, every heartbeat forwarded
    one by one (acceptance 13 counts them) with the stream upload budget a loaded test
    machine needs (Marketplace's 8 s: past 2 s an item is a stub, a gap the counts see), the test sites' CA trusted for HTTPS, and their
    address (127.0.0.1) allowed as a destination. Nothing of OpenAI's: no seller here needs
    an ``OPENAI_API_KEY``; and no model endpoint (``LLM_*``), so a seller that would dial one
    answers with its scripted model unless the test names one."""
    env = {k: v for k, v in os.environ.items() if not k.startswith(("KONTRA_", "OPENAI_", "LLM_"))}
    env.update(
        KONTRA_HOME=str(home),
        KONTRA_PROGRESS_INTERVAL="0",
        KONTRA_STREAM_UPLOAD_BUDGET="8",
        SSL_CERT_FILE=str(sites.ca_file),
        PYTHONUNBUFFERED="1",
        **ALLOW_LOCAL_SITES,
    )
    return env


@contextlib.asynccontextmanager
async def serving(market: Market, name: str, log: Path, **env: str) -> AsyncIterator[Seller]:
    """``examples/<name>``'s ``python worker.py`` as ``acme`` for the block (its log's tail in
    the report if a test fails), stopped gracefully after it."""
    environ = seller_env(market.seller.home, market.sites)
    environ.update(env)
    seller = Seller(name, environ, log)
    market.sellers[name] = seller
    seller.start()
    try:
        await seller.wait_ready(market.seller_client())
        yield seller
    finally:
        await seller.stop()


def example_module(name: str, module: str) -> ModuleType:
    """``examples/<name>/<module>.py`` loaded under a name of its own (``<name>_<module>``,
    never ``models``: every example has one), for a test that reads its types as a buyer's
    own would. Only a module that imports nothing from beside it."""
    spec = importlib.util.spec_from_file_location(
        f"{name.replace('-', '_')}_{module}", EXAMPLES / name / f"{module}.py"
    )
    assert spec is not None
    assert spec.loader is not None
    loaded = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(loaded)
    return loaded
