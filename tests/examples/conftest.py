"""Fixtures for the examples' end-to-end tests: one Temporal dev server per test process
(shared with the other suites), and for this package one marketplace in which ``acme``
serves both example sellers -- ``examples/enrich`` and ``examples/legacy-actor``, each its
own ``python worker.py`` process -- next to local web sites for them to fetch.

Without ``KONTRA_TEMPORAL_CLI`` the dev server is CLI 1.9.1, downloaded once into the temp
directory; with neither the binary nor the network, these tests are skipped."""

from __future__ import annotations

import asyncio
import os
import sys
from collections.abc import AsyncIterator, Iterator

import pytest
import pytest_asyncio

from kontra.testing import Marketplace
from kontra.testing.devserver import (
    CLI_ENV,
    DevServer,
    acquire_shared_dev_server,
    release_shared_dev_server,
)

from .sites import Sites
from .support import ALLOW_LOCAL_SITES, EXAMPLES, SELLERS, Market, Seller, seller_env

# The recon example is a directory of scripts, as a buyer would write it; its workflow
# module must be importable by name for Temporal's sandbox.
RECON = EXAMPLES / "recon"
if str(RECON) not in sys.path:
    sys.path.insert(0, str(RECON))


@pytest.fixture(scope="session")
def dev_server() -> Iterator[DevServer]:
    try:
        server = acquire_shared_dev_server()
    except Exception as e:
        if os.environ.get(CLI_ENV):
            raise  # an explicit binary must work
        pytest.skip(f"no Temporal CLI 1.9.1 dev server: {e}")
    try:
        yield server
    finally:
        release_shared_dev_server()


@pytest.fixture(scope="package", autouse=True)
def allow_local_sites() -> Iterator[None]:
    """The examples' activities run in subprocesses of this one (and their sellers, which
    get it from :func:`seller_env` too) may reach the test sites on 127.0.0.1: the
    examples refuse non-public addresses unless a setting allows them."""
    with pytest.MonkeyPatch.context() as env:
        for name, value in ALLOW_LOCAL_SITES.items():
            env.setenv(name, value)
        yield


@pytest.fixture(scope="package")
def sites(tmp_path_factory: pytest.TempPathFactory) -> Iterator[Sites]:
    running = Sites.start(tmp_path_factory.mktemp("sites"))
    try:
        yield running
    finally:
        running.stop()


@pytest_asyncio.fixture(loop_scope="package", scope="package")
async def market(
    dev_server: DevServer, sites: Sites, tmp_path_factory: pytest.TempPathFactory
) -> AsyncIterator[Market]:
    # Every heartbeat reaches the buyer (the gate's per-job rate is the real gate's concern).
    async with Marketplace(dev_server=dev_server, progress_rate_per_s=10_000) as mp:
        acme = await mp.party("acme")
        logs = tmp_path_factory.mktemp("sellers")
        env = seller_env(acme.home, sites)
        world = Market(mp, acme, sites)
        for name in SELLERS:
            world.sellers[name] = Seller(name, env, logs / f"{name}.log")
        try:
            for seller in world.sellers.values():
                seller.start()
            await asyncio.gather(
                *(s.wait_ready(mp.client(acme.namespace)) for s in world.sellers.values())
            )
            yield world
        finally:
            sites.release()  # nothing may wait on a stalled site while the sellers stop
            await asyncio.gather(*(s.stop() for s in world.sellers.values()))


@pytest.hookimpl(wrapper=True)
def pytest_runtest_makereport(item: pytest.Item, call: pytest.CallInfo[None]) -> object:
    """A failed test that ran against the sellers shows the tail of their logs: they are
    separate processes, so their tracebacks are nowhere else in the report."""
    report = yield
    funcargs = getattr(item, "funcargs", {})
    market = funcargs.get("market") if isinstance(funcargs, dict) else None
    if getattr(report, "failed", False) and isinstance(market, Market):
        report.sections.append(("example sellers (log tails)", market.logs()))
    return report
