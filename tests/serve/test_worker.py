"""``kontra.serve.Worker``: argument checks, the kwargs passthrough (acceptance 10), the hash
lock's decisions, graceful shutdown on SIGTERM, and request URLs kept out of the log."""

from __future__ import annotations

import asyncio
import inspect
import logging
import os
import signal
import threading
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
import nexusrpc
import nexusrpc.handler
import pytest
import temporalio.worker
from nexusrpc.handler import StartOperationContext
from temporalio.plugin import SimplePlugin
from temporalio.worker import (
    Interceptor,
    PollerBehaviorAutoscaling,
    PollerBehaviorSimpleMaximum,
    SharedHeartbeatSender,
    SharedStateManager,
    UnsandboxedWorkflowRunner,
    WorkerDeploymentConfig,
    WorkerDeploymentVersion,
    WorkerTuner,
)
from temporalio.worker.workflow_sandbox import SandboxedWorkflowRunner

from kontra._parallel import _Batch
from kontra.codec.age import generate_identity
from kontra.gate_client import GateError
from kontra.gate_client import models as m
from kontra.profile import NotLoggedIn, Profile
from kontra.serve import KontraInterceptor, ServeError, Worker
from kontra.testing import Marketplace
from kontra.testing.devserver import DevServer
from kontra.types import Artifact

from . import seller
from .flows import SentinelFlow, count, shout
from .support import write_offering

HERE = Path(__file__).parent


@pytest.fixture
def bound(tmp_path: Path) -> Any:
    return seller.bind(write_offering(tmp_path))


# --------------------------------------------------------------------------- construction


def test_task_queue_and_client_are_not_configurable(bound: Any) -> None:
    with pytest.raises(TypeError, match="task_queue is not configurable"):
        Worker(offerings=[bound], task_queue="mine")  # type: ignore[call-arg]
    with pytest.raises(TypeError, match="client is not configurable"):
        Worker(offerings=[bound], client=object())  # type: ignore[call-arg]


def test_unknown_keywords_and_bad_offerings_are_refused(bound: Any) -> None:
    with pytest.raises(TypeError, match="unexpected keyword argument 'max_concurent_activities'"):
        Worker(offerings=[bound], max_concurent_activities=3)  # type: ignore[call-arg]
    with pytest.raises(ValueError, match="at least one offering"):
        Worker(offerings=[])
    with pytest.raises(ValueError, match="bound twice"):
        Worker(offerings=[bound, bound])
    with pytest.raises(TypeError, match=r"kontra\.serve\.offering"):
        Worker(offerings=["acme/upper@1.0.0"])  # type: ignore[list-item]


def test_construction_does_no_io(bound: Any) -> None:
    w = Worker(offerings=[bound], activities=[shout], max_concurrent_activities=4)
    assert w.offerings == (bound,)
    assert w.inner_workers == ()
    assert w.client is None
    assert w.options == {"activities": [shout], "max_concurrent_activities": 4}


# --------------------------------------------------------------------------- acceptance 10


class _SentinelInterceptor(Interceptor):
    pass


@nexusrpc.handler.service_handler(name="sentinel.service")
class _SentinelService:
    @nexusrpc.handler.sync_operation
    async def ping(self, ctx: StartOperationContext, input: str) -> str:
        return input


class _StateManager(SharedStateManager):
    def new_event(self) -> threading.Event:
        return threading.Event()

    async def register_heartbeater(
        self, task_token: bytes, heartbeat: Callable[..., None]
    ) -> SharedHeartbeatSender:
        raise NotImplementedError

    async def unregister_heartbeater(self, task_token: bytes) -> None:
        raise NotImplementedError


async def _on_fatal(exc: BaseException) -> None:
    pass


def _patch_callback(input: Any) -> bool:
    return True


#: A distinct, valid value per temporalio.worker.Worker keyword. A keyword temporalio adds
#: fails the coverage test below until it gets a sentinel here.
SENTINELS: dict[str, Callable[[], Any]] = {
    "activities": lambda: [shout, count],
    "nexus_service_handlers": lambda: [_SentinelService()],
    "workflows": lambda: [SentinelFlow],
    "activity_executor": lambda: ThreadPoolExecutor(max_workers=11),
    "workflow_task_executor": lambda: ThreadPoolExecutor(max_workers=3),
    "nexus_task_executor": lambda: ThreadPoolExecutor(max_workers=17),
    "workflow_runner": lambda: SandboxedWorkflowRunner(),
    "unsandboxed_workflow_runner": lambda: UnsandboxedWorkflowRunner(),
    "plugins": lambda: [SimplePlugin("kontra-sentinel")],
    "interceptors": lambda: [_SentinelInterceptor()],
    "build_id": lambda: "sentinel-build",
    "identity": lambda: "sentinel-identity",
    "max_cached_workflows": lambda: 123,
    "max_concurrent_workflow_tasks": lambda: 7,
    "max_concurrent_activities": lambda: 11,
    "max_concurrent_local_activities": lambda: 13,
    "max_concurrent_nexus_tasks": lambda: 17,
    "tuner": lambda: WorkerTuner.create_fixed(
        workflow_slots=5, activity_slots=6, local_activity_slots=7, nexus_slots=8
    ),
    "max_concurrent_workflow_task_polls": lambda: 3,
    "nonsticky_to_sticky_poll_ratio": lambda: 0.37,
    "max_concurrent_activity_task_polls": lambda: 4,
    "no_remote_activities": lambda: True,
    "sticky_queue_schedule_to_start_timeout": lambda: timedelta(seconds=11),
    "max_heartbeat_throttle_interval": lambda: timedelta(seconds=41),
    "default_heartbeat_throttle_interval": lambda: timedelta(seconds=19),
    "max_activities_per_second": lambda: 12.5,
    "max_task_queue_activities_per_second": lambda: 7.25,
    "max_eager_activity_reservations_per_workflow_task": lambda: 2,
    "graceful_shutdown_timeout": lambda: timedelta(seconds=3),
    "workflow_failure_exception_types": lambda: [KeyError],
    "shared_state_manager": lambda: _StateManager(),
    "debug_mode": lambda: True,
    "disable_eager_activity_execution": lambda: True,
    "on_fatal_error": lambda: _on_fatal,
    "use_worker_versioning": lambda: True,
    "disable_safe_workflow_eviction": lambda: True,
    "deployment_config": lambda: WorkerDeploymentConfig(
        version=WorkerDeploymentVersion("sentinel-deployment", "sentinel-build-2"),
        use_worker_versioning=False,
    ),
    "patch_activation_callback": lambda: _patch_callback,
    "workflow_task_poller_behavior": lambda: PollerBehaviorSimpleMaximum(maximum=6),
    "activity_task_poller_behavior": lambda: PollerBehaviorAutoscaling(
        minimum=1, maximum=9, initial=2
    ),
    "nexus_task_poller_behavior": lambda: PollerBehaviorSimpleMaximum(maximum=2),
    "disable_payload_error_limit": lambda: True,
    "max_workflow_task_external_storage_concurrency": lambda: 2,
}
#: temporalio refuses these together with others (tuner with max_concurrent_*, a
#: deployment with build_id/use_worker_versioning), so they get a worker of their own.
_EXCLUSIVE = frozenset({"tuner", "deployment_config"})
_MERGED = frozenset({"workflows", "nexus_service_handlers", "interceptors"})


def _temporal_kwargs() -> set[str]:
    params = inspect.signature(temporalio.worker.Worker.__init__).parameters
    return {n for n, p in params.items() if n != "self" and p.kind is p.KEYWORD_ONLY} - {
        "task_queue"
    }


def test_every_temporalio_keyword_has_a_sentinel() -> None:
    missing = _temporal_kwargs() - set(SENTINELS)
    assert not missing, f"temporalio.worker.Worker gained {sorted(missing)}: add sentinels"
    assert set(SENTINELS) <= _temporal_kwargs()


@pytest.mark.temporal
async def test_every_keyword_reaches_the_inner_worker_unchanged(
    dev_server: DevServer, tmp_path: Path
) -> None:
    """Acceptance 10: a sentinel per ``temporalio.worker.Worker`` keyword (read from its
    signature) is observable unchanged on the inner worker's ``config()``; the three merged
    ones carry Kontra's part first, then the given values, untouched."""
    names = sorted(_temporal_kwargs())
    groups = [
        [n for n in names if n not in _EXCLUSIVE],
        sorted(_EXCLUSIVE | _MERGED | {"activities", "activity_executor"}),
    ]
    assert set(groups[0]) | set(groups[1]) == set(names)
    async with Marketplace(dev_server=dev_server) as mp:
        acme = await mp.party("acme")
        bound = seller.bind(write_offering(tmp_path))
        for group in groups:
            given = {n: SENTINELS[n]() for n in group}
            worker = Worker(offerings=[bound], **given)
            await worker.prepare(session=acme.session)
            try:
                [inner] = worker.inner_workers
                config = inner.config()
                assert inner.task_queue == bound.key
                assert config["client"] is worker.client
                for name, value in given.items():
                    seen = config[name]  # type: ignore[literal-required]
                    if name == "workflows":
                        assert seen[0] is bound.workflow
                        assert seen[-1] is _Batch
                        assert all(a is b for a, b in zip(seen[1:-1], value, strict=True)), name
                    elif name == "nexus_service_handlers":
                        assert type(seen[0]).__name__ == "KontraOffering"
                        assert all(a is b for a, b in zip(seen[1:], value, strict=True)), name
                    elif name == "interceptors":
                        assert isinstance(seen[0], KontraInterceptor)
                        assert all(a is b for a, b in zip(seen[1:], value, strict=True)), name
                    elif isinstance(value, list):
                        assert all(a is b for a, b in zip(seen, value, strict=True)), name
                    else:
                        assert seen is value, name
            finally:
                await worker.aclose()
                for v in given.values():
                    if isinstance(v, ThreadPoolExecutor):
                        v.shutdown()
            assert worker.inner_workers == ()


@pytest.mark.temporal
async def test_workflows_run_sandboxed_with_kontra_passed_through_by_default(
    dev_server: DevServer, tmp_path: Path
) -> None:
    """Without a ``workflow_runner`` the inner worker gets Temporal's sandbox passing
    ``kontra`` through, and ``kontra.batch.v1`` is registered once even when given too."""
    async with Marketplace(dev_server=dev_server) as mp:
        acme = await mp.party("acme")
        bound = seller.bind(write_offering(tmp_path))
        worker = Worker(offerings=[bound], workflows=[_Batch])
        await worker.prepare(session=acme.session)
        try:
            [inner] = worker.inner_workers
            config = inner.config()
            runner = config["workflow_runner"]
            assert isinstance(runner, SandboxedWorkflowRunner)
            assert "kontra" in runner.restrictions.passthrough_modules
            assert config["workflows"] == [bound.workflow, _Batch]
        finally:
            await worker.aclose()


# --------------------------------------------------------------------------- the hash lock


class _CatalogGate:
    """The gate calls the hash lock makes, scripted."""

    def __init__(
        self,
        entry: m.CatalogEntry | None,
        open_jobs: list[m.JobView] | None = None,
        *,
        hired_hash: str = "",
    ):
        self.entry = entry
        self.open_jobs = open_jobs or []
        self.hired_hash = hired_hash
        self.published: list[dict[str, Any]] = []

    async def get_offering(self, ref: str) -> m.CatalogEntry:
        if self.entry is None:
            raise GateError(404, "offering_not_found", "no such offering")
        return self.entry

    async def publish(self, document: dict[str, Any]) -> m.CatalogEntry:
        self.published.append(document)
        return _entry(document)

    async def jobs(
        self, *, as_: str, state: str | None, cursor: str | None, limit: int
    ) -> m.JobList:
        return m.JobList(items=[j for j in self.open_jobs if j.state.value == state])

    async def seller_job(self, job_id: str) -> m.SellerJobView:
        job = next(j for j in self.open_jobs if j.job_id == job_id)
        return m.SellerJobView(
            job_id=job_id,
            offering=job.offering,
            buyer_public_key=generate_identity()[1],
            contract_hash=self.hired_hash,
            max_duration_seconds=600,
        )


def _entry(
    document: dict[str, Any], *, contract_hash: str | None = None, retired: bool = False
) -> m.CatalogEntry:
    facts = document["x-kontra"]
    return m.CatalogEntry(
        offering=f"{facts['name']}@{facts['version']}",
        party="acme",
        name=facts["name"],
        version=facts["version"],
        description=facts["description"],
        contract_hash=contract_hash or facts["contract_hash"],
        document=document,
        card={},
        endpoint="k" + "0" * 32,
        service="acme.upper",
        task_queue=f"{facts['name']}@{facts['version']}",
        recipients=[generate_identity()[1]],
        published_at=datetime.now(UTC),
        retired=retired,
    )


def _job(offering: str, state: m.JobState = m.JobState.WORKING) -> m.JobView:
    now = datetime.now(UTC)
    return m.JobView(
        job_id="a" * 32,
        offering=offering,
        buyer="bob",
        seller="acme",
        state=state,
        escrow=m.Escrow.HELD,
        amount_minor=1,
        currency="USD",
        price="0.01",
        created_at=now,
        deadline=now,
        operation_id="op",
        workflow_id="job-" + "a" * 32,
    )


def _profile(*, dev: bool) -> Profile:
    return Profile(
        gate_url="http://localhost:1",
        temporal_address="localhost:2",
        party="acme",
        namespace="acme",
        refresh_token="r",
        auth_time=datetime.now(UTC),
        dev=dev,
    )


async def test_the_hash_lock_refuses_an_unpublished_version_outside_dev(bound: Any) -> None:
    gate = _CatalogGate(None)
    with pytest.raises(ServeError, match=r"acme/upper@\S+ is not published: run `kontra publish`"):
        await Worker(offerings=[bound])._hash_lock(gate, _profile(dev=False))  # type: ignore[arg-type]
    assert gate.published == []


async def test_the_hash_lock_publishes_on_a_dev_profile(bound: Any) -> None:
    gate = _CatalogGate(None)
    hashes = await Worker(offerings=[bound])._hash_lock(gate, _profile(dev=True))  # type: ignore[arg-type]
    assert gate.published == [bound.document()]
    assert hashes == {bound.key: bound.contract_hash()}


async def test_the_hash_lock_names_both_hashes(bound: Any) -> None:
    theirs = "sha256:" + "1" * 64
    gate = _CatalogGate(_entry(bound.document(), contract_hash=theirs))
    for dev in (False, True):  # a dev profile does not paper over drift either
        with pytest.raises(ServeError) as refused:
            await Worker(offerings=[bound])._hash_lock(gate, _profile(dev=dev))  # type: ignore[arg-type]
        assert bound.contract_hash() in str(refused.value)
        assert theirs in str(refused.value)
    assert gate.published == []


async def test_a_retired_version_with_open_jobs_is_served_with_a_warning(
    bound: Any, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.WARNING, "kontra.serve")
    gate = _CatalogGate(
        None,
        [_job(bound.key), _job("acme/other@1.0.0")],
        hired_hash=bound.contract_hash(),
    )
    hashes = await Worker(offerings=[bound])._hash_lock(gate, _profile(dev=False))  # type: ignore[arg-type]
    assert hashes == {bound.key: bound.contract_hash()}
    assert "is retired but still has 1 open job(s)" in caplog.text
    caplog.clear()
    gate = _CatalogGate(_entry(bound.document(), retired=True), [_job(bound.key)])
    await Worker(offerings=[bound])._hash_lock(gate, _profile(dev=False))  # type: ignore[arg-type]
    assert "still has 1 open job(s); serving them" in caplog.text


async def test_a_retired_version_with_open_jobs_is_still_hash_locked(bound: Any) -> None:
    """A retired version is gone from the catalog, but its open jobs were bought for a
    contract: code that drifted from it refuses to start, naming both hashes (§11.2)."""
    theirs = "sha256:" + "2" * 64
    gate = _CatalogGate(None, [_job(bound.key)], hired_hash=theirs)
    with pytest.raises(ServeError) as refused:
        await Worker(offerings=[bound])._hash_lock(gate, _profile(dev=True))  # type: ignore[arg-type]
    assert bound.contract_hash() in str(refused.value)
    assert f"hired under {theirs}" in str(refused.value)
    assert gate.published == []  # a dev profile does not republish a retired version


# --------------------------------------------------------------------------- signals


@pytest.mark.temporal
async def test_sigterm_shuts_the_worker_down_gracefully(
    dev_server: DevServer, tmp_path: Path
) -> None:
    if threading.current_thread() is not threading.main_thread():
        pytest.skip("signal handlers need the main thread")
    async with Marketplace(dev_server=dev_server) as mp:
        acme = await mp.party("acme")
        worker = Worker(offerings=[seller.bind(write_offering(tmp_path))], activities=[shout])
        before = signal.getsignal(signal.SIGTERM)
        run = asyncio.create_task(worker.run_async(session=acme.session))
        await asyncio.wait_for(worker.wait_ready(), 30)
        assert all(w.is_running for w in worker.inner_workers)
        assert signal.getsignal(signal.SIGTERM) is not before  # ours, or the kill would end pytest
        os.kill(os.getpid(), signal.SIGTERM)
        await asyncio.wait_for(run, 30)  # returns: a graceful stop is not an error
        assert worker.inner_workers == ()
        assert signal.getsignal(signal.SIGTERM) == before


@pytest.mark.temporal
async def test_one_worker_serves_several_offerings(dev_server: DevServer, tmp_path: Path) -> None:
    """One inner worker per offering, each on its own derived task queue, all published."""
    async with Marketplace(dev_server=dev_server) as mp:
        acme = await mp.party("acme")
        a, b = (seller.bind(write_offering(tmp_path / n)) for n in "ab")
        worker = Worker(offerings=[a, b], activities=[shout])
        async with mp.serve(worker, acme):
            assert [w.task_queue for w in worker.inner_workers] == [a.key, b.key]
            assert [s.key for s in worker.services] == [a.key, b.key]
            for bound in (a, b):
                entry = await acme.gate.get_offering(bound.key)
                assert entry.contract_hash == bound.contract_hash()
                assert entry.task_queue == bound.key


# --------------------------------------------------------------------------- logging


@pytest.mark.temporal
async def test_python_worker_py_keeps_signed_and_buyer_urls_out_of_the_log(
    dev_server: DevServer,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``python worker.py`` is ``Worker.run()`` with nothing around it. httpx logs the full
    URL of every request at INFO: an artifact's presigned upload URL (its signature
    included: whoever reads the log could write that object) and every URL an activity
    fetches for a buyer (the buyer's input, which travels encrypted). The worker turns that
    logger down, as ``kontra serve`` does, unless the seller chose its level."""
    monkeypatch.setattr(logging.getLogger("httpx"), "level", logging.NOTSET)
    caplog.set_level(logging.INFO)
    async with Marketplace(dev_server=dev_server) as mp:
        acme = await mp.party("acme")
        worker = Worker(offerings=[seller.bind(write_offering(tmp_path))], activities=[shout])
        async with mp.serve(worker, acme):
            data = tmp_path / "records.jsonl"
            data.write_text('{"url": "https://buyer.example/private"}\n')
            artifact = await Artifact.from_path(data)  # as an activity would: a presigned PUT
            async with httpx.AsyncClient() as http:  # a buyer's URL, fetched by an activity
                await http.get(f"{mp.gate.base_url}/buyer-supplied/secret-path?token=abc")
    assert artifact.size > 0
    logged = "\n".join(r.getMessage() for r in caplog.records)
    assert "serving acme/upper" in logged  # the worker's own INFO lines are there
    for leak in ("sig=", "/_blob/", "buyer-supplied", "token=abc"):
        assert leak not in logged


async def test_a_level_the_seller_chose_for_httpx_is_kept(
    bound: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    httpx_log = logging.getLogger("httpx")
    monkeypatch.setattr(httpx_log, "level", logging.DEBUG)
    with pytest.raises(NotLoggedIn):  # no profile here: the start fails after the logging
        await Worker(offerings=[bound]).run_async(home=tmp_path, handle_signals=False)
    assert httpx_log.level == logging.DEBUG
    monkeypatch.setattr(httpx_log, "level", logging.NOTSET)
    with pytest.raises(NotLoggedIn):
        await Worker(offerings=[bound]).run_async(home=tmp_path, handle_signals=False)
    assert httpx_log.level == logging.WARNING
