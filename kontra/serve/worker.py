"""``kontra.serve.Worker``: ``temporalio.worker.Worker`` plus exactly four things (PRD §6.5).

1. A generated Nexus service per bound offering (:mod:`kontra.serve.handler`).
2. The hold-token check before ``execute`` starts anything (:mod:`kontra.serve.hold`).
3. The encryption codec (:class:`kontra.codec.KontraCodec`, seller role).
4. The task queue, derived from ``name@version`` (:attr:`OfferingId.task_queue`); passing
   ``task_queue`` is a ``TypeError``.

Everything else is Temporal's: every ``temporalio.worker.Worker`` keyword is forwarded
verbatim to each inner worker (one per offering task queue), except the three that must be
merged -- ``workflows`` (the bound workflow, then yours), ``nexus_service_handlers`` (the
generated one, then yours) and ``interceptors`` (Kontra's first, then yours). The accepted
keywords are read from temporalio's own signature, so a new temporalio keyword passes
through without a Kontra release.

Starting (:meth:`Worker.run` / :meth:`Worker.run_async`), in order: load the session
(``KONTRA_API_KEY``, else the profile), install the process's Kontra runtime, connect to Temporal as the ``worker`` role
(the JWT is refreshed in the background, §10.3) with the pydantic converter and the codec,
fetch the gate's JWKS, then the **hash lock** (§11.2): each offering's contract hash must
equal the catalog's, or the worker refuses to start ("republish or bump the version"). An
unpublished version is refused too -- except on a ``kontra dev`` profile, where the worker
publishes it (the gate creates the Nexus endpoint; the worker never needs its name).
"""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import inspect
import logging
import signal
import sys
import threading
from collections.abc import Generator, Sequence
from pathlib import Path
from typing import Any, Unpack

import temporalio.worker
from temporalio.client import Client, HeaderCodecBehavior
from temporalio.contrib.pydantic import PydanticPayloadConverter
from temporalio.converter import DataConverter
from temporalio.worker import WorkerConfig

from kontra import _runtime
from kontra.auth import Session
from kontra.codec import GateBlobStore, KontraCodec, data_converter
from kontra.codec.age import public_key_of
from kontra.gate_client import GateClient, GateError
from kontra.gate_client import models as m
from kontra.profile import KEY_FILE_ENV, NotLoggedIn, Profile, kontra_home
from kontra.serve.handler import OfferingService, service_handler
from kontra.serve.hold import HoldVerifier
from kontra.serve.interceptors import KontraInterceptor
from kontra.serve.jobs import JobDirectory, SellerKeyCache
from kontra.serve.offering import BoundOffering
from kontra.serve.progress import ProgressForwarder

__all__ = ["ServeError", "Worker", "seller_data_converter"]

log = logging.getLogger("kontra.serve")

#: Every keyword ``temporalio.worker.Worker`` takes, read from its signature.
TEMPORAL_KWARGS: frozenset[str] = frozenset(
    name
    for name, p in inspect.signature(temporalio.worker.Worker.__init__).parameters.items()
    if name != "self" and p.kind is not inspect.Parameter.VAR_KEYWORD
)
_OPEN_STATES = (m.JobState.SUBMITTED, m.JobState.WORKING)
_JOB_PAGES = 20
#: Loggers that write the full URL of every request at INFO: the bucket's presigned URLs
#: (their signatures included) and every URL a seller's activities fetch for a buyer.
_URL_LOGGERS = ("httpx",)
#: The longest a stopping worker waits for temporalio's own threads to leave Python code.
_SETTLE_TIMEOUT = 5.0
#: The longest a failed worker waits for the gate to say whether it still accepts its session.
_REFUSAL_TIMEOUT = 10.0


class ServeError(RuntimeError):
    """The worker refuses to start -- a contract that drifted from the catalog, an
    unpublished version, a key that does not match the profile -- or stopped because its
    credentials were revoked."""


async def _refused(session: Session | None) -> str | None:
    """After an inner worker failed: why, when it is that the gate no longer accepts the
    worker's session -- its API key revoked, its login signed out or disallowed. Temporal
    refuses that session's tokens within seconds of the gate (the revocation list), and
    temporalio reports it as a bare ``RuntimeError("Workflow worker failed")`` over a
    PermissionDenied deep in its core: a seller who revoked a key on purpose, or found one
    revoked, deserves the reason and what to do. Asked of the gate, never read from the
    error's text. None when the gate still accepts the session, or cannot be asked in time:
    the failure is another one, and is raised as it came."""
    if session is None:
        return None
    try:
        async with asyncio.timeout(_REFUSAL_TIMEOUT):
            await session.gate.me()
    except NotLoggedIn as e:
        return f"the worker stopped, refused by Temporal: {e}"
    except Exception:  # unreachable, slow, or failing otherwise: not this function's call
        return None
    return None


def seller_data_converter(codec: KontraCodec) -> DataConverter:
    """:func:`kontra.codec.data_converter` (failures encrypted too) with the pydantic payload
    converter, so models in workflow and activity signatures round-trip."""
    return dataclasses.replace(
        data_converter(codec), payload_converter_class=PydanticPayloadConverter
    )


def _current_runtime() -> _runtime.Runtime | None:
    try:
        return _runtime.current()
    except _runtime.NoRuntime:
        return None


def _restore_runtime(ours: _runtime.Runtime, previous: _runtime.Runtime | None) -> None:
    """Uninstall ours, putting back what was there (a worker that fails to start must not
    take away the runtime of one already serving in this process)."""
    if _current_runtime() is ours:
        _runtime.uninstall(ours)
        if previous is not None:
            _runtime.install(previous)


def _quiet_url_logs() -> None:
    """Turn the loggers that print request URLs at INFO (:data:`_URL_LOGGERS`) down to
    WARNING, unless the seller chose their level. A worker's log must not carry a presigned
    upload URL (whoever reads the log could write that object until it expires) or the URLs
    a buyer sent (they are the buyer's input, which travels encrypted); ``kontra serve``
    has always done this, and ``python worker.py`` must be as safe."""
    for name in _URL_LOGGERS:
        logger = logging.getLogger(name)
        if logger.level == logging.NOTSET:
            logger.setLevel(logging.WARNING)


def _core_threads_in_python() -> bool:
    """Whether a thread Python did not start -- one of temporalio's Rust core threads -- is
    executing Python code right now."""
    ours = {t.ident for t in threading.enumerate() if not isinstance(t, threading._DummyThread)}
    return any(ident not in ours for ident in sys._current_frames())


async def _settle_core_threads() -> None:
    """Return once no temporalio core thread is running Python code (or after
    :data:`_SETTLE_TIMEOUT`).

    temporalio's Rust core hands each result to the event loop from one of its own threads,
    holding the GIL to call ``loop.call_soon_threadsafe``. That call writes to the loop's
    socket, which lets go of the GIL: the loop can see the result -- here, the inner
    workers' final ``finalize_shutdown`` -- and the process run on to interpreter shutdown
    before that thread is scheduled again to take the GIL back. Python 3.12 ends a thread
    that asks for the GIL during finalization where it waits, and the unwinding runs the
    Rust destructors of what the call held (pyo3's task locals: the event loop and a
    ``contextvars.Context``), which free Python objects in the middle of the interpreter's
    last garbage collection: SIGSEGV (exit status 139) after a clean stop -- measured in
    one ``python worker.py`` in 15 to 80 on a busy machine, more often on a single CPU
    (``tests/serve/test_exit.py``). Waiting here, with the GIL free, lets the thread finish
    the call and drop its references under the GIL; after a graceful shutdown nothing of
    ours awaits the core any more, so no core thread enters Python again. Nothing is
    released early instead: the worker's client and runtime are gone with ``aclose``, and
    it is the core thread still running that must finish."""
    loop = asyncio.get_running_loop()
    end = loop.time() + _SETTLE_TIMEOUT
    # Polled: the state is another thread's, and nothing signals when it changes.
    while _core_threads_in_python() and loop.time() < end:  # noqa: ASYNC110
        await asyncio.sleep(0.001)


def _home_dir(profile: Profile, home: str | Path | None) -> Path:
    """Where the profile lives: ``jwks.json`` is cached next to it."""
    if profile.home is not None:
        return profile.home
    return Path(home).expanduser() if home is not None else kontra_home()


def _dedupe(items: Sequence[Any]) -> list[Any]:
    out: list[Any] = []
    for item in items:
        if not any(item is seen for seen in out):
            out.append(item)
    return out


class Worker:
    """``Worker(offerings=[offering(...)], activities=[...], **temporalio_worker_kwargs)``.

    Construction checks the arguments and does no I/O, so ``worker.py`` can be imported by
    ``kontra publish`` without connecting anywhere.
    """

    def __init__(self, offerings: Sequence[BoundOffering], **kwargs: Unpack[WorkerConfig]) -> None:
        for name in kwargs:
            if name == "task_queue":
                raise TypeError(
                    "task_queue is not configurable: each offering's task queue is derived "
                    "from its name@version (PRD §6.5)"
                )
            if name == "client":
                raise TypeError(
                    "client is not configurable: the worker connects with your profile "
                    "(pass client= to run_async in tests)"
                )
            if name not in TEMPORAL_KWARGS:
                raise TypeError(
                    f"Worker() got an unexpected keyword argument {name!r}; it accepts "
                    "offerings= and the keywords of temporalio.worker.Worker"
                )
        bound = list(offerings)
        if not bound:
            raise ValueError("Worker needs at least one offering: offerings=[offering(...)]")
        keys: set[str] = set()
        for b in bound:
            if not isinstance(b, BoundOffering):  # pyright: ignore[reportUnnecessaryIsInstance]
                raise TypeError(f"offerings must come from kontra.serve.offering(...), got {b!r}")
            if b.key in keys:
                raise ValueError(f"{b.key} is bound twice")
            keys.add(b.key)
        self._offerings: tuple[BoundOffering, ...] = tuple(bound)
        self._kwargs: dict[str, Any] = dict(kwargs)
        self._inner: tuple[temporalio.worker.Worker, ...] = ()
        self._services: tuple[OfferingService, ...] = ()
        self._stack: contextlib.AsyncExitStack | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        # Events bind to a loop on first wait, not here; aclose() makes fresh ones so a
        # stopped Worker can run again (in another loop, too).
        self._stop = asyncio.Event()
        self._ready = asyncio.Event()
        self._client: Client | None = None
        self._session: Session | None = None
        self._running = False
        self.progress: ProgressForwarder | None = None
        self.jobs: JobDirectory | None = None
        self.verifier: HoldVerifier | None = None

    # ------------------------------------------------------------------ introspection

    @property
    def offerings(self) -> tuple[BoundOffering, ...]:
        return self._offerings

    @property
    def options(self) -> dict[str, Any]:
        """The temporalio keywords as given (a copy)."""
        return dict(self._kwargs)

    @property
    def inner_workers(self) -> tuple[temporalio.worker.Worker, ...]:
        """One ``temporalio.worker.Worker`` per offering, in order; empty until started."""
        return self._inner

    @property
    def services(self) -> tuple[OfferingService, ...]:
        """The generated Nexus services, in offering order; empty until started."""
        return self._services

    @property
    def client(self) -> Client | None:
        """The Temporal client the inner workers use (codec included); None until started."""
        return self._client

    # ------------------------------------------------------------------ starting

    async def prepare(
        self,
        *,
        session: Session | None = None,
        client: Client | None = None,
        gate: GateClient | None = None,
        home: str | Path | None = None,
    ) -> None:
        """Connect, verify and build the inner workers, without polling yet.

        ``session``: a logged-in :class:`kontra.auth.Session` (default: ``KONTRA_API_KEY``
        if set, else the profile in ``home`` / ``$KONTRA_HOME``; a key serves as well as a
        login, it only cannot publish). ``client``: a Temporal client to share the connection
        of (its namespace must be the profile's; the codec is added here). ``gate``: the gate
        client (default: the session's). The last three exist for tests.
        """
        if self._stack is not None:
            raise RuntimeError("this Worker is already started")
        stack = contextlib.AsyncExitStack()
        try:
            await self._prepare(stack, session=session, client=client, gate=gate, home=home)
        except BaseException:
            await stack.aclose()
            self._inner, self._services, self._client = (), (), None
            self._session = None
            raise
        self._stack = stack

    async def _prepare(
        self,
        stack: contextlib.AsyncExitStack,
        *,
        session: Session | None,
        client: Client | None,
        gate: GateClient | None,
        home: str | Path | None,
    ) -> None:
        if session is None:
            session = Session.from_env(home)  # KONTRA_API_KEY, else the profile
            stack.push_async_callback(session.aclose)
        profile = await session.resolve()
        self._session = session
        gate = gate or session.gate
        home_dir = _home_dir(profile, home)
        # This machine's key: $KONTRA_KEY_FILE when a fleet mounts one from a secret,
        # else <home>/key (decisions I7).
        identity = session.identity()
        public_key = public_key_of(identity)
        me = await gate.me()
        recipients = [k.public_key for k in me.keys]
        if public_key not in recipients:
            raise ServeError(
                f"the key this worker holds ({public_key}) is not one of {profile.party}'s "
                f"recipients. Buyers encrypt to the registered set, so this worker could "
                f"not read a single input. Register it with `kontra recipients add`, or "
                f"point {KEY_FILE_ENV} at a key that is registered "
                f"(looked for in {Profile.key_path(home_dir)})."
            )

        jobs = JobDirectory(gate)
        stack.push_async_callback(jobs.aclose)
        runtime = _runtime.Runtime(
            party=profile.party,
            gate=gate,
            identity=identity,
            recipient=public_key,
            recipients=recipients,
            # A seller whose workflow hires another offering is a buyer there, and that
            # sub-job's delivery is encrypted to the JOB's key, which `kontra.hire` wrote
            # under this home (decisions I7). Read from disk so a delivery decoded outside
            # the hiring client -- a bound artifact, a later activity -- still opens.
            # Local to this host: a fleet that hires needs the shared-key work the issue
            # defers (`kontra hire --share`).
            more_identities=lambda: tuple(Profile.job_identities(home_dir)),
            jobs=jobs,
        )
        previous = _current_runtime()
        _runtime.install(runtime)
        stack.callback(_restore_runtime, runtime, previous)

        # Plaintext replies only for the services generated here, by their exact names.
        # Results and the seller's own copies go to the WHOLE fleet set, not just this
        # pod's key: another worker, and the seller's laptop, must be able to read them.
        codec = KontraCodec(
            identity,
            recipients,
            role="seller",
            buyer_keys=jobs,
            seller_keys=SellerKeyCache(gate),
            blobs=GateBlobStore(gate),
            services=[b.offering_id.service_name for b in self._offerings],
        )
        converter = seller_data_converter(codec)
        # Headers stay out of the codec: the kontra-job header must be readable before any
        # payload key is known (kontra.serve.interceptors).
        if client is None:
            client = await session.connect_temporal(
                "worker",
                data_converter=converter,
                header_codec_behavior=HeaderCodecBehavior.NO_CODEC,
            )
            # The token refresher dies with the worker, not with a session it may share.
            stack.push_async_callback(session.release, client)
        else:
            config = client.config()
            config["data_converter"] = converter
            config["header_codec_behavior"] = HeaderCodecBehavior.NO_CODEC
            client = Client(**config)
        if client.namespace != profile.namespace:
            raise ServeError(
                f"the Temporal client is for namespace {client.namespace!r}, but "
                f"{profile.party} serves from {profile.namespace!r}"
            )

        verifier = HoldVerifier(gate, home=home_dir)
        await verifier.load()
        hashes = await self._hash_lock(gate, profile, public_key)

        progress = ProgressForwarder(gate, jobs)
        progress.start()
        stack.push_async_callback(progress.aclose)

        interceptor = KontraInterceptor(progress)
        services: list[OfferingService] = []
        inner: list[temporalio.worker.Worker] = []
        for b in self._offerings:
            service = OfferingService(
                bound=b,
                namespace=client.namespace,
                party=profile.party,
                contract_hash=hashes[b.key],
                validator=b.validator(),
                verifier=verifier,
                jobs=jobs,
                open_deferred=codec.open_deferred,
                payload_converter=converter.payload_converter,
            )
            kwargs = dict(self._kwargs)
            kwargs["workflows"] = _dedupe([b.workflow, *self._kwargs.get("workflows", ())])
            kwargs["nexus_service_handlers"] = [
                service_handler(service),
                *self._kwargs.get("nexus_service_handlers", ()),
            ]
            kwargs["interceptors"] = [interceptor, *self._kwargs.get("interceptors", ())]
            inner.append(
                temporalio.worker.Worker(client, task_queue=b.offering_id.task_queue, **kwargs)
            )
            services.append(service)
        self._inner, self._services, self._client = tuple(inner), tuple(services), client
        self.progress, self.jobs, self.verifier = progress, jobs, verifier
        for s in services:
            log.info(
                "serving %s (contract %s) on task queue %r in namespace %r",
                s.key,
                s.contract_hash,
                s.bound.offering_id.task_queue,
                s.namespace,
            )

    async def _hash_lock(
        self, gate: GateClient, profile: Profile, public_key: str
    ) -> dict[str, str]:
        """The contract hash of each offering, checked against the catalog (§11.2)."""
        hashes: dict[str, str] = {}
        problems: list[str] = []
        for b in self._offerings:
            ours = b.contract_hash()
            hashes[b.key] = ours
            try:
                entry: m.CatalogEntry | None = await gate.get_offering(b.key)
            except GateError as e:
                if e.status != 404:
                    raise ServeError(f"cannot check {b.key} in the catalog: {e}") from e
                entry = None
            if entry is not None and entry.offering != b.key:
                entry = None  # the gate resolved to another version: ours is not published
            if entry is None:
                open_jobs = await self._open_jobs(gate, b.key)
                if open_jobs:
                    # Retired (the catalog no longer lists it), so its hash comes from a job
                    # hired under it: drifted code must not finish jobs bought for another
                    # contract any more than it may take new ones (§11.2).
                    hired = await self._hired_hash(gate, open_jobs[0])
                    if hired is not None and hired != ours:
                        problems.append(
                            f"{b.key}: this code's contract hash is {ours} but its open jobs "
                            f"were hired under {hired}; the types or offering.yaml changed "
                            "after publishing -- restore the published code to finish them"
                        )
                        continue
                    log.warning(
                        "%s is retired but still has %d open job(s); serving it until they "
                        "finish (no new jobs will come)",
                        b.key,
                        len(open_jobs),
                    )
                elif profile.dev:
                    try:
                        entry = await gate.publish(b.document())
                    except GateError as e:
                        problems.append(
                            f"{b.key} is not published and publishing it failed ({e.message}); "
                            "run `kontra publish` (it needs a recent login)"
                        )
                        continue
                    log.info("published %s (dev profile), contract %s", b.key, entry.contract_hash)
                    if entry.contract_hash != ours:
                        problems.append(
                            f"{b.key}: the catalog recorded contract {entry.contract_hash} "
                            f"for this code's {ours}"
                        )
                else:
                    problems.append(
                        f"{b.key} is not published: run `kontra publish` in {b.source.parent}"
                    )
                continue
            if entry.contract_hash != ours:
                problems.append(
                    f"{b.key}: this code's contract hash is {ours} but the catalog's is "
                    f"{entry.contract_hash}; the types or offering.yaml changed after "
                    "publishing -- republish or bump the version"
                )
            elif entry.retired:
                open_jobs = await self._open_jobs(gate, b.key)
                log.warning(
                    "%s is retired%s",
                    b.key,
                    f" but still has {len(open_jobs)} open job(s); serving them"
                    if open_jobs
                    else "; no new jobs will come",
                )
        if problems:
            raise ServeError("refusing to start:\n  " + "\n  ".join(problems))
        return hashes

    @staticmethod
    async def _open_jobs(gate: GateClient, key: str) -> list[str]:
        """The ids of the open jobs of ``key`` sold by us (bounded: for a warning and a
        hash check)."""
        found: list[str] = []
        for state in _OPEN_STATES:
            cursor: str | None = None
            for _ in range(_JOB_PAGES):
                try:
                    page = await gate.jobs(
                        as_="seller", state=state.value, cursor=cursor, limit=200
                    )
                except GateError as e:
                    log.warning("could not list open jobs of %s: %s", key, e)
                    return found
                found += [j.job_id for j in page.items if j.offering == key]
                cursor = page.next_cursor
                if cursor is None:
                    break
        return found

    @staticmethod
    async def _hired_hash(gate: GateClient, job_id: str) -> str | None:
        """The contract hash ``job_id`` was hired under (None: the gate would not say)."""
        try:
            return (await gate.seller_job(job_id)).contract_hash
        except GateError as e:
            log.warning("could not read the contract of job %s: %s", job_id, e)
            return None

    # ------------------------------------------------------------------ running

    async def run_async(
        self,
        *,
        session: Session | None = None,
        client: Client | None = None,
        gate: GateClient | None = None,
        home: str | Path | None = None,
        handle_signals: bool = True,
    ) -> None:
        """Start (see :meth:`prepare`) and serve until :meth:`shutdown`, SIGINT or SIGTERM
        (``handle_signals``, main thread only), then shut down gracefully (temporalio's
        ``graceful_shutdown_timeout``) and send the progress still queued.

        Request URLs stay out of the log at INFO (:func:`_quiet_url_logs`), and it returns
        only once temporalio's core threads are out of Python code, so the interpreter can
        exit right after (:func:`_settle_core_threads`)."""
        if self._running or self._stack is not None:
            raise RuntimeError("this Worker is already started")
        self._running = True
        self._loop = asyncio.get_running_loop()
        _quiet_url_logs()
        try:
            await self.prepare(session=session, client=client, gate=gate, home=home)
            with self._signals(handle_signals):
                try:
                    await self._serve()
                except Exception as failure:
                    # Before aclose(), which closes a session the worker made itself.
                    reason = await _refused(self._session)
                    if reason is None:
                        raise
                    raise ServeError(reason) from failure
        finally:
            # Release whoever waits in wait_ready(): this run will never be ready (a start
            # that failed, say a hash-lock refusal), and aclose() replaces the event.
            self._ready.set()
            try:
                await self.aclose()
                await _settle_core_threads()
            finally:
                self._running = False

    def run(self, **kwargs: Any) -> None:
        """``python worker.py`` / ``kontra serve``: :meth:`run_async` on a new event loop."""
        if not logging.getLogger().handlers:
            logging.basicConfig(
                level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
            )
        asyncio.run(self.run_async(**kwargs))

    async def wait_ready(self) -> None:
        """Until every inner worker polls, or the worker stopped trying (then
        :attr:`serving` is False and :meth:`run_async` raises or returns)."""
        await self._ready.wait()

    @property
    def serving(self) -> bool:
        """Every inner worker is polling and no shutdown was asked for."""
        return (
            bool(self._inner) and all(w.is_running for w in self._inner) and not self._stop.is_set()
        )

    def shutdown(self) -> None:
        """Ask a running worker to stop gracefully. Thread-safe; idempotent."""
        loop, stop = self._loop, self._stop
        if loop is None or loop.is_closed():
            stop.set()  # not running yet: run_async will return at once
            return
        try:
            running = asyncio.get_running_loop()
        except RuntimeError:
            running = None
        if running is loop:
            stop.set()
        else:
            loop.call_soon_threadsafe(stop.set)

    @contextlib.contextmanager
    def _signals(self, enabled: bool) -> Generator[None]:
        loop = asyncio.get_running_loop()
        installed: list[signal.Signals] = []
        if enabled and threading.current_thread() is threading.main_thread():
            task = asyncio.current_task()
            for sig in (signal.SIGINT, signal.SIGTERM):
                try:
                    loop.add_signal_handler(sig, self._on_signal, sig, task)
                except (NotImplementedError, RuntimeError, ValueError):
                    continue
                installed.append(sig)
        try:
            yield
        finally:
            for sig in installed:
                loop.remove_signal_handler(sig)

    def _on_signal(self, sig: signal.Signals, task: asyncio.Task[Any] | None) -> None:
        stop = self._stop
        if not stop.is_set():
            log.info("%s: shutting down gracefully (again to stop now)", sig.name)
            stop.set()
        elif task is not None:
            log.warning("%s again: stopping now", sig.name)
            task.cancel()

    async def _serve(self) -> None:
        stop, ready = self._stop, self._ready
        runs = {
            asyncio.create_task(w.run(), name=f"kontra-worker {w.task_queue}"): w
            for w in self._inner
        }
        stopper = asyncio.create_task(stop.wait())
        watcher = asyncio.create_task(self._watch_ready(list(runs)))
        try:
            await asyncio.wait([*runs, stopper], return_when=asyncio.FIRST_COMPLETED)
        except asyncio.CancelledError:  # a second signal, or the caller gave up: no grace
            for run in runs:
                run.cancel()
            await asyncio.gather(*runs, return_exceptions=True)
            raise
        finally:
            watcher.cancel()
            stopper.cancel()
            ready.set()  # nobody waits for a worker that is stopping
        # shutdown() also *waits* for a run that never started (forever): it is used only as
        # the signal here, and the runs themselves are awaited.
        shutdowns = [asyncio.create_task(w.shutdown()) for w in runs.values()]
        results = await asyncio.gather(*runs, return_exceptions=True)
        for s in shutdowns:
            s.cancel()
        await asyncio.gather(*shutdowns, return_exceptions=True)
        await asyncio.gather(watcher, stopper, return_exceptions=True)
        failures = [r for r in results if isinstance(r, BaseException)]
        if failures and not stop.is_set():
            raise failures[0]
        for f in failures:
            if not isinstance(f, asyncio.CancelledError):
                log.error("an inner worker failed while shutting down", exc_info=f)

    async def _watch_ready(self, runs: list[asyncio.Task[None]]) -> None:
        ready = self._ready
        while not all(w.is_running for w in self._inner):
            if any(t.done() for t in runs):
                break
            await asyncio.sleep(0.01)
        ready.set()

    async def aclose(self) -> None:
        """Release what :meth:`prepare` acquired (inner workers that never ran included)."""
        stack, self._stack = self._stack, None
        self._inner, self._services, self._client = (), (), None
        self._session = None
        try:
            if stack is not None:
                await stack.aclose()
        finally:
            if self._loop is not None:
                self._loop = None
                self._stop, self._ready = asyncio.Event(), asyncio.Event()
