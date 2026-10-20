"""``kontra.serve.Worker``: the process that serves a seller's offerings (PRD §6.5).

It is ``temporalio``'s client and worker with :class:`~kontra.plugin.KontraPlugin` on them, and
the concerns of a process around them: ``python worker.py`` and ``kontra serve`` run it,
``kontra publish``/``contract``/``graph`` read it without starting anything, and
``kontra.testing.Marketplace`` serves it in a test. Starting it (:meth:`Worker.run` /
:meth:`Worker.run_async`) connects ``Client.connect(plugins=[*your client plugins, kontra])``
-- the plugin last, so it has the final word on the connection, the namespace and the codec
--, then builds one ``temporalio.worker.Worker`` per offering on that client, on the
offering's own task queue (derived from ``name@version``; passing ``task_queue`` is a
``TypeError``), and runs them until SIGINT/SIGTERM or :meth:`Worker.shutdown`.

Every ``temporalio.worker.Worker`` keyword is passed to each inner worker verbatim, except:
``workflows`` (the offering's entry class goes first: it is how the plugin knows which
offering a worker serves), ``workflow_runner`` (Temporal's ``SandboxedWorkflowRunner``
arrives as a copy with ``kontra`` passed through, :func:`with_kontra_passthrough`; any other
runner is untouched), and ``plugins``, which are routed -- a plugin that is also a
``temporalio.client.Plugin`` (any ``SimplePlugin``: ``OpenAIAgentsPlugin``, the agent
harness's ``AgentHarnessPlugin``, :class:`~kontra.contrib.parallel.ParallelPlugin`) goes on the
client, in your order, before Kontra's, so it may set the payload converter; a worker-only
plugin goes to the inner workers, and runs after Kontra's worker configuration (the plugin
checks before polling that nothing Kontra set was undone). The accepted keywords are read
from temporalio's own signature, so a new temporalio keyword passes through without a
Kontra release.

The connection takes two of ``Client.connect``'s own keywords, ``runtime`` and
``interceptors``, as ``kontra serve --metrics`` and ``--otlp`` pass them
(:mod:`kontra.serve.telemetry`): the runtime whose telemetry serves the SDK's metrics, and
client interceptors, which temporalio also puts on every worker of that client.
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
from typing import TYPE_CHECKING, Any, Unpack

import temporalio.client
import temporalio.runtime
import temporalio.worker
from temporalio.client import Client
from temporalio.contrib.pydantic import PydanticPayloadConverter
from temporalio.converter import DataConverter
from temporalio.worker import WorkerConfig, WorkflowRunner
from temporalio.worker.workflow_sandbox import SandboxedWorkflowRunner, SandboxRestrictions

from kontra.auth import Session
from kontra.codec import KontraCodec, data_converter
from kontra.gate_client import GateClient
from kontra.serve.errors import ServeError
from kontra.serve.handler import OfferingService
from kontra.serve.hold import HoldVerifier
from kontra.serve.jobs import JobDirectory
from kontra.serve.offering import BoundOffering
from kontra.serve.progress import ProgressForwarder

if TYPE_CHECKING:
    from kontra.plugin import KontraPlugin

__all__ = [
    "TEMPORAL_KWARGS",
    "ServeError",
    "Worker",
    "seller_data_converter",
    "with_kontra_passthrough",
    "workflow_runner",
]

log = logging.getLogger("kontra.serve")

#: Every keyword ``temporalio.worker.Worker`` takes, read from its signature.
TEMPORAL_KWARGS: frozenset[str] = frozenset(
    name
    for name, p in inspect.signature(temporalio.worker.Worker.__init__).parameters.items()
    if name != "self" and p.kind is not inspect.Parameter.VAR_KEYWORD
)
#: Loggers that write the full URL of every request at INFO: the bucket's presigned URLs
#: (their signatures included) and every URL a seller's activities fetch for a buyer.
_URL_LOGGERS = ("httpx",)
#: The longest a stopping worker waits for temporalio's own threads to leave Python code.
_SETTLE_TIMEOUT = 5.0


def seller_data_converter(codec: KontraCodec) -> DataConverter:
    """:func:`kontra.codec.data_converter` (failures encrypted too) with the pydantic payload
    converter, so models in workflow and activity signatures round-trip: what a Kontra
    worker's client gets when no plugin chose a payload converter (tests build clients
    with it)."""
    return dataclasses.replace(
        data_converter(codec), payload_converter_class=PydanticPayloadConverter
    )


#: What a Kontra worker's sandbox passes through: ``kontra`` -- Kontra's own turn wrapper
#: (:mod:`kontra.serve.turn`) runs in it, a seller's models may use :mod:`kontra.types`, a
#: workflow that hires imports :mod:`kontra.hire`, and :mod:`kontra.contrib.parallel` keeps
#: one class identity for its spec.
_PASSTHROUGH = "kontra"


def workflow_runner() -> SandboxedWorkflowRunner:
    """Temporal's sandbox with ``kontra`` passed through: the ``kontra`` modules a workflow
    reaches are the process's, imported once rather than every run. Safe because nothing in
    ``kontra`` keeps module-level state a workflow sees."""
    return SandboxedWorkflowRunner(
        restrictions=SandboxRestrictions.default.with_passthrough_modules(_PASSTHROUGH)
    )


def with_kontra_passthrough(runner: WorkflowRunner | None) -> WorkflowRunner:
    """``runner`` with ``kontra`` passed through when it is Temporal's sandbox (yours or the
    default), :func:`workflow_runner` when there is none; any other runner is yours, kept as
    it is."""
    if runner is None:
        return workflow_runner()
    if not isinstance(runner, SandboxedWorkflowRunner):
        return runner
    if _PASSTHROUGH in runner.restrictions.passthrough_modules:
        return runner
    return dataclasses.replace(
        runner, restrictions=runner.restrictions.with_passthrough_modules(_PASSTHROUGH)
    )


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
        from kontra.plugin import check_offerings

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
        self._offerings: tuple[BoundOffering, ...] = check_offerings(offerings)
        self._kwargs: dict[str, Any] = dict(kwargs)
        self._kontra: KontraPlugin | None = None
        self._inner: tuple[temporalio.worker.Worker, ...] = ()
        self._stack: contextlib.AsyncExitStack | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        # Events bind to a loop on first wait, not here; aclose() makes fresh ones so a
        # stopped Worker can run again (in another loop, too).
        self._stop = asyncio.Event()
        self._ready = asyncio.Event()
        self._client: Client | None = None
        self._running = False
        self._project: Path | None = None

    # ------------------------------------------------------------------ introspection

    @property
    def offerings(self) -> tuple[BoundOffering, ...]:
        return self._offerings

    @property
    def options(self) -> dict[str, Any]:
        """The temporalio keywords as given (a copy): ``plugins`` as passed, before they are
        routed between the client and the inner workers."""
        return dict(self._kwargs)

    @property
    def project(self) -> Path | None:
        """The directory of the ``worker.py`` that runs this Worker -- ``python worker.py``'s
        (``serve()`` sets it) or ``kontra serve --dir``'s -- else None. Under a dev profile,
        the graph page's static graphs are drawn from it as the worker starts (decisions
        I35); a Worker no one set it on (a test's, a seller's own main) draws none."""
        return self._project

    @project.setter
    def project(self, directory: str | Path | None) -> None:
        self._project = Path(directory).expanduser().resolve() if directory is not None else None

    @property
    def plugin(self) -> KontraPlugin | None:
        """The :class:`~kontra.plugin.KontraPlugin` this run serves with; None until started."""
        return self._kontra

    @property
    def inner_workers(self) -> tuple[temporalio.worker.Worker, ...]:
        """One ``temporalio.worker.Worker`` per offering, in order; empty until started."""
        return self._inner

    @property
    def services(self) -> tuple[OfferingService, ...]:
        """The generated Nexus services, in offering order; empty until started."""
        return self._kontra.services if self._kontra is not None else ()

    @property
    def client(self) -> Client | None:
        """The Temporal client the inner workers use (codec included); None until started."""
        return self._client

    @property
    def progress(self) -> ProgressForwarder | None:
        return self._kontra.progress if self._kontra is not None else None

    @property
    def jobs(self) -> JobDirectory | None:
        return self._kontra.jobs if self._kontra is not None else None

    @property
    def verifier(self) -> HoldVerifier | None:
        return self._kontra.verifier if self._kontra is not None else None

    # ------------------------------------------------------------------ starting

    def _routed(self) -> tuple[list[Any], list[Any]]:
        """``plugins`` split: client plugins (in the seller's order) and worker-only ones."""
        given = list(self._kwargs.get("plugins", ()))
        client = [p for p in given if isinstance(p, temporalio.client.Plugin)]
        return client, [p for p in given if not isinstance(p, temporalio.client.Plugin)]

    async def prepare(
        self,
        *,
        session: Session | None = None,
        client: Client | None = None,
        gate: GateClient | None = None,
        home: str | Path | None = None,
        runtime: temporalio.runtime.Runtime | None = None,
        interceptors: Sequence[temporalio.client.Interceptor] = (),
        stream_upload_budget: float | None = None,
    ) -> None:
        """Connect and build the inner workers, without polling yet (the plugin's start
        checks -- JWKS, the hash lock -- run as they start).

        ``session``: a logged-in :class:`kontra.auth.Session` (default: ``KONTRA_API_KEY``
        if set, else the profile in ``home`` / ``$KONTRA_HOME``; a key serves as well as a
        login, it only cannot publish). ``client``: a Temporal client to share the connection
        of (Kontra's plugin is added to a copy of it: its namespace and codec are replaced).
        ``gate``: the gate client (default: the session's). The last three exist for tests.

        ``runtime`` and ``interceptors``: ``Client.connect``'s, for the connection this
        worker makes. They are how ``kontra serve --metrics`` / ``--otlp`` reach it, not a
        setting of ``worker.py`` (``Worker()`` takes neither). A ``client`` given has its
        own, so passing them with one is a ``TypeError``.

        ``stream_upload_budget``: :class:`~kontra.plugin.KontraPlugin`'s (default:
        ``$KONTRA_STREAM_UPLOAD_BUDGET``, else the plugin's 2 s). Like the first three, for
        tests: :meth:`kontra.testing.Marketplace.serve` gives a loaded test machine more, and
        a seller run as its own process (``python worker.py``) takes it from the environment.
        """
        from kontra.codec.stream import upload_budget_from_env
        from kontra.plugin import TARGET, KontraPlugin

        if self._stack is not None:
            raise RuntimeError("this Worker is already started")
        if client is not None and (runtime is not None or interceptors):
            raise TypeError(
                "runtime and interceptors are for the connection the worker makes; a client "
                "passed in has its own (Client.connect(runtime=..., interceptors=...))"
            )
        kontra = KontraPlugin(
            *self._offerings,
            session=session,
            home=home,
            gate=gate,
            stream_upload_budget=(
                upload_budget_from_env() if stream_upload_budget is None else stream_upload_budget
            ),
            project=self._project,
        )
        stack = contextlib.AsyncExitStack()
        stack.push_async_callback(kontra.aclose)
        client_plugins, worker_plugins = self._routed()
        try:
            if client is None:
                client = await Client.connect(
                    TARGET,
                    runtime=runtime,
                    interceptors=list(interceptors),
                    plugins=[*client_plugins, kontra],
                )
            else:
                await kontra.prepare()  # Client(...) runs no connection hook
                shared = client.config()
                shared["plugins"] = [*shared["plugins"], *client_plugins, kontra]
                client = Client(**shared)
            inner: list[temporalio.worker.Worker] = []
            for b in self._offerings:
                kwargs = dict(self._kwargs)
                kwargs.pop("plugins", None)
                # The entry class first: it is how the plugin knows which offering this
                # worker serves (its own workflows go before it in the final list).
                kwargs["workflows"] = _dedupe(
                    [b.entry_workflow, *self._kwargs.get("workflows", ())]
                )
                inner.append(
                    temporalio.worker.Worker(
                        client,
                        task_queue=b.offering_id.task_queue,
                        plugins=worker_plugins,
                        **kwargs,
                    )
                )
        except BaseException:
            await stack.aclose()
            raise
        self._kontra, self._client, self._inner, self._stack = kontra, client, tuple(inner), stack

    # ------------------------------------------------------------------ running

    async def run_async(
        self,
        *,
        session: Session | None = None,
        client: Client | None = None,
        gate: GateClient | None = None,
        home: str | Path | None = None,
        runtime: temporalio.runtime.Runtime | None = None,
        interceptors: Sequence[temporalio.client.Interceptor] = (),
        handle_signals: bool = True,
        stream_upload_budget: float | None = None,
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
            await self.prepare(
                session=session,
                client=client,
                gate=gate,
                home=home,
                runtime=runtime,
                interceptors=interceptors,
                stream_upload_budget=stream_upload_budget,
            )
            with self._signals(handle_signals):
                try:
                    await self._serve()
                except ServeError:
                    raise  # the plugin's refusal, already said
                except Exception as failure:
                    # Before aclose(), which closes a session the worker made itself.
                    kontra = self._kontra
                    reason = await kontra.refusal() if kontra is not None else None
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
        self._inner, self._client, self._kontra = (), None, None
        try:
            if stack is not None:
                await stack.aclose()
        finally:
            if self._loop is not None:
                self._loop = None
                self._stop, self._ready = asyncio.Event(), asyncio.Event()
