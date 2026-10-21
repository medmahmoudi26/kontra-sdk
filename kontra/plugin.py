"""``KontraPlugin``: sell offerings from a Temporal client's workers (decisions I29).

A ``temporalio`` plugin -- client and worker plugin both, a
:class:`temporalio.plugin.SimplePlugin` -- that does everything Kontra adds to a seller's
Temporal worker, and nothing else:

* **connection** (``connect_service_client``): this machine's profile (or
  ``KONTRA_API_KEY``) resolved, its key checked against the party's recipients, the
  Temporal address, TLS and the ``worker`` JWT from the profile (the JWT kept fresh in the
  background), the process's Kontra runtime installed, and temporalio checked against the
  tested range (:mod:`kontra._compat`);
* **client** (``configure_client``): the profile's namespace, and the codec. Kontra's codec is
  a payload *codec*, so it stacks under any payload *converter*: a plugin earlier in the list
  (OpenAI Agents, the agent harness) keeps the converter it set, and temporalio's default is
  replaced by the pydantic one. Kontra refuses what would defeat it: another payload codec,
  a failure converter other than temporalio's default (Kontra installs its own, which
  encrypts failure messages), and external storage that would offload a job's payload where
  the buyer cannot read it. Headers stay out of the codec. A client plugin placed after
  Kontra has the last word on the client, so the worker hooks check the client again;
* **worker** (``configure_worker``): a worker whose ``workflows`` hold an offering's entry
  class -- the bound workflow, or the agent of a bound handler; for several offerings of one
  class, the one whose task queue the worker names -- serves that offering: its
  task queue (from the contract's ``name@version``), the generated Nexus service, Kontra's
  interceptor first, Temporal's sandbox with ``kontra`` passed through, and for a bound
  handler the turn wrapper and its activity (:mod:`kontra.serve.turn`). Any other worker of
  the client is left alone;
* **start** (``run_worker``), once per plugin: the gate's JWKS for the hold check, the hash
  lock (each offering's contract hash must equal the catalog's; a ``kontra dev`` profile
  publishes an unpublished version), the progress forwarder; then a check that no plugin
  that ran after Kontra (one given to the worker only) undid any of the above. A failed
  check fails the workers starting with it and is then forgotten: a later start checks
  again. Under a dev profile, with the project's directory known (``project``), the graph
  page's static graph of each workflow offering whose contract hash or code is not what it
  was drawn from is drawn again in the background, in a child process (:mod:`kontra._graphs`,
  decisions I35): a failure there is a log line, never a refusal to serve.

Put it **last** in ``Client.connect(plugins=[...])``: the last client plugin has the final word
on the connection, the namespace and the converter. ``kontra.serve.serve`` and
:class:`kontra.serve.Worker` build it themselves; a seller with a worker main of their own uses
it directly::

    client = await Client.connect("kontra", plugins=[*theirs, KontraPlugin.from_profile(SweepFlow)])
    worker = Worker(client, task_queue="kontra", workflows=[SweepFlow], activities=[...])
    await worker.run()

Kontra owns the target host, the namespace and the task queue: whatever is passed for them is
replaced (and logged when it was not the placeholder ``"kontra"``).
"""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import logging
import sys
from collections.abc import AsyncGenerator, Awaitable, Callable, Iterable, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import temporalio.worker
from temporalio.client import Client, ClientConfig, HeaderCodecBehavior
from temporalio.contrib.pydantic import PydanticPayloadConverter
from temporalio.converter import DataConverter, DefaultFailureConverter, DefaultPayloadConverter
from temporalio.plugin import SimplePlugin
from temporalio.service import ConnectConfig, ServiceClient
from temporalio.worker import WorkerConfig

from kontra import _graphs, _runtime
from kontra._compat import check_temporalio
from kontra.auth import Session
from kontra.codec import GateBlobStore, KontraCodec, KontraFailureConverter
from kontra.codec.age import public_key_of
from kontra.codec.stream import ENVELOPE_MAX, STREAM_UPLOAD_BUDGET, check_upload_budget
from kontra.gate_client import GateClient, GateError
from kontra.gate_client import models as m
from kontra.profile import KEY_FILE_ENV, PROFILE_FILE, NotLoggedIn, Profile, kontra_home
from kontra.serve.errors import ServeError
from kontra.serve.handler import OfferingService, service_handler
from kontra.serve.hold import HoldVerifier
from kontra.serve.interceptors import KontraInterceptor
from kontra.serve.jobs import JobDirectory, SellerKeyCache
from kontra.serve.offering import BoundOffering, QuoteFunction, offering
from kontra.serve.progress import ProgressForwarder
from kontra.serve.turn import TurnActivities, TurnWorkflow
from kontra.serve.worker import with_kontra_passthrough

__all__ = ["PLUGIN_NAME", "TARGET", "KontraPlugin"]

log = logging.getLogger("kontra.serve")

#: The plugin's name (temporalio's ``Plugin.name()``).
PLUGIN_NAME = "kontra.KontraPlugin"
#: What to pass as ``Client.connect``'s ``target_host``: the plugin replaces it with the
#: profile's Temporal address.
TARGET = "kontra"
_OPEN_STATES = (m.JobState.SUBMITTED, m.JobState.WORKING)
_JOB_PAGES = 20
#: The longest a failed worker waits for the gate to say whether it still accepts its session.
_REFUSAL_TIMEOUT = 10.0
#: Bytes an encoded payload carries beyond its ciphertext (the codec's metadata, protobuf
#: framing), with room to spare: external storage must not fire below the claim-check
#: threshold plus this.
_INLINE_OVERHEAD = 4096


async def refused(session: Session | None) -> str | None:
    """After a worker failed: why, when it is that the gate no longer accepts the worker's
    session -- its API key revoked, its login signed out or disallowed. Temporal refuses
    that session's tokens within seconds of the gate (the revocation list), and temporalio
    reports it as a bare ``RuntimeError("Workflow worker failed")`` over a PermissionDenied
    deep in its core: a seller who revoked a key on purpose, or found one revoked, deserves
    the reason and what to do. Asked of the gate, never read from the error's text. None
    when the gate still accepts the session, or cannot be asked in time: the failure is
    another one, and is raised as it came."""
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


def check_offerings(offerings: Sequence[BoundOffering]) -> tuple[BoundOffering, ...]:
    """The offerings one plugin (one worker process) serves: at least one, each bound once."""
    bound = tuple(offerings)
    if not bound:
        raise ValueError("Kontra needs at least one offering: offering(...) or serve(MyFlow)")
    keys: set[str] = set()
    for b in bound:
        if not isinstance(b, BoundOffering):  # pyright: ignore[reportUnnecessaryIsInstance]
            raise TypeError(f"offerings must come from kontra.serve.offering(...), got {b!r}")
        if b.key in keys:
            raise ValueError(f"{b.key} is bound twice")
        keys.add(b.key)
    return bound


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


def _home_dir(profile: Profile, home: str | Path | None) -> Path:
    """Where the profile lives: ``jwks.json`` is cached next to it."""
    if profile.home is not None:
        return profile.home
    return Path(home).expanduser() if home is not None else kontra_home()


def _dedupe(items: Iterable[Any]) -> list[Any]:
    out: list[Any] = []
    for item in items:
        if not any(item is seen for seen in out):
            out.append(item)
    return out


def _storage_floor(codec: KontraCodec) -> int:
    """The payload size external storage must stay above: the codec's claim-check threshold
    (when it has a blob store) and the stream envelope's limit, whichever is larger, plus
    framing."""
    return max(codec.claim_check_threshold or 0, ENVELOPE_MAX) + _INLINE_OVERHEAD


def _storage_problem(dc: DataConverter, codec: KontraCodec) -> str | None:
    """What is wrong with ``dc``'s external storage, if anything: a threshold at or below the
    codec's claim-check threshold (plus framing) offloads a job's payload to a store its
    buyer cannot read, and one at or below the stream envelope's limit
    (:data:`~kontra.codec.stream.ENVELOPE_MAX`) would offload a poll's answer the gate's
    relay must read (decisions I31). Encoding is converter, codec, then storage: storage
    only ever sees ciphertext, but the reference it leaves in history resolves only on the
    seller's side."""
    storage = dc.external_storage
    if storage is None:
        return None
    threshold = storage.payload_size_threshold
    if threshold > _storage_floor(codec):
        return None
    inline = codec.claim_check_threshold
    claim = f"claim-check threshold ({inline} bytes of ciphertext) or its " if inline else ""
    return (
        f"external storage offloads payloads from {threshold} bytes, at or below Kontra's "
        f"{claim}stream envelope limit ({ENVELOPE_MAX} bytes)"
    )


def _streams_problem(bound: BoundOffering, entry: m.CatalogEntry) -> str | None:
    """``streams:`` is fixed per published version (decisions I31): the catalog's list and
    the code's must be the same. The codec seals by the worker's own list, so a topic only
    the catalog names reaches no buyer; refusing says why before a buyer asks."""
    theirs = tuple(sorted(entry.streams))
    if theirs == bound.streams:
        return None
    return (
        f"{bound.key}: offering.yaml lists streams {list(bound.streams)} but the catalog has "
        f"{list(theirs)} for this version: streams are fixed per version -- publish a new one"
    )


class _ServiceKey:
    """A ``ServiceClient`` as :meth:`kontra.auth.Session.keep_fresh` holds a client: an
    ``api_key`` property whose setter re-sends it on every later call, as
    ``temporalio.client.Client.api_key`` does."""

    def __init__(self, service: ServiceClient) -> None:
        self._service = service

    @property
    def api_key(self) -> str | None:
        return self._service.config.api_key

    @api_key.setter
    def api_key(self, value: str | None) -> None:
        self._service.config.api_key = value
        self._service.update_api_key(value)


@dataclass
class _Prepared:
    """What :meth:`KontraPlugin.prepare` resolved and built."""

    session: Session
    profile: Profile
    gate: GateClient
    codec: KontraCodec
    jobs: JobDirectory
    verifier: HoldVerifier
    progress: ProgressForwarder
    interceptor: KontraInterceptor
    #: The refreshers started for service clients this plugin connected.
    keys: list[_ServiceKey] = field(default_factory=list[_ServiceKey])
    #: The start checks (JWKS, hash lock) ran; or how they failed, for the workers starting
    #: alongside the one that ran them (``starting``): a worker started later checks again.
    checked: bool = False
    check_failure: Exception | None = None
    starting: int = 0
    #: Claimed workers running now (the progress forwarder runs while there are any).
    serving: int = 0
    #: The progress flush started when the last of them stopped (see ``_serving``).
    flushing: asyncio.Task[None] | None = None
    #: The graph page's static graphs being drawn, under a dev profile (decisions I35).
    graphs: _graphs.Refresh | None = None
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)


class KontraPlugin(SimplePlugin):
    """Sell ``offerings`` from this client's workers (see the module docstring). Put it last
    in ``Client.connect(plugins=[...])``.

    ``session``: a logged-in :class:`kontra.auth.Session` (default: ``KONTRA_API_KEY`` if
    set, else the profile in ``home`` / ``$KONTRA_HOME``, opened at :meth:`prepare` and
    closed by :meth:`aclose`). ``gate``: the gate client (default: the session's). Building
    the plugin does no I/O: a ``worker.py`` that builds one can be imported by
    ``kontra publish``.

    ``stream_upload_budget``: the seconds one workflow task may spend storing its jobs' new
    stream items in the bucket, all of its poll answers together (default
    :data:`~kontra.codec.stream.STREAM_UPLOAD_BUDGET`, 2 s; :class:`~kontra.codec.KontraCodec`'s).
    An item not stored within it reaches the buyer as a gap (``unavailable``): a slow or
    failing bucket costs items, never a workflow task. Keep it well under the workflow task
    timeout (Temporal's 10 s default).

    ``project``: the directory of the ``worker.py`` that binds the offerings, which
    ``serve()`` and ``kontra serve`` pass (:attr:`kontra.serve.Worker.project`): under a dev
    profile the graph page's static graphs are drawn from it (decisions I35); without it,
    none are.
    """

    def __init__(
        self,
        *offerings: BoundOffering,
        session: Session | None = None,
        home: str | Path | None = None,
        gate: GateClient | None = None,
        stream_upload_budget: float = STREAM_UPLOAD_BUDGET,
        project: str | Path | None = None,
    ) -> None:
        super().__init__(PLUGIN_NAME)
        self._offerings = check_offerings(offerings)
        self._session = session
        self._home = home
        self._gate = gate
        self._upload_budget = check_upload_budget(stream_upload_budget)
        self._project = Path(project) if project is not None else None
        self._state: _Prepared | None = None
        self._stack: contextlib.AsyncExitStack | None = None
        self._lock: asyncio.Lock | None = None
        self._services: dict[str, OfferingService] = {}
        self._handlers: dict[str, Any] = {}

    @classmethod
    def from_profile(
        cls,
        entry: type | Callable[..., Any],
        /,
        *,
        quote: QuoteFunction | None = None,
        home: str | Path | None = None,
        engine: str | None = None,
        stream_upload_budget: float = STREAM_UPLOAD_BUDGET,
    ) -> KontraPlugin:
        """The plugin for ``offering.yaml`` beside the calling file bound to ``entry`` (a
        ``@workflow.defn`` class or an agent's ``@agent.accepts`` handler), priced by
        ``quote`` (default: ``quote`` of ``quote.py`` beside the calling file) -- exactly
        ``serve()``'s rules, for a seller who runs their own worker main. The entry is
        required: the contract, the task queue and the generated Nexus service all come
        from it, and the yaml names no code (D11). No activity is discovered: list them on
        your worker. ``engine``: as :func:`kontra.serve`'s (derived from the entry; an
        explicit one must agree). ``stream_upload_budget``: as the constructor's."""
        from kontra.serve._serve import OFFERING_FILE, _base, _default_quote

        caller = sys._getframe(1).f_globals
        base = _base(caller)
        bound = offering(
            base / OFFERING_FILE,
            entry=entry,
            quote=quote if quote is not None else _default_quote(caller, base),
            engine=engine,
        )
        return cls(bound, home=home, stream_upload_budget=stream_upload_budget)

    # ------------------------------------------------------------------ introspection

    @property
    def offerings(self) -> tuple[BoundOffering, ...]:
        return self._offerings

    @property
    def prepared(self) -> bool:
        return self._state is not None

    @property
    def session(self) -> Session | None:
        """The session it serves with, once prepared."""
        return self._state.session if self._state is not None else self._session

    @property
    def codec(self) -> KontraCodec | None:
        return self._state.codec if self._state is not None else None

    @property
    def progress(self) -> ProgressForwarder | None:
        return self._state.progress if self._state is not None else None

    @property
    def jobs(self) -> JobDirectory | None:
        return self._state.jobs if self._state is not None else None

    @property
    def verifier(self) -> HoldVerifier | None:
        return self._state.verifier if self._state is not None else None

    @property
    def services(self) -> tuple[OfferingService, ...]:
        """The generated Nexus services of the workers built so far, in offering order."""
        return tuple(self._services[b.key] for b in self._offerings if b.key in self._services)

    # ------------------------------------------------------------------ preparing

    async def prepare(self) -> None:
        """Resolve the session, check this machine's key, install the runtime, build the
        codec; idempotent. ``Client.connect`` runs it (``connect_service_client``); call it
        yourself before ``Client(service_client, plugins=[..., plugin])``, which skips the
        connection hook."""
        if self._lock is None:
            self._lock = asyncio.Lock()
        async with self._lock:
            if self._state is not None:
                return
            check_temporalio()
            stack = contextlib.AsyncExitStack()
            try:
                state = await self._prepare(stack)
            except BaseException:
                await stack.aclose()
                raise
            self._state, self._stack = state, stack

    async def _prepare(self, stack: contextlib.AsyncExitStack) -> _Prepared:
        session = self._session
        if session is None:
            session = Session.from_env(self._home)  # KONTRA_API_KEY, else the profile
            stack.push_async_callback(session.aclose)
        profile = await session.resolve()
        gate = self._gate or session.gate
        home_dir = _home_dir(profile, self._home)
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
        kontra_runtime = _runtime.Runtime(
            party=profile.party,
            gate=gate,
            identity=identity,
            recipient=public_key,
            recipients=recipients,
            # A seller whose workflow hires another offering is a buyer there, and that
            # sub-job's delivery is encrypted to the JOB's key, which `kontra.hire` wrote
            # under this home (decisions I7). Read from disk so a delivery decoded outside
            # the hiring client -- a bound artifact, a later activity -- still opens.
            # Local to this host: a fleet's mounted key is never one of the account's
            # devices (decisions I25), so a sub-job's key stays where it was hired.
            more_identities=lambda: tuple(Profile.job_identities(home_dir)),
            jobs=jobs,
        )
        previous = _current_runtime()
        _runtime.install(kontra_runtime)
        stack.callback(_restore_runtime, kontra_runtime, previous)
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
            stream_upload_budget=self._upload_budget,
        )
        # Heartbeats are published on each job's own stream (decisions I31), with the
        # worker's client once a worker is configured (bind).
        progress = ProgressForwarder()
        stack.push_async_callback(progress.aclose)
        # The topics a buyer may read of each offering's job stream, by its task queue.
        streams = {b.offering_id.task_queue: b.streams for b in self._offerings}
        return _Prepared(
            session=session,
            profile=profile,
            gate=gate,
            codec=codec,
            jobs=jobs,
            verifier=HoldVerifier(gate, home=home_dir),
            progress=progress,
            interceptor=KontraInterceptor(progress, streams=streams),
        )

    def _prepared(self, hook: str) -> _Prepared:
        if self._state is None:
            raise ServeError(
                f"KontraPlugin.{hook} ran before the plugin was prepared: put it on the "
                "client, last -- Client.connect(..., plugins=[..., kontra]) prepares it; a "
                "client built from a service client, Client(service_client, plugins=[...]), "
                "needs `await kontra.prepare()` first"
            )
        return self._state

    # ------------------------------------------------------------------ client

    async def connect_service_client(
        self,
        config: ConnectConfig,
        next: Callable[[ConnectConfig], Awaitable[ServiceClient]],
    ) -> ServiceClient:
        await self.prepare()
        state = self._prepared("connect_service_client")
        profile = state.profile
        if config.target_host not in (TARGET, profile.temporal_address):
            log.info(
                "connecting to %s, the profile's Temporal, not %s",
                profile.temporal_address,
                config.target_host,
            )
        if config.tls is not None and config.tls != profile.tls:
            log.info("TLS to Temporal is the profile's (%s)", "on" if profile.tls else "off")
        token = await state.session.temporal_token("worker")
        service = await next(
            dataclasses.replace(
                config, target_host=profile.temporal_address, api_key=token.token, tls=profile.tls
            )
        )
        key = _ServiceKey(service)
        # The JWT is replaced in the background before it expires, until aclose().
        state.session.keep_fresh(key, "worker")
        state.keys.append(key)
        return service

    def configure_client(self, config: ClientConfig) -> ClientConfig:
        state = self._prepared("configure_client")
        namespace = state.profile.namespace
        if config.get("namespace") not in (namespace, "default"):
            log.info("serving from %s's namespace %r", state.profile.party, namespace)
        config["namespace"] = namespace
        config["data_converter"] = self._data_converter(config.get("data_converter"), state.codec)
        # The kontra-job header must be readable before any payload key is known
        # (kontra.serve.interceptors): headers stay out of the codec.
        if config.get("header_codec_behavior", HeaderCodecBehavior.NO_CODEC) != (
            HeaderCodecBehavior.NO_CODEC
        ):
            log.warning("headers are not encoded on a Kontra worker's client (the job header)")
        config["header_codec_behavior"] = HeaderCodecBehavior.NO_CODEC
        return config

    @staticmethod
    def _data_converter(existing: DataConverter | None, codec: KontraCodec) -> DataConverter:
        """``existing`` with Kontra's codec and failure converter, its payload converter kept
        unless it is temporalio's default (then pydantic). Refused: another codec, another
        failure converter, external storage below the claim-check threshold."""
        dc = existing if existing is not None else DataConverter.default
        if dc.payload_codec is codec and dc.failure_converter_class is KontraFailureConverter:
            return dc  # this client was configured by this plugin already
        if dc.payload_codec is not None:
            raise ServeError(
                f"the client already has a payload codec ({type(dc.payload_codec).__name__}); "
                "Kontra's codec encrypts every payload end to end and cannot be stacked with "
                "another: remove it (or the plugin that sets it)"
            )
        if dc.failure_converter_class is not DefaultFailureConverter:
            raise ServeError(
                f"the client has the failure converter {dc.failure_converter_class.__name__}; "
                "Kontra installs its own, which encrypts failure messages: remove it"
            )
        storage = _storage_problem(dc, codec)
        if storage is not None:
            raise ServeError(
                f"the client's {storage}: a job's payload would go where its buyer (or the "
                f"gate's stream relay) cannot read it. Raise its payload_size_threshold above "
                f"{_storage_floor(codec)}, or remove it"
            )
        converter = dc.payload_converter_class
        if converter is DefaultPayloadConverter:
            converter = PydanticPayloadConverter
        return dataclasses.replace(
            dc,
            payload_converter_class=converter,
            payload_codec=codec,
            failure_converter_class=KontraFailureConverter,
        )

    @staticmethod
    def _client_problems(client: Client | None, codec: KontraCodec) -> list[str]:
        """What is wrong with the client a worker of Kontra's runs on: what
        :meth:`configure_client` sets, checked again on the client as it ended up. A client
        plugin placed after Kontra has the last word on the client, and may have put back a
        failure converter that writes messages in plaintext, offloading external storage, or
        header encoding."""
        if client is None:
            return ["it has no client"]
        dc = client.data_converter
        problems: list[str] = []
        if dc.payload_codec is not codec:
            problems.append("its client has no Kontra codec")
        if dc.failure_converter_class is not KontraFailureConverter:
            problems.append(
                f"its client's failure converter is {dc.failure_converter_class.__name__}, "
                "which writes failure messages in plaintext"
            )
        storage = _storage_problem(dc, codec)
        if storage is not None:
            problems.append(f"its client's {storage}")
        behavior = client.config(active_config=True).get(
            "header_codec_behavior", HeaderCodecBehavior.NO_CODEC
        )
        if behavior != HeaderCodecBehavior.NO_CODEC:
            problems.append("its client encodes headers (the job header must stay readable)")
        return problems

    # ------------------------------------------------------------------ worker

    def _claim(self, config: WorkerConfig) -> BoundOffering | None:
        """The offering a worker serves: the one whose entry class its ``workflows`` register
        (bound by code, never by a name), or None (not a Kontra worker). Several offerings
        bound to one class -- versions of the same code, say -- are told apart by the task
        queue the worker names, compared with each offering's constructed one
        (``kontra.serve.Worker`` names it); a worker that names none of them is refused."""
        given = list(config.get("workflows", ()))
        found = [b for b in self._offerings if any(w is b.entry_workflow for w in given)]
        if len(found) <= 1:
            return found[0] if found else None
        queue = config.get("task_queue")
        named = [b for b in found if b.offering_id.task_queue == queue]
        if len(named) == 1:
            return named[0]
        keys = ", ".join(b.key for b in found)
        raise ServeError(
            f"a worker registers the classes of {keys}: name the one it serves with its "
            "task queue (task_queue=offering.offering_id.task_queue), one worker per offering"
        )

    def configure_worker(self, config: WorkerConfig) -> WorkerConfig:
        bound = self._claim(config)
        if bound is None:
            return config  # another worker of the seller's: not Kontra's
        ours = self._handlers.get(bound.key)
        if ours is not None and any(h is ours for h in config.get("nexus_service_handlers", ())):
            return config  # configured already (the plugin was given to the worker too)
        state = self._prepared("configure_worker")
        client = config.get("client")
        if client is None or client.data_converter.payload_codec is not state.codec:
            raise ServeError(
                f"the worker serving {bound.key} must be built from a client that has this "
                "KontraPlugin (Client.connect(..., plugins=[..., kontra])); the plugin "
                "given to a worker alone cannot encrypt its payloads"
            )
        problems = self._client_problems(client, state.codec)
        if problems:
            raise ServeError(
                f"refusing to serve {bound.key}: a client plugin that ran after Kontra changed "
                f"the client ({'; '.join(problems)}); put KontraPlugin last in "
                "Client.connect(plugins=[...])"
            )
        queue = bound.offering_id.task_queue
        given = config.get("task_queue")
        if given not in (queue, TARGET):
            log.info("%s is served on its task queue %r, not %r", bound.key, queue, given)
        config["task_queue"] = queue
        entry: list[Any] = [bound.workflow]
        if bound.agent is not None:
            entry = [TurnWorkflow, bound.agent]
        config["workflows"] = _dedupe([*entry, *config.get("workflows", ())])
        service = OfferingService(
            bound=bound,
            namespace=client.namespace,
            party=state.profile.party,
            contract_hash=bound.contract_hash(),  # equal to the catalog's: run_worker checks
            validator=bound.validator(),
            verifier=state.verifier,
            jobs=state.jobs,
            open_deferred=state.codec.open_deferred,
            payload_converter=client.data_converter.payload_converter,
        )
        handler = service_handler(service)
        self._services[bound.key], self._handlers[bound.key] = service, handler
        config["nexus_service_handlers"] = [handler, *config.get("nexus_service_handlers", ())]
        config["interceptors"] = [state.interceptor, *config.get("interceptors", ())]
        config["workflow_runner"] = with_kontra_passthrough(config.get("workflow_runner"))
        state.progress.bind(client)
        if bound.kind == "handler":
            # The reply is checked against the served contract's output (decisions I38), and
            # the agent's listed topics are mirrored onto the job's stream (decisions I31).
            turn = TurnActivities(
                client, bound.output_type, bound.validator(), mirror=frozenset(bound.streams)
            )
            config["activities"] = [*config.get("activities", ()), turn.deliver]
        return config

    def _recheck(self, bound: BoundOffering, config: WorkerConfig, state: _Prepared) -> None:
        """What a plugin that ran after Kontra (given to the worker alone) may not undo."""
        problems: list[str] = []
        if config.get("task_queue") != bound.offering_id.task_queue:
            problems.append(f"its task queue is {config.get('task_queue')!r}")
        handler = self._handlers.get(bound.key)
        if not any(h is handler for h in config.get("nexus_service_handlers", ())):
            problems.append("Kontra's Nexus service is gone")
        interceptors = list(config.get("interceptors", ()))
        if not interceptors or interceptors[0] is not state.interceptor:
            problems.append("Kontra's interceptor is not the first")
        workflows = list(config.get("workflows", ()))
        for cls in (bound.workflow, bound.agent):
            if cls is not None and not any(w is cls for w in workflows):
                problems.append(f"{cls.__qualname__} is no longer registered")
        problems += self._client_problems(config.get("client"), state.codec)
        if problems:
            raise ServeError(
                f"refusing to serve {bound.key}: a plugin that ran after Kontra changed the "
                f"worker ({'; '.join(problems)}); give that plugin to the client, before "
                "Kontra, or drop it"
            )

    # ------------------------------------------------------------------ running

    def _claimed(self, worker: temporalio.worker.Worker) -> BoundOffering | None:
        """The offering a starting worker serves. Claimed as :meth:`configure_worker` claims
        it -- from the config the worker was built with, before any plugin, or from its final
        one -- else recognised by what Kontra put on it (the Nexus service it generated, an
        offering's task queue, its interceptor): a plugin that ran after Kontra may have
        swapped the entry class for another of the same name, and a worker Kontra configured
        is re-checked, never waved through as somebody else's."""
        active = worker.config(active_config=True)
        bound = self._claim(worker.config()) or self._claim(active)
        if bound is not None:
            return bound
        handlers = list(active.get("nexus_service_handlers", ()))
        for b in self._offerings:
            ours = self._handlers.get(b.key)
            if ours is not None and any(h is ours for h in handlers):
                return b
        for b in self._offerings:  # a worker polling an offering's queue serves it
            if active.get("task_queue") == b.offering_id.task_queue:
                return b
        state = self._state
        if state is not None and any(
            i is state.interceptor for i in active.get("interceptors", ())
        ):
            raise ServeError(
                "refusing to serve: a plugin that ran after Kontra changed a worker Kontra "
                "configured, and it no longer registers the class of any offering this plugin "
                f"binds ({', '.join(b.key for b in self._offerings)}); give that plugin to the "
                "client, before Kontra, or drop it"
            )
        return None

    async def run_worker(
        self,
        worker: temporalio.worker.Worker,
        next: Callable[[temporalio.worker.Worker], Awaitable[None]],
    ) -> None:
        config = worker.config(active_config=True)
        bound = self._claimed(worker)
        if bound is None:
            await next(worker)
            return
        state = self._prepared("run_worker")
        async with self._serving(state):
            self._recheck(bound, config, state)
            service = self._services[bound.key]
            log.info(
                "serving %s (contract %s) on task queue %r in namespace %r",
                bound.key,
                service.contract_hash,
                bound.offering_id.task_queue,
                service.namespace,
            )
            try:
                await next(worker)
            except Exception as failure:
                reason = await refused(state.session)
                if reason is None:
                    raise
                raise ServeError(reason) from failure

    @contextlib.asynccontextmanager
    async def _serving(self, state: _Prepared) -> AsyncGenerator[None]:
        """The start checks, once for the plugin, and the progress forwarder while any of
        its workers runs (it sends what is queued when the last one stops).

        A check that fails fails the workers starting with it, at once and with the same
        error; it is not remembered beyond them. A seller's main that runs the worker again
        -- after a gate that did not answer, or after publishing the version it was told is
        not published -- gets the checks again, not the first failure forever."""
        state.starting += 1
        try:
            async with state.lock:
                if state.check_failure is not None:
                    raise state.check_failure
                if not state.checked:
                    try:
                        await state.verifier.load()
                        hashes = await self._hash_lock(state.gate, state.profile)
                    except Exception as e:  # a cancellation is the cancelled worker's own
                        state.check_failure = e
                        raise
                    state.checked = True
                    state.graphs = await self._draw_graphs(state.profile, hashes)
                if state.serving == 0:
                    if state.flushing is not None:  # the previous run's, still sending
                        await asyncio.gather(state.flushing, return_exceptions=True)
                        state.flushing = None
                    state.progress.start()
                state.serving += 1
        finally:
            state.starting -= 1
            if state.starting == 0:
                state.check_failure = None
        try:
            yield
        finally:
            state.serving -= 1
            if state.serving == 0:
                # Not awaited here: temporalio's `async with worker` cancels the worker's
                # run as soon as its shutdown returns, and a run cancelled while it still
                # awaits cancels the task that entered the block. aclose() awaits it.
                state.flushing = asyncio.get_running_loop().create_task(
                    state.progress.aclose(), name="kontra-progress-flush"
                )

    async def refusal(self) -> str | None:
        """:func:`refused` for this plugin's session."""
        return await refused(self.session)

    async def aclose(self) -> None:
        """Release what :meth:`prepare` acquired: the JWT refreshers, the progress forwarder
        (what is queued is sent), the job directory, the runtime, and a session it opened."""
        state, self._state = self._state, None
        stack, self._stack = self._stack, None
        self._services.clear()
        self._handlers.clear()
        try:
            if state is not None:
                if state.graphs is not None:  # the child dies with the worker
                    await asyncio.to_thread(state.graphs.stop)
                for key in state.keys:
                    await state.session.release(key)
                if state.flushing is not None:
                    await asyncio.gather(state.flushing, return_exceptions=True)
        finally:
            if stack is not None:
                await stack.aclose()

    # ------------------------------------------------------------------ the graph page

    async def _draw_graphs(
        self, profile: Profile, hashes: dict[str, str]
    ) -> _graphs.Refresh | None:
        """Under a dev profile, once the hash lock passed: the graph page's static graphs of
        the workflow offerings whose stored graph was drawn from another contract hash or
        other code (:func:`kontra._graphs.code_hash` of the project), or that have none, drawn
        again in the background (:class:`kontra._graphs.Refresh`), from the project's
        directory. A handler offering has no static graph; a non-dev profile has no graph
        page. Never raises: a failure is a log line."""
        project = self._project
        if not profile.dev or project is None:
            return None
        try:
            home = _home_dir(profile, self._home)
            if not (home / PROFILE_FILE).is_file():
                return None  # an API key's session: nowhere the page reads
            workflows = [b for b in self._offerings if b.kind == "workflow"]
            if not workflows:
                return None
            code = await asyncio.to_thread(_graphs.code_hash, project)
            wanted = [
                _graphs.Wanted(b.key, b.offering_id.task_queue, hashes[b.key], code)
                for b in workflows
            ]
            stale = await asyncio.to_thread(_graphs.out_of_date, home, wanted)
            if not stale:
                return None
            refresh = _graphs.Refresh(home, project, stale)
            refresh.start()
        except Exception as e:  # a log line, never a refusal to serve
            log.warning("the graph page's static graphs were not drawn: %s", e)
            return None
        return refresh

    # ------------------------------------------------------------------ the hash lock

    async def _hash_lock(self, gate: GateClient, profile: Profile) -> dict[str, str]:
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
                        entry = await gate.publish(b.document(), streams=b.streams, engine=b.engine)
                    except GateError as e:
                        if e.code == "engine_fixed":
                            recorded = (e.detail or {}).get("engine")
                            problems.append(
                                f"{b.key} was published for the {recorded!r} engine; this "
                                f"worker serves the {b.engine!r} engine: publish a new version"
                            )
                            continue
                        problems.append(
                            f"{b.key} is not published and publishing it failed ({e.message}); "
                            "run `kontra publish` (it needs your login, confirmed within "
                            "the hour; in a terminal it asks)"
                        )
                        continue
                    log.info("published %s (dev profile), contract %s", b.key, entry.contract_hash)
                    if entry.contract_hash != ours:
                        problems.append(
                            f"{b.key}: the catalog recorded contract {entry.contract_hash} "
                            f"for this code's {ours}"
                        )
                    elif (streamed := _streams_problem(b, entry)) is not None:
                        problems.append(streamed)
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
            elif (streamed := _streams_problem(b, entry)) is not None:
                problems.append(streamed)
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
