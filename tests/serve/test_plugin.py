"""``kontra.KontraPlugin`` (decisions I29): what it does to a Temporal client and worker, what it
refuses, its order among other plugins, and the escape hatch -- a seller's own worker main,
``Client.connect(plugins=[KontraPlugin.from_profile(Flow)])`` and a plain
``temporalio.worker.Worker``, serving a whole Marketplace job with no ``kontra.serve.Worker``
and no CLI."""

from __future__ import annotations

import asyncio
import dataclasses
import importlib.util
import logging
import re
import runpy
import textwrap
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import pytest
import temporalio.worker
from temporalio.client import Client, ClientConfig, HeaderCodecBehavior
from temporalio.contrib.pydantic import PydanticPayloadConverter
from temporalio.converter import (
    DataConverter,
    DefaultFailureConverter,
    DefaultFailureConverterWithEncodedAttributes,
    PayloadCodec,
)
from temporalio.plugin import SimplePlugin

from kontra import KontraPlugin, _graphs
from kontra.codec import KontraCodec, KontraFailureConverter
from kontra.codec.age import generate_identity
from kontra.codec.stream import STREAM_UPLOAD_BUDGET
from kontra.plugin import PLUGIN_NAME
from kontra.profile import PROFILE_FILE
from kontra.serve import ServeError, Worker
from kontra.testing import Marketplace, TestParty
from kontra.testing.devserver import DevServer

from . import seller
from .flows import EchoChild, ImpostorFlow, UpperFlow, shout
from .models import Shout
from .probes import where
from .support import MARKER, Buyer, write_offering

AGENTS = importlib.util.find_spec("temporalio.openai_agents") is not None


def _codec() -> KontraCodec:
    identity, recipient = generate_identity()

    class _Blobs:
        async def put(self, digest: str, data: bytes) -> None: ...

        async def get(self, digest: str, *, limit: int) -> bytes:
            raise AssertionError("not fetched")

    return KontraCodec(identity, [recipient], role="seller", blobs=_Blobs())  # type: ignore[arg-type]


def _storage(threshold: int) -> Any:
    from temporal_agent_harness.utils.large_payload import local_payload_storage

    return local_payload_storage(payload_size_threshold=threshold)


# --------------------------------------------------------------------------- the converter


def test_the_codec_stacks_under_any_payload_converter() -> None:
    """Pydantic replaces temporalio's default payload converter; a converter another plugin
    set (OpenAI Agents', the harness's) is kept; the codec and Kontra's failure converter go
    on top. Applying it again to its own result changes nothing."""
    codec = _codec()
    made = KontraPlugin._data_converter(None, codec)
    assert made.payload_converter_class is PydanticPayloadConverter
    assert made.payload_codec is codec
    assert made.failure_converter_class is KontraFailureConverter
    assert KontraPlugin._data_converter(made, codec) is made

    class Theirs(PydanticPayloadConverter):
        pass

    kept = KontraPlugin._data_converter(DataConverter(payload_converter_class=Theirs), codec)
    assert (kept.payload_converter_class, kept.payload_codec) == (Theirs, codec)


def test_what_would_defeat_the_codec_is_refused() -> None:
    codec = _codec()

    class Other(PayloadCodec):
        async def encode(self, payloads: Any) -> Any:
            return list(payloads)

        async def decode(self, payloads: Any) -> Any:
            return list(payloads)

    with pytest.raises(ServeError, match=r"already has a payload codec \(Other\)"):
        KontraPlugin._data_converter(DataConverter(payload_codec=Other()), codec)
    with pytest.raises(ServeError, match="failure converter"):
        KontraPlugin._data_converter(
            DataConverter(failure_converter_class=DefaultFailureConverterWithEncodedAttributes),
            codec,
        )


def test_external_storage_must_not_take_a_payload_the_buyer_could_not_read() -> None:
    """Encoding is converter, then codec, then external storage: storage only ever sees
    ciphertext, but a payload it offloads leaves a reference in history that the buyer cannot
    resolve. At or below the codec's claim-check threshold (above it, the codec has already
    moved the ciphertext to the gate's bucket) it is refused; the harness's 1.5 MB is fine."""
    codec = _codec()
    assert codec.claim_check_threshold == 256 * 1024
    for threshold in (0, 64 * 1024, 256 * 1024, 256 * 1024 + 100):
        with pytest.raises(ServeError, match="external storage offloads payloads"):
            KontraPlugin._data_converter(DataConverter(external_storage=_storage(threshold)), codec)
    fine = KontraPlugin._data_converter(DataConverter(external_storage=_storage(1_500_000)), codec)
    assert fine.payload_codec is codec


def test_external_storage_must_not_take_a_stream_envelope() -> None:
    """A job stream's poll answer is an envelope of at most ``ENVELOPE_MAX`` (references,
    decisions I39) the gate's relay must read: external storage at or below it (plus framing)
    is refused, with or without a blob store -- with one, the claim-check threshold, which is
    larger, is the floor."""
    from kontra.codec.stream import ENVELOPE_MAX

    identity, recipient = generate_identity()
    plain = KontraCodec(identity, [recipient], role="seller")  # no blob store
    for codec in (_codec(), plain):
        floor = max(codec.claim_check_threshold or 0, ENVELOPE_MAX) + 4096
        for threshold in (ENVELOPE_MAX, floor):
            with pytest.raises(ServeError, match="stream envelope limit"):
                KontraPlugin._data_converter(
                    DataConverter(external_storage=_storage(threshold)), codec
                )
        assert (
            KontraPlugin._data_converter(
                DataConverter(external_storage=_storage(floor + 1)), codec
            ).payload_codec
            is codec
        )


async def test_the_hash_lock_refuses_streams_the_catalog_does_not_have(tmp_path: Path) -> None:
    """``streams:`` is fixed per published version (decisions I31, owner OD5): code whose
    list differs from the catalog's is refused at start, naming both lists."""
    from types import SimpleNamespace

    from kontra.plugin import _streams_problem  # pyright: ignore[reportPrivateUsage]

    bound = seller.bind(write_offering(tmp_path, streams=["events", "audit"]))
    assert bound.streams == ("audit", "events")
    same = SimpleNamespace(streams=["events", "audit"])
    assert _streams_problem(bound, same) is None  # type: ignore[arg-type]
    other = SimpleNamespace(streams=["events"])
    problem = _streams_problem(bound, other)  # type: ignore[arg-type]
    assert problem is not None
    assert "['audit', 'events']" in problem
    assert "['events']" in problem
    assert "publish a new one" in problem


def test_a_client_hook_before_prepare_says_what_to_do(tmp_path: Path) -> None:
    """``Client(service_client, plugins=[...])`` skips the connection hook, where the plugin
    prepares itself: configuring a client unprepared is refused, saying how."""
    plugin = KontraPlugin(seller.bind(write_offering(tmp_path)))
    assert plugin.name() == PLUGIN_NAME
    with pytest.raises(ServeError, match=r"await kontra\.prepare\(\)"):
        plugin.configure_client({"namespace": "x"})  # type: ignore[typeddict-item]


def test_a_worker_without_the_entry_class_is_not_kontras(tmp_path: Path) -> None:
    """Bound by code: a worker is Kontra's only if it registers the offering's class; any
    other worker of the client is left exactly as it was (and needs no preparation)."""
    plugin = KontraPlugin(seller.bind(write_offering(tmp_path)))
    config: Any = {"task_queue": "mine", "workflows": [EchoChild], "activities": [shout]}
    assert plugin.configure_worker(dict(config)) == config


def test_a_stream_upload_budget_that_bounds_nothing_is_refused(tmp_path: Path) -> None:
    """The budget is a finite number of seconds above 0, checked when the plugin is built
    (no I/O yet): 0 would stub every item, an infinite one hold a task on a slow bucket."""
    bound = seller.bind(write_offering(tmp_path))
    for bad in (0, -2.0, 30.0, float("inf"), float("nan")):
        with pytest.raises(ValueError, match="stream_upload_budget"):
            KontraPlugin(bound, stream_upload_budget=bad)


# --------------------------------------------------------------------------- on a running server


@pytest.mark.temporal
async def test_the_stream_upload_budget_is_the_codecs(
    dev_server: DevServer, tmp_path: Path
) -> None:
    """The codec the plugin builds stores a workflow task's stream items within the plugin's
    ``stream_upload_budget``: 2 s unless given (decisions I39)."""
    async with Marketplace(dev_server=dev_server) as mp:
        acme = await mp.party("acme")
        bound = seller.bind(write_offering(tmp_path))
        plugins = (
            (KontraPlugin(bound, session=acme.session), STREAM_UPLOAD_BUDGET),
            (KontraPlugin(bound, session=acme.session, stream_upload_budget=5), 5.0),
        )
        for plugin, budget in plugins:
            await plugin.prepare()
            try:
                assert plugin.codec is not None
                assert plugin.codec.stream_upload_budget == budget
            finally:
                await plugin.aclose()


@pytest.mark.temporal
async def test_a_plugin_given_to_the_worker_alone_is_refused(
    dev_server: DevServer, tmp_path: Path
) -> None:
    """Kontra's codec lives on the client; on a worker alone the plugin could not encrypt."""
    async with Marketplace(dev_server=dev_server) as mp:
        acme = await mp.party("acme")
        plugin = KontraPlugin(seller.bind(write_offering(tmp_path)), session=acme.session)
        await plugin.prepare()
        try:
            with pytest.raises(ServeError, match="must be built from a client that has"):
                temporalio.worker.Worker(
                    mp.client(acme.namespace),
                    task_queue="x",
                    workflows=[UpperFlow],
                    plugins=[plugin],
                )
        finally:
            await plugin.aclose()


@pytest.mark.temporal
@pytest.mark.skipif(not AGENTS, reason="temporalio-openai-agents (the dev group's) is missing")
async def test_kontra_goes_last_after_an_ai_sdk_plugin(
    dev_server: DevServer, tmp_path: Path
) -> None:
    """``OpenAIAgentsPlugin`` sets its payload converter and refuses one it does not know.
    After it, Kontra keeps it and adds the codec; before it, Kontra's pydantic converter is
    what the agents plugin refuses. ``kontra.serve.Worker`` routes a seller's client plugin
    to the client ahead of Kontra's, so its worker gets the agents' converter under the codec
    (the regression this guards: the model activity's input does not decode with strict
    pydantic)."""
    from agents import OpenAIProvider
    from temporalio.openai_agents import (
        ModelActivityParameters,
        OpenAIAgentsPlugin,
        OpenAIPayloadConverter,
    )

    def agents() -> OpenAIAgentsPlugin:
        # OpenAIProvider(): no client is built before the first model call (none here).
        return OpenAIAgentsPlugin(
            model_params=ModelActivityParameters(), model_provider=OpenAIProvider()
        )

    async with Marketplace(dev_server=dev_server) as mp:
        acme = await mp.party("acme")
        bound = seller.bind(write_offering(tmp_path))
        plugin = KontraPlugin(bound, session=acme.session)
        await plugin.prepare()
        try:
            service = mp.client(acme.namespace).service_client
            client = Client(service, plugins=[agents(), plugin])
            assert client.data_converter.payload_converter_class is OpenAIPayloadConverter
            assert client.data_converter.payload_codec is plugin.codec
            assert client.namespace == acme.namespace
            with pytest.raises(ValueError, match="must be of type OpenAIPayloadConverter"):
                Client(service, plugins=[plugin, agents()])
        finally:
            await plugin.aclose()
        worker = Worker(offerings=[bound], plugins=[agents()], activities=[shout])
        await worker.prepare(session=acme.session)
        try:
            assert worker.client is not None
            converter = worker.client.data_converter
            assert converter.payload_converter_class is OpenAIPayloadConverter
            assert isinstance(converter.payload_codec, KontraCodec)
            assert converter.failure_converter_class is KontraFailureConverter
        finally:
            await worker.aclose()


#: A seller's own worker main: KontraPlugin.from_profile binds offering.yaml beside it.
_MAIN = """
from kontra import KontraPlugin
from tests.serve import flows
from tests.serve.flows import UpperFlow
from tests.serve.seller import quote


def plugin(home):
    return KontraPlugin.from_profile(UpperFlow, quote=quote, home=home)
"""


def _main(directory: Path) -> dict[str, Any]:
    """The globals of ``directory/main.py``: ``plugin(home)`` calls
    ``KontraPlugin.from_profile`` from there, and ``flows`` is the module it binds (imported
    as the main imports it: the classes are compared by identity)."""
    directory.mkdir(parents=True)
    write_offering(directory).rename(directory / "offering.yaml")
    (directory / "main.py").write_text(textwrap.dedent(_MAIN).lstrip())
    return runpy.run_path(str(directory / "main.py"))


async def _hire(mp: Marketplace, party: TestParty, key: str, text: str) -> Shout:
    buyer = Buyer(mp, party)
    entry = await party.gate.get_offering(key)
    value = {"text": text, "copies": 2}
    hired = await buyer.hire(entry, value)
    return await (await buyer.start(hired, value, result_type=Shout)).result()


@pytest.mark.temporal
async def test_the_escape_hatch_serves_a_job_on_a_plain_temporal_worker(
    dev_server: DevServer, tmp_path: Path
) -> None:
    """No ``kontra.serve.Worker``, no CLI: the seller's main connects with Kontra's plugin
    last and builds temporalio's own worker. Kontra replaces the target host, the namespace
    and the task queue, adds the codec, the Nexus service, the hold check and the hash lock
    (a dev profile publishes), and a buyer's job runs to its result."""
    main = _main(tmp_path / "seller")
    flows = main["flows"]
    async with Marketplace(dev_server=dev_server) as mp:
        acme = await mp.party("acme")
        bob = await mp.party(f"bob-{uuid.uuid4().hex[:6]}", balance="100.00")
        plugin = main["plugin"](acme.home)
        [bound] = plugin.offerings
        assert bound.source == tmp_path / "seller" / "offering.yaml"
        client = await Client.connect("anywhere.invalid:7233", plugins=[plugin])
        try:
            assert client.namespace == acme.namespace
            assert client.data_converter.payload_codec is plugin.codec
            worker = temporalio.worker.Worker(
                client,
                task_queue="ignored",
                workflows=[flows.UpperFlow, flows.EchoChild],
                activities=[flows.shout, flows.count, where],
                activity_executor=ThreadPoolExecutor(2),  # count is not async
            )
            assert worker.task_queue == bound.offering_id.task_queue
            async with worker:
                async with asyncio.timeout(30):  # the start checks (a dev profile publishes)
                    while not worker.is_running:  # noqa: ASYNC110 (temporalio has no event)
                        await asyncio.sleep(0.05)
                out = await _hire(mp, bob, bound.key, f"hi {MARKER}")
            assert out.text == f"HI {MARKER.upper()}" * 2
            assert out.job is not None  # the job context reached the seller's activity
            entry = await acme.gate.get_offering(bound.key)
            assert entry.contract_hash == bound.contract_hash()
        finally:
            await plugin.aclose()


class _Hijack(temporalio.worker.Plugin):
    """A worker-only plugin that moves the worker to another task queue, after Kontra."""

    def configure_worker(self, config: Any) -> Any:
        config["task_queue"] = "elsewhere"
        return config

    async def run_worker(self, worker: temporalio.worker.Worker, next: Any) -> None:
        await next(worker)

    def configure_replayer(self, config: Any) -> Any:
        return config

    def run_replayer(self, replayer: Any, histories: Any, next: Any) -> Any:
        return next(replayer, histories)


@pytest.mark.temporal
async def test_a_plugin_after_kontra_cannot_undo_it(dev_server: DevServer, tmp_path: Path) -> None:
    """A plugin given to the worker alone runs after Kontra's worker configuration: before
    polling, Kontra checks its task queue, Nexus service, interceptor and codec are still
    there, and refuses to serve otherwise."""
    async with Marketplace(dev_server=dev_server) as mp:
        acme = await mp.party("acme")
        bound = seller.bind(write_offering(tmp_path))
        plugin = KontraPlugin(bound, session=acme.session)
        client = await Client.connect("kontra", plugins=[plugin])
        try:
            worker = temporalio.worker.Worker(
                client, task_queue="x", workflows=[UpperFlow], plugins=[_Hijack()]
            )
            assert worker.task_queue == "elsewhere"
            with pytest.raises(ServeError, match=r"a plugin that ran after Kontra changed"):
                await worker.run()
        finally:
            await plugin.aclose()


class _ImpostorPlugin(temporalio.worker.Plugin):
    """A worker-only plugin that swaps the entry class for another of its Temporal name, and
    with ``strip`` also moves the queue and drops Kontra's Nexus service and interceptor."""

    def __init__(self, *, strip: bool = False) -> None:
        self.strip = strip

    def configure_worker(self, config: Any) -> Any:
        config["workflows"] = [ImpostorFlow if w is UpperFlow else w for w in config["workflows"]]
        if self.strip:
            config["task_queue"] = "elsewhere"
            config["nexus_service_handlers"] = []
            config["interceptors"] = []
        return config

    async def run_worker(self, worker: temporalio.worker.Worker, next: Any) -> None:
        await next(worker)

    def configure_replayer(self, config: Any) -> Any:
        return config

    def run_replayer(self, replayer: Any, histories: Any, next: Any) -> Any:
        return next(replayer, histories)


@pytest.mark.temporal
@pytest.mark.parametrize(
    ("strip", "entry_from_a_client_plugin", "said"),
    [
        (False, False, r"UpperFlow is no longer registered"),
        (True, False, r"its task queue is 'elsewhere'; Kontra's Nexus service is gone"),
        (False, True, r"UpperFlow is no longer registered"),
    ],
)
async def test_a_plugin_after_kontra_cannot_swap_the_entry_class_past_the_check(
    dev_server: DevServer, tmp_path: Path, strip: bool, entry_from_a_client_plugin: bool, said: str
) -> None:
    """The start check used to claim the worker from its final config only: a worker-only
    plugin that replaced UpperFlow with another class of the same name took the worker out
    of Kontra's sight, and with it the check -- queue moved, Nexus service and interceptor
    gone, no ServeError. Now the worker is claimed from the config it was built with too,
    and by what Kontra put on it (its Nexus service, when the entry came from a client
    plugin before Kontra's and the worker's own config never named it), and refused."""
    async with Marketplace(dev_server=dev_server) as mp:
        acme = await mp.party("acme")
        plugin = KontraPlugin(seller.bind(write_offering(tmp_path)), session=acme.session)
        before = [SimplePlugin("adds-the-entry", workflows=[UpperFlow])]
        client = await Client.connect(
            "kontra", plugins=[*(before if entry_from_a_client_plugin else []), plugin]
        )
        try:
            worker = temporalio.worker.Worker(
                client,
                task_queue="x",
                workflows=[EchoChild] if entry_from_a_client_plugin else [UpperFlow],
                plugins=[_ImpostorPlugin(strip=strip)],
            )
            with pytest.raises(ServeError, match=r"a plugin that ran after Kontra changed") as e:
                async with asyncio.timeout(30):  # unchecked, it would serve until stopped
                    await worker.run()
            assert re.search(said, str(e.value)), str(e.value)
        finally:
            await plugin.aclose()


class _EncodeHeaders(SimplePlugin):
    """A client plugin that turns header encoding back on."""

    def __init__(self) -> None:
        super().__init__("encode-headers")

    def configure_client(self, config: ClientConfig) -> ClientConfig:
        config = super().configure_client(config)
        config["header_codec_behavior"] = HeaderCodecBehavior.CODEC
        return config


@pytest.mark.temporal
async def test_a_client_plugin_after_kontra_cannot_undo_the_converter(
    dev_server: DevServer, tmp_path: Path
) -> None:
    """In a worker main of the seller's own, plugin order is theirs: a client plugin after
    Kontra has the last word on the client. One that put temporalio's failure converter
    back would write failure messages to history in plaintext (the codec alone was
    checked, and passed); offloading storage and header encoding likewise. The worker is
    refused when it is built; with Kontra last, the same worker builds."""
    async with Marketplace(dev_server=dev_server) as mp:
        acme = await mp.party("acme")
        plugin = KontraPlugin(seller.bind(write_offering(tmp_path)), session=acme.session)
        await plugin.prepare()
        try:
            service = mp.client(acme.namespace).service_client
            cases = [
                (
                    SimplePlugin(
                        "plaintext-failures",
                        data_converter=lambda dc: dataclasses.replace(
                            dc, failure_converter_class=DefaultFailureConverter
                        ),
                    ),
                    r"failure converter is DefaultFailureConverter, which writes failure "
                    r"messages in plaintext",
                ),
                (
                    SimplePlugin(
                        "offload-everything",
                        data_converter=lambda dc: dataclasses.replace(
                            dc, external_storage=_storage(0)
                        ),
                    ),
                    r"external storage offloads payloads from 0 bytes",
                ),
                (_EncodeHeaders(), r"encodes headers"),
            ]
            for after, said in cases:
                client = Client(service, plugins=[plugin, after])
                assert client.data_converter.payload_codec is plugin.codec  # what was checked
                with pytest.raises(
                    ServeError, match=rf"ran after Kontra changed the client.*{said}"
                ):
                    temporalio.worker.Worker(
                        client, task_queue="x", workflows=[UpperFlow], activities=[shout, where]
                    )
            built = temporalio.worker.Worker(
                Client(service, plugins=[plugin]),
                task_queue="x",
                workflows=[UpperFlow],
                activities=[shout, where],
            )
            assert built.task_queue == plugin.offerings[0].offering_id.task_queue
        finally:
            await plugin.aclose()


# --------------------------------------------------------------------------- the graph page


async def test_the_graph_page_is_drawn_under_a_dev_profile_when_the_contract_changed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Decisions I35: once the hash lock passed, a dev profile's worker draws the static
    graph of each workflow offering whose stored one was drawn from another contract hash,
    or that has none, from the project's directory; a non-dev profile (production has no
    graph page), a plugin that knows no project, and a home without a profile draw none."""
    from types import SimpleNamespace

    started: list[_graphs.Refresh] = []

    def start(self: _graphs.Refresh) -> None:
        started.append(self)

    monkeypatch.setattr(_graphs.Refresh, "start", start)
    bound = seller.bind(write_offering(tmp_path))
    home = tmp_path / "home"
    home.mkdir()
    (home / PROFILE_FILE).write_text("{}")
    hashes = {bound.key: bound.contract_hash()}
    dev = SimpleNamespace(dev=True, home=home)
    plugin = KontraPlugin(bound, project=tmp_path)

    async def draw(profile: Any, of: KontraPlugin = plugin) -> _graphs.Refresh | None:
        return await of._draw_graphs(profile, hashes)  # pyright: ignore[reportPrivateUsage]

    assert await draw(SimpleNamespace(dev=False, home=home)) is None
    assert await draw(dev, KontraPlugin(bound)) is None
    refresh = await draw(dev)  # nothing stored yet
    assert refresh is not None
    assert started == [refresh]
    queue = bound.offering_id.task_queue
    assert refresh.wanted == (_graphs.Wanted(bound.key, queue, hashes[bound.key]),)
    assert (refresh.home, refresh.project) == (home, tmp_path)
    g: dict[str, Any] = {"workflow": {}, "nodes": [], "edges": []}

    def drawn(contract_hash: str) -> None:
        _graphs.save_static(
            home, queue, offering=bound.key, contract_hash=contract_hash, runs=8, seed=0, graph=g
        )

    drawn(hashes[bound.key])
    assert await draw(dev) is None  # current: no child
    drawn("sha256:" + "0" * 64)
    assert await draw(dev) is not None  # drawn from another contract
    (home / PROFILE_FILE).unlink()
    assert await draw(dev) is None  # nowhere the page reads (an API key's session)
    assert len(started) == 2


@pytest.mark.temporal
async def test_a_dev_worker_serves_while_its_graph_fails_to_draw(
    dev_server: DevServer, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """The refresh never holds up nor refuses serving: here its child cannot even load the
    project, the worker serves, and the failure is one WARNING."""
    caplog.set_level(logging.INFO, logger="kontra.serve")

    def warned() -> list[str]:
        return [
            r.getMessage()
            for r in caplog.records
            if r.levelno == logging.WARNING and "static graph" in r.getMessage()
        ]

    async with Marketplace(dev_server=dev_server) as mp:
        acme = await mp.party("acme")
        worker = Worker(offerings=[seller.bind(write_offering(tmp_path))], activities=[shout])
        worker.project = tmp_path / "not-a-project"
        async with mp.serve(worker, acme):
            assert worker.serving
            async with asyncio.timeout(120):
                while not warned():  # noqa: ASYNC110 (a log line, no event)
                    await asyncio.sleep(0.1)
            assert worker.serving
        [warning] = warned()
        assert "was not drawn: the mock runs exited with status 1 (error: no worker.py" in (warning)


def test_the_plugin_is_exported_lazily() -> None:
    """``import kontra`` loads no Temporal; ``kontra.KontraPlugin`` (and
    ``kontra.serve.KontraPlugin``) loads the serving stack on first use."""
    import subprocess
    import sys

    probe = textwrap.dedent(
        """
        import sys
        import kontra
        assert "temporalio" not in sys.modules, "import kontra loaded temporalio"
        from kontra import KontraPlugin
        import kontra.serve
        assert kontra.serve.KontraPlugin is KontraPlugin
        print(KontraPlugin.__module__)
        """
    )
    out = subprocess.run(
        [sys.executable, "-c", probe], capture_output=True, text=True, check=True
    ).stdout
    assert out.split() == ["kontra.plugin"]


@pytest.mark.parametrize(
    "gone", ["workflow", "activity", "RetryPolicy", "ApplicationError", "Page"]
)
def test_the_facades_names_are_gone_with_a_fix_it(gone: str) -> None:
    import kontra

    with pytest.raises(
        AttributeError, match=rf"`kontra\.{gone}` is gone: `from (temporalio|kontra)"
    ):
        getattr(kontra, gone)
    assert set(kontra.__all__) == {"KontraPlugin", "Worker", "__version__", "serve"}
