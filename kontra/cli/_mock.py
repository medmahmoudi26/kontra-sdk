"""``kontra graph``'s mock runs (decisions I27): a seller's workflow run in Temporal's
time-skipping test server, every activity answered by a stub, and the runs' histories read
back into one static graph (:mod:`kontra.graph`).

Nothing in the seller's code changes. The worker ``worker.py`` builds is read, not run: its
bound workflows, its extra workflows (the children they start), its registered activities
and its plugins. A plain ``temporalio.worker.Worker`` per bound offering, on that offering's
own task queue, serves the workflows, ``kontra.batch.v1`` (the optional
:mod:`kontra.contrib.parallel` helper's child, whether or not the seller's plugins add it)
and ONE dynamic activity, which answers every activity type nothing else registered -- the
seller's, a ``pages=`` activity, a tool -- with a value of the type the seller's activity of
that name declares (:mod:`kontra._sample`), or null for a name the worker does not register.
A workflow whose worker has Temporal's ``OpenAIAgentsPlugin`` among its ``plugins=`` (an
agent workflow; the seller's own dependency, ``temporalio-openai-agents``) gets that
integration with a scripted model instead (:mod:`kontra.cli._scripted`); no other seller
plugin is run. The data converter is the payload converter ``kontra serve`` ends up with for
such a worker, without the codec: nothing is encrypted. An offering bound to an agent's
handler (one hire, one turn: :mod:`kontra.serve.turn`) is not drawn yet (kontra-sdk#56). The test server cannot be bound to the loopback (it takes a port, nothing else),
so it listens on every interface, unauthenticated, while the command runs: anyone who can
reach the machine can read the runs' inputs and results -- an ``--input`` given too -- and
start workflows the mock worker runs. It is downloaded once into a directory only this user
can write (:func:`server_dir`), never the shared temp directory temporalio defaults to,
where whoever creates that file name first chooses what runs.

Runs are sequential, in one test server: run ``i`` is workflow ``kontra-graph-<i>``, started
with the given input or one sampled in run ``i``'s mode, and waited for up to a wall-clock
deadline. Timers skip, and so does a wait no timer ends (a signal, a ``wait_condition``
without a timeout): the test server skips to the run's timeout and times it out. A run
still open at the deadline (a loop on an activity's result, which the stub answers the
same way every time in a run, or a very long one), or whose workflow task fails on the mock
values (it fails again on every retry), is terminated. Either way its history up to
there still carries its structure. Each run's history is parsed with every child's (the
first run of each, found by the ids its events name, never built), and the graphs are
united.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import stat
import sys
import warnings
from collections.abc import Callable, Generator, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import temporalio.worker
from temporalio import activity
from temporalio.api.enums.v1 import EventType, WorkflowTaskFailedCause
from temporalio.client import Client, WorkflowHistory
from temporalio.common import RawValue
from temporalio.contrib.pydantic import PydanticPayloadConverter
from temporalio.converter import DataConverter, PayloadConverter
from temporalio.runtime import LoggingConfig, Runtime, TelemetryConfig, TelemetryFilter
from temporalio.service import RPCError
from temporalio.testing import WorkflowEnvironment

from kontra import graph
from kontra._sample import Unsampleable, draw
from kontra.cli._common import fail
from kontra.contrib.parallel import _Batch
from kontra.serve.worker import _dedupe, workflow_runner

__all__ = ["Explored", "explore", "server_dir"]

#: How often a run still open is looked at for a workflow task that keeps failing.
_POLL_S = 0.1
#: The most child histories read for one run (its children's children included).
_CHILDREN_MAX = 1000
#: The longest failure message a warning quotes.
_SAID = 200
_ACTIVITY_KINDS = ("activity", "local_activity")
#: The module temporalio's sandbox warns from.
_SANDBOX = r"temporalio\.worker\.workflow_sandbox\..*"
#: Where the test server is kept, under the user's cache directory.
_SERVER_DIR = ("kontra", "temporal-test-server")


@dataclass
class Explored:
    """What the mock runs of one bound offering showed."""

    offering: str
    task_queue: str
    #: The workflow's Temporal type.
    workflow: str
    runs: int
    #: :func:`kontra.graph.union` of the runs' graphs.
    graph: graph.Graph
    #: Activities the worker registers that no run of any offering explored (nor any of
    #: their children) scheduled: the same list for every offering of one command.
    never_scheduled: list[str]
    #: Runs the test server timed out: they waited for what no timer brings.
    timed_out: list[int] = field(default_factory=list[int])
    #: Runs terminated at the deadline.
    deadline: list[int] = field(default_factory=list[int])
    #: Runs terminated because their workflow task kept failing: what it failed with.
    failing: dict[int, str] = field(default_factory=dict[int, str])
    #: Activities whose return value could not be sampled (the stub returned null): why.
    unsampled: dict[str, str] = field(default_factory=dict[str, str])
    #: Each run's status as its history ends (``terminated`` once it was terminated).
    statuses: list[str | None] = field(default_factory=list[str | None])


class _Answers:
    """What the stub answers: a value of the declared return type, drawn in the mode of
    the run in progress."""

    def __init__(self, registered: Mapping[str, Any], seed: int) -> None:
        self.registered = registered
        self.seed = seed
        self.run = 0
        self.unsampled: dict[str, str] = {}

    def returned(self, name: str) -> Any:
        definition = self.registered.get(name)
        if definition is None or definition.ret_type is None:
            return None
        try:
            return draw(definition.ret_type, self.run, self.seed)
        except Unsampleable as e:
            self.unsampled[name] = str(e)
            return None


#: Where Temporal's OpenAI Agents integration lives (the seller's dependency).
_AGENTS_MODULE = "temporalio.openai_agents"


def uses_agents(plugins: object) -> bool:
    """Whether a worker's ``plugins=`` hold Temporal's ``OpenAIAgentsPlugin``. Read without
    importing anything: a plugin instance exists only once its module was imported."""
    module = sys.modules.get(_AGENTS_MODULE)
    plugin_type = getattr(module, "OpenAIAgentsPlugin", None)
    if not isinstance(plugin_type, type) or not isinstance(plugins, list | tuple):
        return False
    return any(isinstance(p, plugin_type) for p in plugins)  # pyright: ignore[reportUnknownVariableType]


def _payload_converter(plugins: object) -> type[PayloadConverter]:
    """The payload converter ``kontra serve``'s client ends up with: the agents plugin's
    (it decodes the model activity's input leniently), else pydantic."""
    if not uses_agents(plugins):
        return PydanticPayloadConverter
    from temporalio.openai_agents import OpenAIPayloadConverter

    return OpenAIPayloadConverter


def _stub(answers: _Answers) -> Callable[..., Any]:
    @activity.defn(dynamic=True)
    async def stub(args: Sequence[RawValue]) -> Any:
        # Drawn off the event loop: a large value must not hold up the runs' deadline.
        return await asyncio.to_thread(answers.returned, activity.info().activity_type)

    return stub


def _registered(activities: Sequence[Any]) -> dict[str, Any]:
    """The worker's activities by Temporal name (the dynamic one has none)."""
    out: dict[str, Any] = {}
    for fn in activities:
        definition = getattr(fn, "__temporal_activity_definition", None)
        name = getattr(definition, "name", None)
        if isinstance(name, str) and name:
            out[name] = definition
    return out


def _started_children(history: WorkflowHistory) -> list[tuple[str, str]]:
    """Each child the run started: its workflow id and first run id, as its
    ChildWorkflowExecutionStarted event names them (one that never started has no history)."""
    return [
        (a.workflow_execution.workflow_id, a.workflow_execution.run_id)
        for e in history.events
        if e.event_type == EventType.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED
        for a in (e.child_workflow_execution_started_event_attributes,)
    ]


def _failing(history: WorkflowHistory) -> str | None:
    """What the run's last workflow task failed with, when the workflow's own code failed
    it (a mock value it could not handle): the mock values are the same on every retry, so
    it never recovers. None while the last one completed, or failed for the server's own
    reasons (a signal arriving mid-task), which a retry gets past."""
    last = None
    for e in history.events:
        if e.event_type in (
            EventType.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
            EventType.EVENT_TYPE_WORKFLOW_TASK_FAILED,
        ):
            last = e
    if last is None or last.event_type != EventType.EVENT_TYPE_WORKFLOW_TASK_FAILED:
        return None
    failed = last.workflow_task_failed_event_attributes
    unhandled = WorkflowTaskFailedCause.WORKFLOW_TASK_FAILED_CAUSE_WORKFLOW_WORKER_UNHANDLED_FAILURE
    if failed.cause != unhandled:
        return None
    said = (failed.failure.message or "no message").splitlines()[0]
    return said if len(said) <= _SAID else said[: _SAID - 3] + "..."


async def _children(client: Client, history: WorkflowHistory) -> dict[str, Any]:
    """Every descendant's history (first run), by workflow id."""
    found: dict[str, Any] = {}
    queue = _started_children(history)
    while queue and len(found) < _CHILDREN_MAX:
        workflow_id, run_id = queue.pop(0)
        if workflow_id in found:
            continue
        child = await client.get_workflow_handle(workflow_id, run_id=run_id).fetch_history()
        found[workflow_id] = child.to_json_dict()
        queue.extend(_started_children(child))
    return found


def server_dir() -> Path:
    """Where the time-skipping test server is downloaded to and run from:
    ``$XDG_CACHE_HOME/kontra/temporal-test-server`` (``~/.cache`` without it), made 0700.
    temporalio's default is the shared temp directory, where it runs whatever file sits at
    its predictable name: on a shared machine, whoever creates that name first chooses
    what runs as the user. So the directory must be the user's own, a real directory that
    no one else can write, and so must everything in it; anything else is refused."""
    cache = os.environ.get("XDG_CACHE_HOME") or ""
    base = Path(cache) if os.path.isabs(cache) else Path.home() / ".cache"
    path = base.joinpath(*_SERVER_DIR)
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    if not hasattr(os, "getuid"):
        return path
    uid = os.getuid()
    found = os.lstat(path)
    if not stat.S_ISDIR(found.st_mode) or found.st_uid != uid:
        fail(
            f"{path} is not a directory of yours: kontra graph runs the test server it holds, "
            "so no one else may own it"
        )
    if stat.S_IMODE(found.st_mode) & 0o077:
        os.chmod(path, 0o700)
    for entry in os.scandir(path):
        held = entry.stat(follow_symlinks=False)
        if held.st_uid != uid or held.st_mode & 0o022 or stat.S_ISLNK(held.st_mode):
            fail(
                f"{entry.path} is not yours alone (a link, or someone else's, or writable by "
                "others): kontra graph would run it; remove it and the test server is "
                "downloaded again"
            )
    return path


@dataclass
class _Run:
    graph: graph.Graph
    #: Activity names it and its children scheduled.
    scheduled: set[str]
    #: Terminated at the deadline.
    deadline: bool
    #: Terminated because its workflow task kept failing: what it failed with.
    failing: str | None


async def _wait(handle: Any, seconds: float) -> tuple[bool, str | None]:
    """Wait up to ``seconds`` for the run to close: whether the deadline passed first, and
    what its workflow task keeps failing with, if it does. The result is awaited the whole
    time: the time-skipping server skips time only while a result is awaited."""
    loop = asyncio.get_running_loop()
    end = loop.time() + seconds
    result = asyncio.ensure_future(handle.result())
    try:
        while not result.done():
            left = end - loop.time()
            if left <= 0:
                return True, None
            await asyncio.wait([result], timeout=min(_POLL_S, left))
            if not result.done():
                failing = _failing(await handle.fetch_history())
                if failing is not None:
                    return False, failing
        return False, None
    finally:
        result.cancel()
        await asyncio.gather(result, return_exceptions=True)  # a failed run is a run too


async def _run(client: Client, bound: Any, index: int, arg: Any, seconds: float) -> _Run:
    handle = await client.start_workflow(
        bound.run_fn, arg, id=f"kontra-graph-{index}", task_queue=bound.offering_id.task_queue
    )
    deadline, failing = await _wait(handle, seconds)
    if deadline or failing is not None:
        with contextlib.suppress(RPCError):  # it closed in the meantime
            await handle.terminate(reason="kontra graph: the mock run did not close")
    first = client.get_workflow_handle(handle.id, run_id=handle.first_execution_run_id)
    history = await first.fetch_history()
    children = await _children(client, history)
    parsed = graph.parse(
        history.to_json_dict(),
        workflow_id=handle.id,
        run_id=handle.first_execution_run_id,
        children=children,
    )
    scheduled = {
        n["name"]
        for g in (parsed, *(graph.parse(h) for h in children.values()))
        for n in g["nodes"]
        if n["kind"] in _ACTIVITY_KINDS
    }
    return _Run(parsed, scheduled, deadline, failing)


@contextlib.contextmanager
def _quiet() -> Generator[None]:
    """temporalio's warnings while the runs go: a failing workflow task's traceback on
    every retry (what it says is in the warnings ``kontra graph`` prints), and the sandbox's
    note of a module a workflow imported late (an agent run's, every run)."""
    logger = logging.getLogger("temporalio")
    level = logger.level
    logger.setLevel(logging.CRITICAL)
    try:
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=UserWarning, module=_SANDBOX)
            yield
    finally:
        logger.setLevel(level)


def _quiet_runtime() -> Runtime:
    """A runtime whose core logs errors only: the test server is not the server a seller
    serves on, and what the core says of its capabilities is noise here."""
    quiet = TelemetryFilter(core_level="ERROR", other_level="ERROR")
    return Runtime(telemetry=TelemetryConfig(logging=LoggingConfig(filter=quiet)))


async def explore(
    worker: Any,
    bindings: Sequence[Any],
    inputs: Sequence[Sequence[Any]],
    *,
    seed: int,
    deadline: float,
) -> list[Explored]:
    """The mock runs of each of ``bindings`` (the worker's bound offerings), run ``i`` of
    binding ``k`` started with ``inputs[k][i]`` and given ``deadline`` seconds; one test
    server for all of them."""
    for bound in bindings:
        if bound.kind == "handler":
            fail(
                f"kontra graph does not draw an agent's handler yet: {bound.key} is "
                f"{bound.agent.__name__}.{bound.handler} (kontra-sdk#56)"
            )
    options = worker.options
    plugins = options.get("plugins")
    registered = _registered(options.get("activities") or ())
    answers = _Answers(registered, seed)
    converter = DataConverter(payload_converter_class=_payload_converter(plugins))
    extra = list(options.get("workflows") or ())
    worker_plugins: list[temporalio.worker.Plugin] = []
    if uses_agents(plugins):
        from kontra.cli._scripted import scripted_plugin

        worker_plugins.append(scripted_plugin(lambda: answers.run, seed))
    out: list[Explored] = []
    scheduled: list[set[str]] = []
    where = server_dir()
    with _quiet():
        try:
            env = await WorkflowEnvironment.start_time_skipping(
                data_converter=converter, runtime=_quiet_runtime(), download_dest_dir=str(where)
            )
        except Exception as e:  # first use, offline: the download failed
            fail(
                "cannot start Temporal's time-skipping test server, which temporalio "
                f"downloads into {where} on first use: {e}"
            )
        async with env:
            for bound, args in zip(bindings, inputs, strict=True):
                task_queue = bound.offering_id.task_queue
                runs: list[_Run] = []
                answers.unsampled = {}
                async with temporalio.worker.Worker(
                    env.client,
                    task_queue=task_queue,
                    workflows=_dedupe([bound.workflow, *extra, _Batch]),
                    activities=[_stub(answers)],
                    workflow_runner=workflow_runner(),
                    plugins=worker_plugins,
                ):
                    for i, arg in enumerate(args):
                        answers.run = i
                        runs.append(await _run(env.client, bound, i, arg, deadline))
                union = graph.union(r.graph for r in runs)
                scheduled.append(set[str]().union(*(r.scheduled for r in runs)))
                statuses = [r.graph["workflow"]["status"] for r in runs]
                out.append(
                    Explored(
                        offering=bound.key,
                        task_queue=task_queue,
                        workflow=union["workflow"]["type"] or bound.workflow.__name__,
                        runs=len(runs),
                        graph=union,
                        never_scheduled=[],
                        timed_out=[i for i, status in enumerate(statuses) if status == "timed_out"],
                        deadline=[i for i, r in enumerate(runs) if r.deadline],
                        failing={i: r.failing for i, r in enumerate(runs) if r.failing},
                        unsampled=dict(sorted(answers.unsampled.items())),
                        statuses=statuses,
                    )
                )
    # The worker's activities are every offering's: one offering's runs never schedule
    # another's, so what counts is what no offering's runs scheduled.
    never = sorted(set(registered) - set[str]().union(*scheduled))
    for found in out:
        found.never_scheduled = never
    return out
