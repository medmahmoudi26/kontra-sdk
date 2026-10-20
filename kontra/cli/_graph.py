"""``kontra graph``: the static graph of worker.py's workflows, from mock runs (decisions
I27, I35; kontra-sdk#42, #56).

A hidden command: the graph page's static graphs are drawn by ``kontra serve`` under a dev
profile, which runs this command in a child process when an offering's contract changed
(:class:`kontra._graphs.Refresh`). It stays, with its flags, as that child's entry point and
for the examples' README graphs (``tests/graph/test_cli.py``), never in ``kontra --help``.

Temporal keeps no definition of a workflow, only what each run did (I26), so the graph of
code nobody has run yet comes from runs nobody pays for: :mod:`kontra.cli._mock` runs each
bound offering's workflow ``--runs`` times in Temporal's time-skipping test server, every
activity answered with a value of the type it declares (:mod:`kontra._sample`, seeded, so
the same command prints the same graph), and unites the runs' graphs. The graph is printed
as one line per edge, as Mermaid (``--mermaid``) or as JSON (``--json``). On stderr, and
advisory (the exit status stays 0): runs that did not close, return types no valid value was
found for, and, once for the worker, the activities it registers that no run of any
offering scheduled.

With a profile in ``$KONTRA_HOME``, each offering's graph is also saved where ``kontra dev``'s
graph page (#43) reads it, with the contract hash of the code it was drawn from
(:func:`kontra._graphs.save_static`): that page is served by a process that has no access to
the seller's project, and paints a live run on this graph.
"""

from __future__ import annotations

import contextlib
import json
import signal
import threading
from collections.abc import Generator
from pathlib import Path
from types import FrameType
from typing import TYPE_CHECKING, Any

import click

from kontra._graphs import GRAPHS_DIR, load_static, read_static, save_static, static_path
from kontra.cli._common import emit_json, fail, read_input
from kontra.cli._seller import _DIR, _pick

if TYPE_CHECKING:
    from kontra.cli._mock import Explored
    from kontra.graph import Graph

__all__ = ["GRAPHS_DIR", "graph", "load_static", "read_static", "save_static", "static_path"]

_START, _END = "start", "end"  # kontra.graph.START, END, without importing it for --help
#: What a project that runs an OpenAI agent imports: its own dependency
#: (``temporalio-openai-agents``), which ``kontra graph`` needs to run it.
_AGENT_MODULES = ("agents", "openai")
_AGENT_DEPENDENCY = "temporalio-openai-agents"


# --------------------------------------------------------------------------- signals


@contextlib.contextmanager
def _stopped_like_ctrl_c() -> Generator[None]:
    """SIGTERM and SIGHUP (a supervisor, an IDE's stop button, ``subprocess.terminate()``, a
    closed terminal) take Ctrl-C's way out while the runs go: the test server is shut down
    on the way, instead of being left running, orphaned, on a port of every interface."""
    if threading.current_thread() is not threading.main_thread():
        yield
        return

    def interrupted(signum: int, frame: FrameType | None) -> None:
        handler = signal.getsignal(signal.SIGINT)  # asyncio.run's own while it runs
        if callable(handler):
            handler(signal.SIGINT, frame)
        else:  # SIGINT ignored (a background job): end anyway
            raise KeyboardInterrupt

    signals = [getattr(signal, name) for name in ("SIGTERM", "SIGHUP") if hasattr(signal, name)]
    before = {sig: signal.signal(sig, interrupted) for sig in signals}
    try:
        yield
    finally:
        for sig, handler in before.items():
            signal.signal(sig, handler)


# --------------------------------------------------------------------------- printing


def _edges(g: Graph) -> list[tuple[str, str]]:
    """The graph's edges, from ``start`` down in node order, ``end`` last."""
    order = {_START: -1, _END: len(g["nodes"])}
    order.update({n["id"]: i for i, n in enumerate(g["nodes"])})
    return sorted(
        ((e["from"], e["to"]) for e in g["edges"]),
        key=lambda e: (order.get(e[0], len(order)), order.get(e[1], len(order)), e),
    )


def _warn(message: str) -> None:
    click.echo(f"warning: {message}", err=True)


def _which(indexes: list[int]) -> str:
    return ("run " if len(indexes) == 1 else "runs ") + ", ".join(map(str, indexes))


def _mock_runs(n: int) -> str:
    return f"{n} mock run" if n == 1 else f"{n} mock runs"


def _inputs(bound: Any, given: object, source: str | None, runs: int, seed: int) -> list[Any]:
    """Each run's input for ``bound``: ``given`` (from ``--input``), checked against the
    workflow's input type, else one sampled in each run's mode."""
    from pydantic import TypeAdapter, ValidationError

    from kontra._sample import Unsampleable, draw

    name = bound.input_type.__name__
    if given is None:
        try:
            return [draw(bound.input_type, i, seed) for i in range(runs)]
        except Unsampleable as e:
            fail(f"cannot sample a valid {name} ({e}); give one with --input FILE")
    try:
        TypeAdapter(bound.input_type).validate_json(json.dumps(given))
    except ValidationError as e:
        problems = "; ".join(
            f"{'.'.join(map(str, err['loc'])) or 'the input'}: {err['msg']}" for err in e.errors()
        )
        fail(f"the input {source} is not a valid {name}: {problems}")
    return [given] * runs


def _never_scheduled(explored: list[Explored], every_offering: bool) -> None:
    """The worker's activities no run scheduled: once for the worker, since one offering's
    runs never schedule another's activities."""
    if not explored or not explored[0].never_scheduled:
        return
    offerings = ", ".join(e.offering for e in explored)
    runs = sum(e.runs for e in explored)
    _warn(
        f"{offerings}: never scheduled in {_mock_runs(runs)}: "
        f"{', '.join(explored[0].never_scheduled)} — branches on timers, signals or inputs "
        "the runs did not reach"
        + ("" if every_offering else ", or the worker's offerings --offering left out")
    )


def _warnings(e: Explored, timeout: float) -> None:
    if e.timed_out:
        _warn(
            f"{e.offering}: {_which(e.timed_out)} of {e.runs} waited for what no timer brings "
            "(a signal, or a wait_condition without a timeout), so the test server timed "
            "them out; what they did until then is in the graph"
        )
    if e.deadline:
        _warn(
            f"{e.offering}: {_which(e.deadline)} of {e.runs} still open after {timeout:g}s "
            f"{'was' if len(e.deadline) == 1 else 'were'} terminated (a loop on an "
            "activity's result, which the mock answers the same way every time in a run, or "
            "a very long loop); what they did until then is in the graph"
        )
    for run, said in e.failing.items():
        _warn(
            f"{e.offering}: run {run}'s workflow task kept failing on the mock values "
            f"({said}); the run was terminated"
        )
    for name, why in e.unsampled.items():
        _warn(
            f"{e.offering}: no valid return value of {name} could be sampled ({why}); the "
            "stub returned null"
        )


def _chosen(bound: tuple[Any, ...], only: tuple[str, ...]) -> list[Any]:
    """The offerings ``--offering`` names (each one at least once), in the worker's order;
    every bound offering without it."""
    if not only:
        return list(bound)
    picked = [b for name in only for b in _pick(bound, name)]
    return [b for b in bound if any(b is p for p in picked)]


@click.command(hidden=True)
@_DIR
@click.option(
    "--offering",
    "only",
    metavar="NAME@VERSION",
    multiple=True,
    help="Only this offering of the worker (default: every bound offering); repeatable.",
)
@click.option(
    "--runs",
    default=8,
    show_default=True,
    type=click.IntRange(min=1),
    help="Mock runs per offering: run 0 draws minimal values, 1 maximal, 2 large inputs, "
    "the rest random ones.",
)
@click.option(
    "--seed",
    default=0,
    show_default=True,
    type=int,
    help="The seed of the random runs: the same seed draws the same values.",
)
@click.option(
    "--input",
    "source",
    metavar="FILE|-",
    help="Start every run with this input (a JSON file, - for stdin) instead of sampled ones. "
    "The test server listens on every interface, unauthenticated, while the runs go: anyone "
    "who can reach this machine can read it there.",
)
@click.option(
    "--timeout",
    default=5.0,
    show_default=True,
    type=click.FloatRange(min=0, min_open=True),
    help="Wall-clock seconds a run may take; one still open then is terminated.",
)
@click.option("--json", "as_json", is_flag=True, help="Machine-readable output.")
@click.option("--mermaid", is_flag=True, help="A Mermaid flowchart per offering.")
@click.option("--no-save", is_flag=True, help="Do not save the graph for kontra dev's graph page.")
def graph(
    directory: Path,
    only: tuple[str, ...],
    runs: int,
    seed: int,
    source: str | None,
    timeout: float,
    as_json: bool,
    mermaid: bool,
    no_save: bool,
) -> None:
    """Print the static graph of worker.py's workflows, from mock runs in Temporal's
    time-skipping test server (every activity answered with a sampled value of its type)."""
    from kontra.cli._common import run
    from kontra.cli._mock import explore
    from kontra.cli._project import load_worker
    from kontra.graph import mermaid as to_mermaid
    from kontra.profile import PROFILE_FILE, kontra_home

    if as_json and mermaid:
        fail("--json or --mermaid, not both")
    given = read_input(source) if source is not None else None
    try:
        with load_worker(directory) as worker:
            chosen = _chosen(worker.offerings, only)
            hashes = {b.key: b.contract_hash() for b in chosen}
            every_offering = len(chosen) == len(worker.offerings)
            inputs = [_inputs(b, given, source, runs, seed) for b in chosen]
            with _stopped_like_ctrl_c():
                explored = run(lambda: explore(worker, chosen, inputs, seed=seed, deadline=timeout))
    except ModuleNotFoundError as err:
        # An agent workflow's project imports `agents` (and worker.py Temporal's
        # integration): without the seller's dependency installed here it cannot even be
        # imported, so it cannot be run either.
        top = (err.name or "").partition(".")[0]
        if top in _AGENT_MODULES or (err.name or "").startswith("temporalio.openai_agents"):
            fail(
                f"{directory} runs an agent: install the project's own dependency here "
                f"(pip install {_AGENT_DEPENDENCY}) ({err})"
            )
        raise

    if as_json:
        emit_json(
            [
                {
                    "offering": found.offering,
                    "runs": found.runs,
                    "graph": found.graph,
                    "never_scheduled": found.never_scheduled,
                }
                for found in explored
            ]
        )
    for i, found in enumerate(explored):
        if mermaid:
            click.echo(f"%% {found.offering}")
            click.echo(to_mermaid(found.graph), nl=False)
        elif not as_json:
            if i:
                click.echo()
            click.echo(f"{found.workflow} ({found.offering}): {_mock_runs(found.runs)}")
            for a, b in _edges(found.graph):
                click.echo(f"  {a} → {b}")
        _warnings(found, timeout)
    _never_scheduled(explored, every_offering)
    home = kontra_home()
    if no_save or not (home / PROFILE_FILE).is_file():
        return
    for found in explored:
        save_static(
            home,
            found.task_queue,
            offering=found.offering,
            contract_hash=hashes[found.offering],
            runs=found.runs,
            seed=seed,
            graph=found.graph,
        )
    click.echo(f"static graph saved for kontra dev's graph page ({home / GRAPHS_DIR})", err=True)
