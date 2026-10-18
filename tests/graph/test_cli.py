"""``kontra graph`` (decisions I27; kontra-sdk#42): worker.py's workflows as a graph, from mock
runs in Temporal's time-skipping test server.

#42's "done when", as far as this command answers for it: every seller example's graph is
the one its README documents (and ``examples/recon`` is refused, with a clear line: it is
buyer code, with no worker.py); ``examples/sweep``'s fan-out is one parallel group, its batch
one child node with no continue-as-new chain, and its ``if`` on what the batch found shows
both branches from two runs; never-scheduled activities are listed with exit status 0. Then
the other warnings, ``--json``, ``--input``, and the static graph ``kontra dev``'s page reads.
The projects under ``projects/`` are seller projects written for these tests (``branches``
has the same three shapes, and an activity only a signal reaches); the command runs
in-process (``CliRunner``)."""

from __future__ import annotations

import asyncio
import importlib.util
import json
import os
import re
import shutil
import stat
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner, Result
from temporalio.testing import WorkflowEnvironment

from kontra import activity as kontra_activity
from kontra.cli import cli
from kontra.cli._graph import load_static, static_path
from kontra.cli._project import load_worker

pytestmark = pytest.mark.temporal

ROOT = Path(__file__).resolve().parents[2]
EXAMPLES = ROOT / "examples"
PROJECTS = Path(__file__).resolve().parent / "projects"
AGENTS = importlib.util.find_spec("temporalio.openai_agents") is not None

#: Every seller example: a directory with a worker.py (examples/recon is buyer code).
SELLERS = sorted(p.parent.name for p in EXAMPLES.glob("*/worker.py"))
#: The one ```mermaid block of a README.
_MERMAID = re.compile(r"^```mermaid\n(.*?)^```$", re.MULTILINE | re.DOTALL)


def readme_graph(example: str) -> str:
    """The graph ``examples/<example>/README.md`` documents."""
    [block] = _MERMAID.findall((EXAMPLES / example / "README.md").read_text(encoding="utf-8"))
    return block


@pytest.fixture(scope="module", autouse=True)
def test_server() -> None:
    """Temporal's time-skipping test server, which temporalio downloads into the temp
    directory on first use: without it, and without the network, these tests are skipped."""

    async def start() -> None:
        async with await WorkflowEnvironment.start_time_skipping():
            pass

    try:
        asyncio.run(start())
    except Exception as e:
        pytest.skip(f"no time-skipping test server: {e}")


@pytest.fixture(autouse=True)
def only_the_projects_activities(monkeypatch: pytest.MonkeyPatch) -> None:
    """``serve()`` registers every kontra activity defined in the process: in the command's
    own process, the project's. This one holds every test module's too (``tests/parallel``
    defines its activities as it is collected), which would be registered on every worker
    and reported never scheduled. Each test starts with none defined, as ``kontra graph``
    does; ``load_worker`` imports the project afresh, which defines its own."""
    monkeypatch.setattr(kontra_activity, "_DEFINED", {})


@pytest.fixture
def home(tmp_path: Path) -> Path:
    """A ``KONTRA_HOME`` without a profile: nothing is saved."""
    return tmp_path / "home"


def kontra(home: Path, *args: str, stdin: str | None = None) -> Result:
    env: dict[str, str | None] = {
        "KONTRA_HOME": str(home),
        "KONTRA_GATE": None,
        "KONTRA_API_KEY": None,
        "KONTRA_DEBUG": None,
    }
    return CliRunner().invoke(cli, list(args), input=stdin, env=env, catch_exceptions=True)


def ok(result: Result) -> Result:
    assert result.exit_code == 0, (
        f"exit {result.exit_code}\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}\n"
        f"{result.exception!r}"
    )
    return result


def graph_of(home: Path, directory: Path, *args: str, stdin: str | None = None) -> dict[str, Any]:
    [found] = json.loads(
        ok(kontra(home, "graph", "--dir", str(directory), "--json", *args, stdin=stdin)).stdout
    )
    return found["graph"]


def edges(g: dict[str, Any]) -> set[tuple[str, str]]:
    return {(e["from"], e["to"]) for e in g["edges"]}


def node(g: dict[str, Any], node_id: str) -> dict[str, Any]:
    [found] = [n for n in g["nodes"] if n["id"] == node_id]
    return found


# --------------------------------------------------------------------------- the examples


def test_every_seller_example_is_drawn() -> None:
    assert SELLERS == ["chef-agents", "chef-claude", "enrich", "hello", "legacy-actor", "sweep"]


@pytest.mark.parametrize("example", SELLERS)
def test_the_graph_of_every_seller_example_is_its_readmes(home: Path, example: str) -> None:
    """Golden: the README's ```mermaid block is what ``kontra graph --mermaid`` prints (8
    runs, seed 0). After a deliberate change, ``UPDATE_GOLDEN=1 pytest tests/graph/test_cli.py``
    rewrites the blocks; review the diff."""
    if example == "chef-agents" and not AGENTS:
        pytest.skip("the [agents] extra is not installed")
    result = ok(kontra(home, "graph", "--dir", str(EXAMPLES / example), "--mermaid"))
    assert result.stderr == ""  # every activity scheduled, every run closed
    assert not home.exists()  # no profile: nothing saved
    if os.environ.get("UPDATE_GOLDEN") == "1":
        readme = EXAMPLES / example / "README.md"
        text = readme.read_text(encoding="utf-8")
        [old] = _MERMAID.findall(text)
        readme.write_text(text.replace(old, result.stdout, 1), encoding="utf-8")
    assert result.stdout == readme_graph(example)


@pytest.mark.parametrize("runs", ["2", "4"])
def test_the_sweep_example_shows_a_fan_out_a_batch_and_both_branches(home: Path, runs: str) -> None:
    """examples/sweep, #42's three shapes: the two fetches are one parallel group (same
    group, the same predecessor); the batch is ONE child node, named from its history and
    labelled with the call, with no continue-as-new chain and none of its item activities
    in the parent's graph; and the ``if`` on what it found shows both branches from two
    runs (run 0's checks find nothing, run 1's find pages to score)."""
    g = graph_of(home, EXAMPLES / "sweep", "--runs", runs)
    robots, security_txt = node(g, "fetch_robots"), node(g, "fetch_security_txt")
    assert robots["group"] == security_txt["group"] != node(g, "plan")["group"]
    assert {s for s, t in edges(g) if t in ("fetch_robots", "fetch_security_txt")} == {"plan"}
    batch = "kontra.batch.v1[check_headers]"
    children = [n for n in g["nodes"] if n["kind"] == "child"]
    assert [(n["id"], n["label"]) for n in children] == [
        (batch, "execute_activities_in_parallel(check_headers)")
    ]
    assert {(s, t) for s, t in edges(g) if batch in (s, t)} == {
        ("fetch_robots", batch),
        ("fetch_security_txt", batch),
        (batch, "score"),
        (batch, "all_clear"),
    }  # no self edge: one node, not a chain of runs
    assert not any(n["name"] == "check_headers" for n in g["nodes"])
    assert {("score", "write_report"), ("all_clear", "write_report"), ("write_report", "end")} <= (
        edges(g)
    )


def test_recon_is_buyer_code_and_is_refused(home: Path) -> None:
    # examples/recon hires offerings from its own workflow: it binds none, so it has no
    # worker.py, and there is nothing to run.
    result = kontra(home, "graph", "--dir", str(EXAMPLES / "recon"))
    assert result.exit_code == 1
    assert result.stdout == ""
    assert result.stderr.startswith(f"error: no worker.py in {EXAMPLES / 'recon'}: run this from")


def test_the_text_output_is_an_edge_a_line(home: Path) -> None:
    result = ok(kontra(home, "graph", "--dir", str(EXAMPLES / "enrich"), "--runs", "3"))
    assert result.stdout == (
        "EnrichFlow (acme/enrich@1.2.0): 3 mock runs\n"
        "  start → fetch_batch\n"
        "  fetch_batch → fetch_batch\n"
        "  fetch_batch → write_records\n"
        "  write_records → end\n"
    )


# --------------------------------------------------------------------------- the shapes


def test_a_fan_out_is_one_parallel_group_and_a_batch_one_child(home: Path) -> None:
    g = graph_of(home, PROJECTS / "branches", "--runs", "4")
    a, b = node(g, "fetch_a"), node(g, "fetch_b")
    assert a["group"] == b["group"] != node(g, "plan")["group"]
    assert {s for s, t in edges(g) if t in ("fetch_a", "fetch_b")} == {"plan"}
    children = [n for n in g["nodes"] if n["kind"] == "child"]
    assert [(n["id"], n["label"]) for n in children] == [
        ("kontra.batch.v1[check]", "execute_activities_in_parallel(check)")
    ]  # one node, named from its history: no continue-as-new chain, no item activities
    assert ("fetch_a", "kontra.batch.v1[check]") in edges(g)
    assert ("fetch_b", "kontra.batch.v1[check]") in edges(g)
    assert ("kontra.batch.v1[check]", "classify") in edges(g)
    assert not any(n["name"] == "check" for n in g["nodes"])


@pytest.mark.parametrize("runs", ["2", "4"])
def test_an_if_on_an_activity_result_shows_both_branches(home: Path, runs: str) -> None:
    # Run 0 draws flag=false, run 1 flag=true.
    g = graph_of(home, PROJECTS / "branches", "--runs", runs)
    assert {("classify", "accept"), ("classify", "reject")} <= edges(g)
    assert {("accept", "wait"), ("reject", "wait"), ("wait", "end")} <= edges(g)
    assert node(g, "wait")["label"] == "wait 24h"  # the approval's timeout, skipped


def test_never_scheduled_activities_are_listed_and_the_exit_status_is_0(home: Path) -> None:
    # `ship` runs only after an approval, which only a signal brings.
    result = ok(kontra(home, "graph", "--dir", str(PROJECTS / "branches"), "--runs", "2"))
    assert result.stderr == (
        "warning: acme/branches@1.0.0: never scheduled in 2 mock runs: ship — branches on "
        "timers, signals or inputs the runs did not reach\n"
    )
    [found] = json.loads(
        ok(kontra(home, "graph", "--dir", str(PROJECTS / "branches"), "--json")).stdout
    )
    assert found["never_scheduled"] == ["ship"]


# --------------------------------------------------------------------------- output, input


def test_json_is_always_a_list_of_offerings(home: Path) -> None:
    result = ok(kontra(home, "graph", "--dir", str(EXAMPLES / "hello"), "--json", "--runs", "2"))
    [found] = json.loads(result.stdout)
    assert set(found) == {"offering", "runs", "graph", "never_scheduled"}
    assert (found["offering"], found["runs"], found["never_scheduled"]) == (
        "acme/hello@0.1.0",
        2,
        [],
    )
    g = found["graph"]
    assert set(g) == {"version", "workflow", "nodes", "edges"}
    assert (g["workflow"]["type"], g["workflow"]["task_queue"]) == ("HelloFlow", "acme/hello@0.1.0")
    assert edges(g) == {("start", "greet"), ("greet", "end")}
    assert node(g, "greet")["status"] is None  # structure only: union keeps no run's state


def test_json_and_mermaid_are_one_or_the_other(home: Path) -> None:
    result = kontra(home, "graph", "--dir", str(EXAMPLES / "hello"), "--json", "--mermaid")
    assert result.exit_code == 1
    assert result.stderr == "error: --json or --mermaid, not both\n"


def test_input_is_validated(home: Path, tmp_path: Path) -> None:
    bad = tmp_path / "bad.json"
    bad.write_text('{"then": "later"}')
    result = kontra(home, "graph", "--dir", str(PROJECTS / "stuck"), "--input", str(bad))
    assert result.exit_code == 1
    assert result.stderr == (
        f"error: the input {bad} is not a valid StuckInput: then: Input should be 'done', "
        "'loop', 'wait' or 'crash'\n"
    )
    result = kontra(home, "graph", "--dir", str(PROJECTS / "stuck"), "--input", "-", stdin="[]")
    assert result.exit_code == 1
    assert result.stderr == "error: the input - must be a JSON object, got list\n"
    # Every run starts with the input given: none of them reaches `after`.
    g = graph_of(home, PROJECTS / "stuck", "--input", "-", "--runs", "2", stdin='{"then": "done"}')
    assert edges(g) == {("start", "step"), ("step", "odd"), ("odd", "after"), ("after", "end")}


# --------------------------------------------------------------------------- runs that stop


def stuck(home: Path, then: str, *args: str) -> Result:
    directory = str(PROJECTS / "stuck")
    stdin = json.dumps({"then": then})
    return ok(
        kontra(home, "graph", "--dir", directory, "--input", "-", "--runs", "2", *args, stdin=stdin)
    )


def test_a_wait_no_timer_ends_is_timed_out_and_reported(home: Path) -> None:
    result = stuck(home, "wait")
    assert result.stdout == (
        "StuckFlow (acme/stuck@1.0.0): 2 mock runs\n  start → step\n  step → odd\n"
    )  # what it did until the wait; a run that did not close itself has no end
    assert (
        "warning: acme/stuck@1.0.0: runs 0, 1 of 2 waited for what no timer brings (a signal, "
        "or a wait_condition without a timeout), so the test server timed them out; what they "
        "did until then is in the graph\n"
    ) in result.stderr


def test_a_run_still_open_at_the_deadline_is_terminated(home: Path) -> None:
    result = stuck(home, "loop", "--timeout", "0.5")
    assert result.stdout == (
        "StuckFlow (acme/stuck@1.0.0): 2 mock runs\n"
        "  start → step\n"
        "  step → odd\n"
        "  odd → step#2\n"
        "  step#2 → step#2\n"
    )
    assert (
        "warning: acme/stuck@1.0.0: runs 0, 1 of 2 did not close within 0.5s and were "
        "terminated (a loop that never waits on a timer runs until then); what they did until "
        "then is in the graph\n"
    ) in result.stderr


def test_a_workflow_task_that_keeps_failing_and_an_unsampleable_return_are_reported(
    home: Path,
) -> None:
    result = stuck(home, "crash")
    assert result.stdout == (
        "StuckFlow (acme/stuck@1.0.0): 2 mock runs\n  start → step\n  step → odd\n"
    )
    assert result.stderr.splitlines() == [
        "warning: acme/stuck@1.0.0: never scheduled in 2 mock runs: after — branches on "
        "timers, signals or inputs the runs did not reach",
        "warning: acme/stuck@1.0.0: run 0's workflow task kept failing on the mock values "
        "(cannot handle a crash); the run was terminated",
        "warning: acme/stuck@1.0.0: run 1's workflow task kept failing on the mock values "
        "(cannot handle a crash); the run was terminated",
        "warning: acme/stuck@1.0.0: no valid return value of odd could be sampled (the "
        "pattern '^(?=x)y$' has a lookaround); the stub returned null",
    ]


# --------------------------------------------------------------------------- the static graph


def make_profile(home: Path) -> None:
    from datetime import UTC, datetime

    from kontra.profile import Profile

    Profile(
        gate_url="http://127.0.0.1:1",
        temporal_address="127.0.0.1:2",
        party="acme",
        namespace="acme",
        refresh_token=None,
        auth_time=datetime.now(UTC),
        dev=True,
    ).save(home)


def serve_task_queue(directory: Path) -> str:
    """The task queue ``kontra serve`` runs the offering on, as its worker builds it."""
    with load_worker(directory) as worker:
        [bound] = worker.offerings
        return bound.offering_id.task_queue


def test_the_static_graph_is_saved_for_kontra_devs_page(home: Path) -> None:
    make_profile(home)
    result = ok(kontra(home, "graph", "--dir", str(EXAMPLES / "hello"), "--json", "--runs", "2"))
    assert result.stderr == f"static graph saved for kontra dev's graph page ({home / 'graphs'})\n"
    task_queue = serve_task_queue(EXAMPLES / "hello")
    path = static_path(home, task_queue)
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert stat.S_IMODE(path.parent.stat().st_mode) == 0o700
    saved = json.loads(path.read_text())
    [printed] = json.loads(result.stdout)
    assert saved == {
        "version": 1,
        "task_queue": task_queue,
        "offering": "acme/hello@0.1.0",
        "runs": 2,
        "seed": 0,
        "graph": printed["graph"],
    }
    assert load_static(home, task_queue) == printed["graph"]
    assert [p.name for p in path.parent.iterdir()] == [path.name]  # no temporary file left


def test_no_save_and_no_profile_save_nothing(home: Path, tmp_path: Path) -> None:
    ok(kontra(home, "graph", "--dir", str(EXAMPLES / "hello"), "--runs", "1"))
    assert not home.exists()
    make_profile(home)
    result = ok(kontra(home, "graph", "--dir", str(EXAMPLES / "hello"), "--runs", "1", "--no-save"))
    assert result.stderr == ""
    assert not (home / "graphs").exists()


def test_load_static_reads_only_the_graph_of_that_task_queue(tmp_path: Path) -> None:
    from kontra.cli._graph import save_static

    g: dict[str, Any] = {"version": 1, "workflow": {}, "nodes": [], "edges": []}
    a, b = "acme/a@1.0.0", "acme/b@1.0.0"
    assert load_static(tmp_path, a) is None  # nothing saved
    save_static(tmp_path, a, offering=a, runs=8, seed=0, graph=g)
    assert load_static(tmp_path, a) == g
    # A's file under B's name: the stored task queue is A's, so it is not B's graph.
    shutil.copy(static_path(tmp_path, a), static_path(tmp_path, b))
    assert load_static(tmp_path, b) is None
    path = static_path(tmp_path, a)
    for broken in (b"{", b"[]", json.dumps({"version": 2, "task_queue": a, "graph": g}).encode()):
        path.write_bytes(broken)
        assert load_static(tmp_path, a) is None
    path.unlink()
    path.mkdir()  # not a regular file
    assert load_static(tmp_path, a) is None
    path.rmdir()
    os.mkfifo(path)  # one planted there must not hang the page
    assert load_static(tmp_path, a) is None
