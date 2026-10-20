"""``kontra graph`` (decisions I27; kontra-sdk#42): worker.py's workflows as a graph, from mock
runs in Temporal's time-skipping test server.

#42's "done when", as far as this command answers for it: every seller example's graph is
the one its README documents (and ``examples/recon`` is refused, with a clear line: it is
buyer code, with no worker.py); ``examples/sweep``'s fan-out is one parallel group, its one
activity per URL one node, and its ``if`` on what the checks found shows both branches from
two runs; never-scheduled activities are listed with exit status 0. Then the other warnings,
``--json``, ``--input``, and the static graph ``kontra dev``'s page reads. The projects under
``projects/`` are seller projects written for these tests (``branches`` has a fan-out, a
batch -- the optional ``kontra.contrib.parallel`` helper's child -- and a branch, and an
activity only a signal reaches; ``nested`` what only a
child's history or a marker shows, and a batch over pages; ``two`` a worker that binds two
offerings); the command runs in-process (``CliRunner``)."""

from __future__ import annotations

import asyncio
import importlib.util
import json
import os
import re
import shutil
import signal
import stat
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import click
import pytest
from click.testing import CliRunner, Result
from temporalio.testing import WorkflowEnvironment

from kontra.cli import cli
from kontra.cli._graph import load_static, static_path
from kontra.cli._mock import explore, server_dir
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
    """Temporal's time-skipping test server, which ``kontra graph`` downloads into its own
    directory on first use: without it, and without the network, these tests are skipped."""

    async def start() -> None:
        where = str(server_dir())
        async with await WorkflowEnvironment.start_time_skipping(download_dest_dir=where):
            pass

    try:
        asyncio.run(start())
    except Exception as e:
        pytest.skip(f"no time-skipping test server: {e}")


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


def test_kontra_graph_is_hidden_from_help_and_still_answers_its_own() -> None:
    """Not a command for users (decisions I35): ``kontra serve`` runs it under a dev profile
    to draw the graph page's static graphs. Kept, with its flags, as that child's entry point
    and for the READMEs' graphs below."""
    top = CliRunner().invoke(cli, ["--help"])
    assert top.exit_code == 0, top.output
    assert "graph" not in top.output.lower()
    own = CliRunner().invoke(cli, ["graph", "--help"])
    assert own.exit_code == 0, own.output
    for flag in ("--dir", "--offering", "--runs", "--seed", "--input", "--timeout", "--json"):
        assert flag in own.output
    assert "--mermaid" in own.output
    assert "--no-save" in own.output


def test_every_seller_example_is_drawn() -> None:
    assert SELLERS == ["chef-agents", "chef-claude", "enrich", "hello", "legacy-actor", "sweep"]


@pytest.mark.parametrize("example", SELLERS)
def test_the_graph_of_every_seller_example_is_its_readmes(home: Path, example: str) -> None:
    """Golden: the README's ```mermaid block is what ``kontra graph --mermaid`` prints (8
    runs, seed 0). After a deliberate change, ``UPDATE_GOLDEN=1 pytest tests/graph/test_cli.py``
    rewrites the blocks; review the diff."""
    if example == "chef-agents" and not AGENTS:
        pytest.skip("temporalio-openai-agents (the dev group's) is not installed")
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
def test_the_sweep_example_shows_a_fan_out_one_activity_per_url_and_both_branches(
    home: Path, runs: str
) -> None:
    """examples/sweep, #42's three shapes in vanilla Temporal: the two fetches are one parallel
    group (same group, the same predecessor), which the ``if``'s two branches are not; the
    per-URL checks are ONE activity node however many URLs there are, and no child workflow
    -- with an edge to itself only from run 2 on, whose ``plan`` drew 201 URLs, more than
    the 50 the workflow's semaphore lets open, so each completion starts the next in a
    workflow task of its own; and the ``if`` on what they found shows both branches from two
    runs (run 0's check finds nothing, run 1's carries an error, so its page counts as
    unreachable and the run scores)."""
    g = graph_of(home, EXAMPLES / "sweep", "--runs", runs)
    robots, security_txt = node(g, "fetch_robots"), node(g, "fetch_security_txt")
    assert robots["group"] == security_txt["group"] != node(g, "plan")["group"]
    assert {s for s, t in edges(g) if t in ("fetch_robots", "fetch_security_txt")} == {"plan"}
    # The same predecessor, but taken by different runs: two groups, not a fan-out.
    assert node(g, "score")["group"] != node(g, "all_clear")["group"]
    assert not [n for n in g["nodes"] if n["kind"] == "child"]
    check = "check_headers"
    assert [(n["id"], n["kind"]) for n in g["nodes"] if n["name"] == check] == [
        (check, "activity")
    ]  # one node: never check_headers#2
    loop = {(check, check)} if runs == "4" else set()  # runs 0 and 1 draw 1 and 3 URLs
    assert {(s, t) for s, t in edges(g) if check in (s, t)} == {
        ("fetch_robots", check),
        ("fetch_security_txt", check),
        (check, "score"),
        (check, "all_clear"),
        *loop,
    }
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
    assert node(g, "accept")["group"] != node(g, "reject")["group"]  # an if/else
    assert {s for s, t in edges(g) if t in ("fetch_a", "fetch_b")} == {"plan"}
    children = [n for n in g["nodes"] if n["kind"] == "child"]
    assert [(n["id"], n["label"]) for n in children] == [
        ("kontra.batch.v1[check]", "execute_activities_in_parallel(check)")
    ]  # one node, though run 0's batch got no items: no continue-as-new chain, no item activities
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


def test_what_only_a_marker_or_a_childs_child_schedules_is_scheduled(home: Path) -> None:
    """A local activity leaves only its marker in history, and ``probe`` runs only in the
    batch a child workflow starts: both count as scheduled. The batch over pages is one node,
    though run 0's first page was empty and scheduled no item activity."""
    result = ok(kontra(home, "graph", "--dir", str(PROJECTS / "nested"), "--json"))
    assert result.stderr == ""
    [found] = json.loads(result.stdout)
    assert found["never_scheduled"] == []
    g = found["graph"]
    assert [(n["id"], n["kind"]) for n in g["nodes"]] == [
        ("note", "local_activity"),
        ("Inner", "child"),
        ("kontra.batch.v1[list_pages,visit]", "child"),
    ]
    assert edges(g) == {
        ("start", "note"),
        ("note", "Inner"),
        ("Inner", "kontra.batch.v1[list_pages,visit]"),
        ("kontra.batch.v1[list_pages,visit]", "end"),
    }


def test_a_worker_of_two_offerings_warns_once_of_what_neither_scheduled(home: Path) -> None:
    """Each offering's runs schedule only its own activity: neither is reported because
    the other's runs never scheduled it; ``unused``, which no run scheduled, is, once."""
    directory = str(PROJECTS / "two")
    result = ok(kontra(home, "graph", "--dir", directory, "--runs", "2"))
    assert result.stderr == (
        "warning: acme/two-a@1.0.0, acme/two-b@1.0.0: never scheduled in 4 mock runs: unused "
        "— branches on timers, signals or inputs the runs did not reach\n"
    )
    found = json.loads(
        ok(kontra(home, "graph", "--dir", directory, "--json", "--runs", "1")).stdout
    )
    assert [(f["offering"], f["never_scheduled"]) for f in found] == [
        ("acme/two-a@1.0.0", ["unused"]),
        ("acme/two-b@1.0.0", ["unused"]),
    ]
    only = ok(
        kontra(home, "graph", "--dir", directory, "--runs", "1", "--offering", "acme/two-a@1.0.0")
    )
    assert only.stderr == (
        "warning: acme/two-a@1.0.0: never scheduled in 1 mock run: act_b, unused — branches "
        "on timers, signals or inputs the runs did not reach, or the worker's offerings "
        "--offering left out\n"
    )
    logged = ok(
        kontra(
            home,
            *("graph", "--dir", directory, "--runs", "1", "--offering", "acme/two-a@1.0.0"),
            "--background",
        )
    )  # kontra serve's child, which draws only the offerings out of date
    assert logged.stderr == (
        "warning: acme/two-a@1.0.0: never scheduled in 1 mock run: act_b, unused — branches "
        "on timers, signals or inputs the runs did not reach, or the worker's offerings "
        "whose graph was current\n"
    )


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


def test_an_unsampleable_input_is_advised_for_whoever_reads_it(home: Path, tmp_path: Path) -> None:
    """Typed, the way out is ``--input``; run by ``kontra serve`` (``--background``), whose
    worker logs it and has no ``--input``, it says what the graph page does instead."""
    project = tmp_path / "picky"
    shutil.copytree(EXAMPLES / "hello", project, ignore=shutil.ignore_patterns("__pycache__"))
    models = project / "models.py"
    field = '    name: str = Field(min_length=1, max_length=100, description="Who to greet.")\n'
    text = models.read_text()
    assert field in text
    models.write_text(
        text.replace("import BaseModel,", "import field_validator, BaseModel,").replace(
            field,
            field
            + "\n    @field_validator('name')\n    @classmethod\n"
            + "    def _known(cls, v: str) -> str:\n"
            + "        raise ValueError('an unknown name')\n",
        )
    )
    typed = kontra(home, "graph", "--dir", str(project), "--runs", "1")
    assert typed.exit_code == 1
    assert typed.stderr.startswith("error: cannot sample a valid HelloInput (")
    assert typed.stderr.endswith("); give one with --input FILE\n")
    logged = kontra(home, "graph", "--dir", str(project), "--runs", "1", "--background")
    assert logged.exit_code == 1
    assert logged.stderr.startswith("error: cannot sample a valid HelloInput (")
    assert logged.stderr.endswith("), so the graph page draws only what each run did\n")
    assert "--input" not in logged.stderr


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
        "warning: acme/stuck@1.0.0: runs 0, 1 of 2 still open after 0.5s were terminated (a "
        "loop on an activity's result, which the mock answers the same way every time in a "
        "run, or a very long loop); what they did until then is in the graph\n"
    ) in result.stderr


def test_the_runs_open_at_the_deadline_are_terminated_not_left_running() -> None:
    """Their histories end terminated: a run left open would go on looping on the worker
    while the next runs go."""
    with load_worker(PROJECTS / "stuck") as worker:
        [found] = asyncio.run(
            explore(worker, worker.offerings, [[{"then": "loop"}] * 2], seed=0, deadline=0.5)
        )
    assert found.deadline == [0, 1]
    assert found.statuses == ["terminated", "terminated"]


@pytest.mark.skipif(not Path("/proc/self/stat").exists(), reason="reads /proc")
@pytest.mark.parametrize("sig", ["SIGTERM", "SIGHUP"])
def test_sigterm_and_sighup_shut_the_test_server_down(home: Path, sig: str) -> None:
    """A supervisor, an IDE's stop button or ``subprocess.terminate()`` sends SIGTERM, a
    closed terminal SIGHUP: like Ctrl-C, the command stops and the test server stops with
    it, instead of staying behind, orphaned, on a port of every interface."""
    stdin = json.dumps({"then": "loop"})
    command = [sys.executable, "-m", "kontra.cli", "graph", "--dir", str(PROJECTS / "stuck")]
    env = {**os.environ, "KONTRA_HOME": str(home)}
    with subprocess.Popen(
        [*command, "--input", "-", "--runs", "1", "--timeout", "60"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env,
    ) as proc:
        assert proc.stdin is not None
        proc.stdin.write(stdin.encode())
        proc.stdin.close()
        server = None
        for _ in range(300):  # the run loops for a minute once the server is up
            server = next(iter(children_of(proc.pid)), None)
            if server is not None:
                break
            time.sleep(0.05)
        assert server is not None, "no test server started"
        time.sleep(1)
        proc.send_signal(getattr(signal, sig))
        assert proc.wait(timeout=30) != 0
    for _ in range(100):
        if not Path(f"/proc/{server}").exists():
            break
        time.sleep(0.05)
    assert not Path(f"/proc/{server}").exists(), f"test server {server} left running"


def children_of(pid: int) -> list[int]:
    """The processes whose parent is ``pid``."""
    found: list[int] = []
    for stat_file in Path("/proc").glob("[0-9]*/stat"):
        try:
            fields = stat_file.read_text().rpartition(")")[2].split()
        except OSError:
            continue
        if int(fields[1]) == pid:
            found.append(int(stat_file.parent.name))
    return found


def test_a_workflow_task_that_keeps_failing_and_an_unsampleable_return_are_reported(
    home: Path,
) -> None:
    result = stuck(home, "crash")
    assert result.stdout == (
        "StuckFlow (acme/stuck@1.0.0): 2 mock runs\n  start → step\n  step → odd\n"
    )
    assert result.stderr.splitlines() == [
        "warning: acme/stuck@1.0.0: run 0's workflow task kept failing on the mock values "
        "(cannot handle a crash); the run was terminated",
        "warning: acme/stuck@1.0.0: run 1's workflow task kept failing on the mock values "
        "(cannot handle a crash); the run was terminated",
        "warning: acme/stuck@1.0.0: no valid return value of odd could be sampled (the "
        "pattern '^(?=x)y$' has a lookaround); the stub returned null",
        "warning: acme/stuck@1.0.0: never scheduled in 2 mock runs: after — branches on "
        "timers, signals or inputs the runs did not reach",
    ]


# --------------------------------------------------------------------------- the test server


def test_the_test_server_is_kept_where_only_its_user_can_write(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """temporalio's default is the shared temp directory, where it runs whatever file sits
    at its predictable name. The command's is the user's cache directory, 0700, and anything
    in it that someone else owns, or can write, is refused."""
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "cache"))
    where = server_dir()
    assert where == tmp_path / "cache" / "kontra" / "temporal-test-server"
    assert stat.S_IMODE(where.stat().st_mode) == 0o700
    where.chmod(0o777)  # ours, but loose: made private again
    assert stat.S_IMODE(server_dir().stat().st_mode) == 0o700
    planted = where / "temporal-test-server-sdk-python-1.34.0"
    planted.write_text("#!/bin/sh\n")
    planted.chmod(0o777)
    with pytest.raises(click.ClickException, match="not yours alone"):
        server_dir()
    planted.unlink()
    planted.symlink_to("/bin/sh")
    with pytest.raises(click.ClickException, match="not yours alone"):
        server_dir()
    planted.unlink()
    if os.getuid() == 0:  # only root can give a file away
        planted.write_text("#!/bin/sh\n")
        planted.chmod(0o700)
        os.chown(planted, 4242, 4242)
        with pytest.raises(click.ClickException, match="not yours alone"):
            server_dir()
        planted.unlink()
        os.chown(where, 4242, 4242)
        with pytest.raises(click.ClickException, match="not a directory of yours"):
            server_dir()
    monkeypatch.setenv("XDG_CACHE_HOME", "relative")  # not a path the spec allows: ignored
    assert server_dir() == Path.home() / ".cache" / "kontra" / "temporal-test-server"


def test_kontra_graph_runs_the_test_server_from_that_directory(
    home: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    asked: list[object] = []
    start = WorkflowEnvironment.start_time_skipping

    async def spy(**kwargs: Any) -> WorkflowEnvironment:
        asked.append(kwargs.get("download_dest_dir"))
        return await start(**kwargs)

    monkeypatch.setattr(WorkflowEnvironment, "start_time_skipping", staticmethod(spy))
    ok(kontra(home, "graph", "--dir", str(EXAMPLES / "hello"), "--runs", "1"))
    assert asked == [str(server_dir())]


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
    return served(directory)[0]


def served(directory: Path) -> tuple[str, str, str]:
    """The task queue ``kontra serve`` runs the offering on, its contract hash, and the
    project's code hash, as its worker builds them."""
    from kontra._graphs import code_hash

    with load_worker(directory) as worker:
        [bound] = worker.offerings
        return bound.offering_id.task_queue, bound.contract_hash(), code_hash(directory)


def test_the_static_graph_is_saved_for_kontra_devs_page(home: Path) -> None:
    make_profile(home)
    result = ok(kontra(home, "graph", "--dir", str(EXAMPLES / "hello"), "--json", "--runs", "2"))
    assert result.stderr == f"static graph saved for kontra dev's graph page ({home / 'graphs'})\n"
    task_queue, drawn_from, code = served(EXAMPLES / "hello")
    path = static_path(home, task_queue)
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert stat.S_IMODE(path.parent.stat().st_mode) == 0o700
    saved = json.loads(path.read_text())
    [printed] = json.loads(result.stdout)
    assert saved == {
        "version": 2,
        "task_queue": task_queue,
        "offering": "acme/hello@0.1.0",
        "contract_hash": drawn_from,  # the code's (BoundOffering.contract_hash()), I35
        "code_hash": code,  # the project's code as imported here, by hand
        "runs": 2,
        "seed": 0,
        "graph": printed["graph"],
    }
    assert load_static(home, task_queue) == printed["graph"]
    assert [p.name for p in path.parent.iterdir()] == [path.name]  # no temporary file left
    # kontra serve's child saves under the serving worker's code hash, whatever it imported.
    given = "sha256:" + "5" * 64
    args = ("--json", "--runs", "2", "--background", "--code-hash", given)
    ok(kontra(home, "graph", "--dir", str(EXAMPLES / "hello"), *args))
    assert json.loads(path.read_text())["code_hash"] == given


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
    save_static(
        tmp_path,
        a,
        offering=a,
        contract_hash="sha256:a",
        code_hash="sha256:c",
        runs=8,
        seed=0,
        graph=g,
    )
    assert load_static(tmp_path, a) == g
    # A's file under B's name: the stored task queue is A's, so it is not B's graph.
    shutil.copy(static_path(tmp_path, a), static_path(tmp_path, b))
    assert load_static(tmp_path, b) is None
    path = static_path(tmp_path, a)
    whole = json.loads(path.read_text())
    v1 = {k: v for k, v in whole.items() if k not in ("contract_hash", "code_hash")}
    v1 |= {"version": 1}
    for broken in (
        b"{",
        b"[]",
        json.dumps(v1).encode(),  # what kontra graph saved before I35: drawn again
        json.dumps(whole | {"version": 3}).encode(),
        json.dumps(whole | {"contract_hash": None}).encode(),
        json.dumps(whole | {"code_hash": ""}).encode(),
        json.dumps(whole | {"runs": True}).encode(),
    ):
        path.write_bytes(broken)
        assert load_static(tmp_path, a) is None
    path.unlink()
    path.mkdir()  # not a regular file
    assert load_static(tmp_path, a) is None
    path.rmdir()
    os.mkfifo(path)  # one planted there must not hang the page
    assert load_static(tmp_path, a) is None
