"""The graph page's static graphs, drawn by ``kontra serve`` (decisions I35; kontra-sdk#56):
the document records the contract hash it was drawn from, two writers leave a valid file,
and :class:`kontra._graphs.Refresh` runs ``kontra graph`` in a child process -- drawing what
the examples' READMEs show, nothing when the graph is current, one at a time per home --
whose every failure (no test server, past its time limit, a non-zero exit, an import error)
is one WARNING line and never an exception, and whose child dies with its test server when
stopped. The plugin's side is in ``tests/serve/test_plugin.py``, the whole loop on a real
``kontra dev up`` in ``tests/cli/test_dev_stack.py``."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import sys
import threading
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from temporalio.testing import WorkflowEnvironment

from kontra import _graphs
from kontra._graphs import (
    Refresh,
    Wanted,
    child_argv,
    out_of_date,
    read_static,
    save_static,
    static_path,
)
from kontra.cli._mock import server_dir
from kontra.cli._project import load_worker
from kontra.graph import mermaid
from kontra.profile import Profile

pytestmark = pytest.mark.temporal

ROOT = Path(__file__).resolve().parents[2]
EXAMPLES = ROOT / "examples"
PROJECTS = Path(__file__).resolve().parent / "projects"
_MERMAID = re.compile(r"^```mermaid\n(.*?)^```$", re.MULTILINE | re.DOTALL)
#: Long enough for a child on a loaded machine: imports, the test server, 8 runs.
_PATIENCE = 240.0


@pytest.fixture(scope="module")
def test_server() -> None:
    """Temporal's time-skipping test server in ``kontra graph``'s directory: without it,
    and without the network, the tests that draw are skipped."""

    async def start() -> None:
        async with await WorkflowEnvironment.start_time_skipping(
            download_dest_dir=str(server_dir())
        ):
            pass

    try:
        asyncio.run(start())
    except Exception as e:
        pytest.skip(f"no time-skipping test server: {e}")


@pytest.fixture
def home(tmp_path: Path) -> Path:
    """A ``KONTRA_HOME`` with a dev profile: where ``kontra graph`` saves."""
    home = tmp_path / "home"
    Profile(
        gate_url="http://127.0.0.1:1",
        temporal_address="127.0.0.1:2",
        party="acme",
        namespace="acme",
        refresh_token=None,
        auth_time=datetime.now(UTC),
        dev=True,
    ).save(home)
    return home


@pytest.fixture
def warnings(caplog: pytest.LogCaptureFixture) -> pytest.LogCaptureFixture:
    caplog.set_level(logging.DEBUG, logger="kontra.serve")
    return caplog


def wanted_of(directory: Path) -> list[Wanted]:
    with load_worker(directory) as worker:
        return [
            Wanted(b.key, b.offering_id.task_queue, b.contract_hash())
            for b in worker.offerings
            if b.kind == "workflow"
        ]


def said(caplog: pytest.LogCaptureFixture, level: int) -> list[str]:
    return [r.getMessage() for r in caplog.records if r.levelno == level]


def session(sid: int) -> list[tuple[int, str]]:
    """The processes of session ``sid`` (the child's: it leads its own), from ``/proc``."""
    out: list[tuple[int, str]] = []
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            stat_line = (entry / "stat").read_text()
            cmd = (entry / "cmdline").read_bytes().replace(b"\0", b" ").decode(errors="replace")
        except OSError:
            continue
        fields = stat_line[stat_line.rindex(")") + 2 :].split()
        if int(fields[3]) == sid and fields[0] != "Z":  # field 6, the session
            out.append((int(entry.name), cmd))
    return out


def readme_graph(example: str) -> str:
    [block] = _MERMAID.findall((EXAMPLES / example / "README.md").read_text(encoding="utf-8"))
    return block


def project_with(root: Path, worker: str) -> Path:
    root.mkdir(parents=True)
    (root / "worker.py").write_text(worker)
    return root


def run(refresh: Refresh, within: float = _PATIENCE) -> str | None:
    refresh.start()
    assert refresh.wait(within), "the refresh never finished"
    return refresh.outcome


# --------------------------------------------------------------------------- the document


def test_the_document_records_the_contract_hash_and_a_v1_file_reads_as_absent(
    tmp_path: Path,
) -> None:
    g: dict[str, Any] = {"workflow": {}, "nodes": [], "edges": []}
    q = "acme/a@1.0.0"
    path = save_static(tmp_path, q, offering=q, contract_hash="sha256:1", runs=8, seed=0, graph=g)
    found = read_static(tmp_path, q)
    assert found is not None
    assert (found.task_queue, found.offering, found.contract_hash) == (q, q, "sha256:1")
    assert (found.runs, found.seed, found.graph) == (8, 0, g)
    document = json.loads(path.read_text())
    assert document["version"] == _graphs.STATIC_VERSION == 2
    del document["contract_hash"]
    path.write_text(json.dumps(document | {"version": 1}))  # what kontra graph wrote before
    assert read_static(tmp_path, q) is None
    assert out_of_date(tmp_path, [Wanted(q, q, "sha256:1")]) == [Wanted(q, q, "sha256:1")]


def test_two_saves_at_once_leave_one_valid_file(tmp_path: Path) -> None:
    """Two workers that draw the same queue at once: each write replaces the whole file
    (``os.replace``), so the last one wins and the file is always one of them, whole."""
    q = "acme/a@1.0.0"
    graphs = {
        h: {"workflow": {"type": h}, "nodes": [{"id": f"n{i}"} for i in range(500)], "edges": []}
        for h in ("sha256:1", "sha256:2")
    }
    seen: list[str | None] = []
    torn: list[str] = []
    start = threading.Barrier(3)

    def writer(h: str) -> None:
        start.wait()
        for _ in range(40):
            save_static(tmp_path, q, offering=q, contract_hash=h, runs=8, seed=0, graph=graphs[h])

    def reader() -> None:
        start.wait()
        for _ in range(200):
            found = read_static(tmp_path, q)
            seen.append(found.contract_hash if found is not None else None)
            if found is not None and found.graph != graphs.get(found.contract_hash):
                torn.append(found.contract_hash)

    threads = [threading.Thread(target=writer, args=(h,)) for h in graphs]
    threads.append(threading.Thread(target=reader))
    for t in threads:
        t.start()
    for t in threads:
        t.join(60)
    found = read_static(tmp_path, q)
    assert found is not None
    assert found.graph == graphs[found.contract_hash]
    assert set(seen) - {None} <= set(graphs)
    assert not torn  # every read was one writer's whole document
    assert [p.name for p in static_path(tmp_path, q).parent.iterdir()] == [
        static_path(tmp_path, q).name
    ]  # no temporary file left


def test_only_a_missing_graph_or_one_of_another_contract_is_out_of_date(tmp_path: Path) -> None:
    g: dict[str, Any] = {"workflow": {}, "nodes": [], "edges": []}
    a, b, c = (Wanted(f"acme/{x}@1.0.0", f"acme/{x}@1.0.0", "sha256:new") for x in "abc")
    save_static(
        tmp_path,
        a.task_queue,
        offering=a.offering,
        contract_hash="sha256:new",
        runs=8,
        seed=0,
        graph=g,
    )
    save_static(
        tmp_path,
        b.task_queue,
        offering=b.offering,
        contract_hash="sha256:old",
        runs=8,
        seed=0,
        graph=g,
    )
    assert out_of_date(tmp_path, [a, b, c]) == [b, c]


def test_the_child_is_kontra_graph_with_the_readmes_defaults(tmp_path: Path) -> None:
    argv = child_argv(tmp_path, ["acme/a@1.0.0", "acme/b@1.0.0"], timeout=5.0)
    assert argv[:3] == [sys.executable, "-P", "-c"]  # -P: the working directory stays off
    assert "os.nice(" in argv[3]
    assert "from kontra.cli import main" in argv[3]
    assert argv[4:] == [
        "graph",
        "--dir",
        str(tmp_path),
        "--runs",
        "8",
        "--seed",
        "0",
        "--timeout",
        "5.0",
        "--offering",
        "acme/a@1.0.0",
        "--offering",
        "acme/b@1.0.0",
    ]


# --------------------------------------------------------------------------- drawing


def test_a_refresh_draws_what_the_readme_shows_and_nothing_once_current(
    test_server: None, home: Path, warnings: pytest.LogCaptureFixture
) -> None:
    wanted = wanted_of(EXAMPLES / "hello")
    refresh = Refresh(home, EXAMPLES / "hello", wanted)
    assert run(refresh) == "drawn", said(warnings, logging.WARNING)
    [w] = wanted
    found = read_static(home, w.task_queue)
    assert found is not None
    assert (found.offering, found.contract_hash) == (w.offering, w.contract_hash)
    assert (found.runs, found.seed) == (8, 0)
    # The README's graph is this one: the child is kontra graph with its defaults.
    assert f"%% {w.offering}\n{mermaid(found.graph)}" == readme_graph("hello")
    assert not said(warnings, logging.WARNING)
    infos = said(warnings, logging.INFO)
    assert any(
        m.startswith("drawing the graph page's static graph of acme/hello@0.1.0") for m in infos
    )
    assert any(
        m.startswith("drew the graph page's static graph of acme/hello@0.1.0") for m in infos
    )
    assert refresh.pid is not None
    assert not session(refresh.pid)  # the child and its test server are gone

    again = Refresh(home, EXAMPLES / "hello", wanted)
    assert run(again) == "current"
    assert again.pid is None  # no child


def test_a_missing_test_server_is_one_warning(
    home: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, warnings: pytest.LogCaptureFixture
) -> None:
    """No test server in the cache and no network to download it: one WARNING, nothing
    saved, no exception."""
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "empty-cache"))
    for proxy in ("HTTPS_PROXY", "HTTP_PROXY", "ALL_PROXY", "https_proxy", "http_proxy"):
        monkeypatch.setenv(proxy, "http://127.0.0.1:9")  # nothing listens on discard
    monkeypatch.delenv("NO_PROXY", raising=False)
    monkeypatch.delenv("no_proxy", raising=False)
    wanted = wanted_of(EXAMPLES / "hello")
    assert run(Refresh(home, EXAMPLES / "hello", wanted)) == "failed"
    [warning] = said(warnings, logging.WARNING)
    assert warning.startswith("the graph page's static graph of acme/hello@0.1.0 was not drawn:")
    assert "cannot start Temporal's time-skipping test server" in warning
    assert out_of_date(home, wanted) == wanted


@pytest.mark.skipif(not Path("/proc/self/stat").exists(), reason="reads /proc")
def test_a_child_past_its_limit_is_stopped_with_its_test_server(
    test_server: None, home: Path, warnings: pytest.LogCaptureFixture
) -> None:
    """``stuck``'s runs stay open up to their deadline, so eight take far longer than the
    limit: the child is stopped, its whole group with it, and that is one WARNING."""
    wanted = wanted_of(PROJECTS / "stuck")
    refresh = Refresh(home, PROJECTS / "stuck", wanted, limit=12.0)
    assert run(refresh) == "failed"
    [warning] = said(warnings, logging.WARNING)
    assert warning == (
        "the graph page's static graph of acme/stuck@1.0.0 was not drawn: the mock runs "
        "took longer than 12s and were stopped"
    )
    assert refresh.pid is not None
    assert not session(refresh.pid)


def test_a_child_that_exits_non_zero_is_one_warning(
    home: Path, tmp_path: Path, warnings: pytest.LogCaptureFixture
) -> None:
    empty = tmp_path / "no-project"
    empty.mkdir()
    wanted = [Wanted("acme/x@1.0.0", "acme/x@1.0.0", "sha256:x")]
    assert run(Refresh(home, empty, wanted)) == "failed"
    [warning] = said(warnings, logging.WARNING)
    assert warning.startswith(
        "the graph page's static graph of acme/x@1.0.0 was not drawn: the mock runs exited "
        "with status 1 (error: no worker.py in "
    )


def test_an_import_error_in_the_child_is_one_warning(
    home: Path, tmp_path: Path, warnings: pytest.LogCaptureFixture
) -> None:
    """The child imports the project afresh: one that fails to import there is a WARNING
    naming the error, never the worker's."""
    project = project_with(tmp_path / "broken", "import kontra_sdk_56_no_such_module\n")
    wanted = [Wanted("acme/x@1.0.0", "acme/x@1.0.0", "sha256:x")]
    assert run(Refresh(home, project, wanted)) == "failed"
    [warning] = said(warnings, logging.WARNING)
    assert "exited with status 1" in warning
    assert "No module named 'kontra_sdk_56_no_such_module'" in warning


@pytest.mark.skipif(not Path("/proc/self/stat").exists(), reason="reads /proc")
def test_stop_ends_the_child_and_its_test_server(
    test_server: None, home: Path, warnings: pytest.LogCaptureFixture
) -> None:
    """What the worker's shutdown does: the child is asked to stop (it shuts its test server
    down, as on Ctrl-C), then killed with its group. Nothing is logged as a failure."""
    wanted = wanted_of(PROJECTS / "stuck")
    refresh = Refresh(home, PROJECTS / "stuck", wanted)
    refresh.start()
    end = time.monotonic() + _PATIENCE
    while not (
        refresh.pid is not None
        and any("temporal-test-server" in cmd for _, cmd in session(refresh.pid))
    ):
        assert time.monotonic() < end, "the child never started its test server"
        assert refresh.outcome is None, said(warnings, logging.WARNING)
        time.sleep(0.2)
    refresh.stop()
    assert not session(refresh.pid)
    assert refresh.wait(30)
    assert refresh.outcome == "stopped"
    assert not said(warnings, logging.WARNING)

    never = Refresh(home, PROJECTS / "stuck", wanted)
    never.stop()  # before it spawned: it never does
    assert run(never) == "stopped"
    assert never.pid is None


def test_one_refresh_at_a_time_per_home_and_the_second_finds_the_first_s_graph(
    home: Path, tmp_path: Path
) -> None:
    """While another refresh of this home holds the lock, a refresh waits; when the graph
    it wanted was drawn meanwhile, it spawns nothing."""
    w = Wanted("acme/x@1.0.0", "acme/x@1.0.0", "sha256:x")
    stop = threading.Event()
    with _graphs._one_at_a_time(home, stop) as held:  # pyright: ignore[reportPrivateUsage]
        assert held
        waiting = Refresh(home, tmp_path / "unused", [w])
        waiting.start()
        assert not waiting.wait(1.5)  # it waits for the lock
        save_static(
            home,
            w.task_queue,
            offering=w.offering,
            contract_hash=w.contract_hash,
            runs=8,
            seed=0,
            graph={"workflow": {}, "nodes": [], "edges": []},
        )
    assert waiting.wait(10)
    assert (waiting.outcome, waiting.pid) == ("current", None)
    assert os.stat(home / "graphs" / ".drawing.lock").st_mode & 0o077 == 0
