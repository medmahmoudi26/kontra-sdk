"""The graph page's static graphs, drawn by ``kontra serve`` (decisions I35; kontra-sdk#56):
the document records the contract hash and the code hash it was drawn from, two writers
leave a valid file, and :class:`kontra._graphs.Refresh` runs ``kontra graph`` in a child
process -- drawing what the examples' READMEs show, nothing when the graph is current, one at
a time per home -- whose every failure (no test server, past its time limit, a non-zero exit,
an import error, a child killed) is one WARNING line and never an exception, whose child
takes its whole group with it however it ends (stopped, killed, or its worker gone), and
whose stderr costs the worker a bounded amount however much the child writes. The plugin's side is in ``tests/serve/test_plugin.py``, the whole loop on a real
``kontra dev up`` in ``tests/cli/test_dev_stack.py``."""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import re
import shutil
import signal
import subprocess
import sys
import textwrap
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
    code_hash,
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
    """What ``kontra serve``'s worker asks for: each workflow offering's contract hash, and the
    project's code hash as that worker imported it."""
    with load_worker(directory) as worker:
        code = code_hash(directory)
        return [
            Wanted(b.key, b.offering_id.task_queue, b.contract_hash(), code)
            for b in worker.offerings
            if b.kind == "workflow"
        ]


def with_boot(monkeypatch: pytest.MonkeyPatch, code: str) -> None:
    """The child runs ``code`` instead of :func:`kontra._graphs.child_main`."""
    monkeypatch.setattr(_graphs, "_BOOT", textwrap.dedent(code))


_X = Wanted("acme/x@1.0.0", "acme/x@1.0.0", "sha256:x", "sha256:code")


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
    path = save_static(
        tmp_path,
        q,
        offering=q,
        contract_hash="sha256:1",
        code_hash="sha256:c",
        runs=8,
        seed=0,
        graph=g,
    )
    found = read_static(tmp_path, q)
    assert found is not None
    assert (found.task_queue, found.offering, found.contract_hash) == (q, q, "sha256:1")
    assert (found.code_hash, found.runs, found.seed, found.graph) == ("sha256:c", 8, 0, g)
    document = json.loads(path.read_text())
    assert document["version"] == _graphs.STATIC_VERSION == 2
    for key in ("code_hash", "contract_hash"):
        path.write_text(json.dumps(document))
        assert read_static(tmp_path, q) is not None
        path.write_text(json.dumps({k: v for k, v in document.items() if k != key}))
        assert read_static(tmp_path, q) is None  # a document without either is absent
    del document["contract_hash"], document["code_hash"]
    path.write_text(json.dumps(document | {"version": 1}))  # what kontra graph wrote before
    assert read_static(tmp_path, q) is None
    w = Wanted(q, q, "sha256:1", "sha256:c")
    assert out_of_date(tmp_path, [w]) == [w]


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
            save_static(
                tmp_path,
                q,
                offering=q,
                contract_hash=h,
                code_hash=h,
                runs=8,
                seed=0,
                graph=graphs[h],
            )

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


def test_only_a_missing_graph_or_one_of_another_contract_or_code_is_out_of_date(
    tmp_path: Path,
) -> None:
    g: dict[str, Any] = {"workflow": {}, "nodes": [], "edges": []}
    a, b, c, d = (
        Wanted(f"acme/{x}@1.0.0", f"acme/{x}@1.0.0", "sha256:new", "sha256:code") for x in "abcd"
    )
    for w, contract, code in ((a, "sha256:new", "sha256:code"), (b, "sha256:old", "sha256:code")):
        save_static(
            tmp_path,
            w.task_queue,
            offering=w.offering,
            contract_hash=contract,
            code_hash=code,
            runs=8,
            seed=0,
            graph=g,
        )
    save_static(  # the same contract, other code: the workflow was edited, its types kept
        tmp_path,
        d.task_queue,
        offering=d.offering,
        contract_hash="sha256:new",
        code_hash="sha256:edited",
        runs=8,
        seed=0,
        graph=g,
    )
    assert out_of_date(tmp_path, [a, b, c, d]) == [b, c, d]


def test_the_code_hash_follows_the_project_s_code_not_only_its_types(tmp_path: Path) -> None:
    """An edit to the workflow that keeps its types keeps the contract hash and changes the
    code hash; the same code hashes the same each time it is loaded, wherever it is."""
    project = tmp_path / "hello"
    shutil.copytree(EXAMPLES / "hello", project, ignore=shutil.ignore_patterns("__pycache__"))
    [before] = wanted_of(project)
    assert wanted_of(project) == [before]
    [elsewhere] = wanted_of(EXAMPLES / "hello")  # the same files, by relative path
    assert elsewhere.code_hash == before.code_hash
    assert re.fullmatch(r"sha256:[0-9a-f]{64}", before.code_hash)
    (project / "tests").mkdir()
    (project / "tests" / "test_x.py").write_text("x = 1\n")  # neither is code it imports
    (project / "README.md").write_text("changed\n")
    assert wanted_of(project) == [before]
    flow = project / "workflows.py"
    flow.write_text(flow.read_text().replace("maximum_attempts=3", "maximum_attempts=4"))
    [after] = wanted_of(project)
    assert after.contract_hash == before.contract_hash
    assert after.code_hash != before.code_hash
    assert code_hash(tmp_path / "nothing-imported") != after.code_hash


def test_the_child_is_kontra_graph_with_the_readmes_defaults(tmp_path: Path) -> None:
    argv = child_argv(tmp_path, ["acme/a@1.0.0", "acme/b@1.0.0"], timeout=5.0, code_hash="sha256:c")
    assert argv[:3] == [sys.executable, "-P", "-c"]  # -P: the working directory stays off
    assert argv[3] == "from kontra._graphs import child_main\nchild_main()\n"
    assert argv[4:] == [
        "graph",
        "--dir",
        str(tmp_path),
        "--background",
        "--code-hash",
        "sha256:c",
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
    assert run(Refresh(home, empty, [_X])) == "failed"
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
    assert run(Refresh(home, project, [_X])) == "failed"
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


def wait_for_session(pid: int, *, at_least: int, within: float = 60.0) -> None:
    end = time.monotonic() + within
    while len(session(pid)) < at_least:
        assert time.monotonic() < end, f"the child's session never had {at_least}: {session(pid)}"
        time.sleep(0.1)


def empty_within(pid: int, within: float) -> list[tuple[int, str]]:
    end = time.monotonic() + within
    while (left := session(pid)) and time.monotonic() < end:
        time.sleep(0.1)
    return left


def kill_session(pid: int | None) -> None:
    """Cleanup after a failed assertion: whatever is left in the child's session."""
    for found, _ in session(pid) if pid is not None else []:
        with contextlib.suppress(ProcessLookupError):
            os.kill(found, signal.SIGKILL)


@pytest.mark.skipif(not Path("/proc/self/stat").exists(), reason="reads /proc")
def test_a_child_that_ends_before_what_it_started_leaves_nothing(
    home: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, warnings: pytest.LogCaptureFixture
) -> None:
    """The child exits while a process it started -- a test server it never stopped, as a
    stop at the wrong moment or a crash leaves -- still holds its stderr: the group is
    killed as soon as the child ends, so the thread neither waits for that pipe's end until
    the limit nor hangs after it."""
    with_boot(
        monkeypatch,
        """\
        import subprocess, sys
        subprocess.Popen([sys.executable, "-c", "import time; time.sleep(600)"])
        """,
    )
    refresh = Refresh(home, tmp_path, [_X], limit=120.0)
    began = time.monotonic()
    try:
        refresh.start()
        assert refresh.wait(15), "the refresh waited on the pipe its child's child held"
        assert time.monotonic() - began < 15
        assert refresh.outcome == "failed"
        assert refresh.pid is not None
        assert not empty_within(refresh.pid, 5)
        [warning] = said(warnings, logging.WARNING)
        assert warning.endswith("was not drawn: the mock runs saved no graph for it")
    finally:
        kill_session(refresh.pid)


@pytest.mark.skipif(not Path("/proc/self/stat").exists(), reason="reads /proc")
def test_a_child_killed_mid_run_takes_its_test_server_with_it(
    test_server: None, home: Path, warnings: pytest.LogCaptureFixture
) -> None:
    """SIGKILL to the child alone (the OOM killer, a segfault) while its test server runs:
    the group is killed at once, the refresh ends well within its limit, and that is one
    WARNING."""
    wanted = wanted_of(PROJECTS / "stuck")
    refresh = Refresh(home, PROJECTS / "stuck", wanted, limit=120.0)
    refresh.start()
    try:
        end = time.monotonic() + _PATIENCE
        while not (
            refresh.pid is not None
            and any("temporal-test-server" in cmd for _, cmd in session(refresh.pid))
        ):
            assert time.monotonic() < end, "the child never started its test server"
            assert refresh.outcome is None, said(warnings, logging.WARNING)
            time.sleep(0.2)
        os.kill(refresh.pid, signal.SIGKILL)
        assert refresh.wait(15), "the refresh outlived its killed child"
        assert refresh.outcome == "failed"
        assert not empty_within(refresh.pid, 5)  # the test server went with it
        [warning] = said(warnings, logging.WARNING)
        assert warning.startswith(
            "the graph page's static graph of acme/stuck@1.0.0 was not drawn: the mock runs "
            "were killed by SIGKILL ("
        )
    finally:
        kill_session(refresh.pid)


@pytest.mark.skipif(not Path("/proc/self/stat").exists(), reason="reads /proc")
def test_the_child_dies_with_its_worker_however_the_worker_ends(home: Path, tmp_path: Path) -> None:
    """The worker dies without running a line of its own (SIGKILL here; a closed terminal's
    SIGHUP or the OOM killer alike): the child's lifeline, its stdin, whose other end only
    the worker held, reads EOF, and the child kills its whole group -- here a child of its
    own, as its test server would be -- instead of running on with nothing left to stop it."""
    project = project_with(
        tmp_path / "slow",
        textwrap.dedent(
            """\
            import subprocess, sys, time
            subprocess.Popen([sys.executable, "-c", "import time; time.sleep(600)"])
            time.sleep(600)
            """
        ),
    )
    script = textwrap.dedent(
        f"""\
        import time
        from pathlib import Path
        from kontra._graphs import Refresh, Wanted
        w = Wanted("acme/x@1.0.0", "acme/x@1.0.0", "sha256:x", "sha256:code")
        r = Refresh(Path({str(home)!r}), Path({str(project)!r}), [w])
        r.start()
        while r.pid is None:
            time.sleep(0.05)
        print(r.pid, flush=True)
        time.sleep(600)
        """
    )
    worker = subprocess.Popen([sys.executable, "-c", script], stdout=subprocess.PIPE, text=True)
    child: int | None = None
    try:
        assert worker.stdout is not None
        child = int(worker.stdout.readline())
        wait_for_session(child, at_least=2)  # the child, and the process it started
        worker.kill()
        worker.wait(10)
        assert not empty_within(child, 10)
    finally:
        worker.kill()
        worker.wait(10)
        kill_session(child)


def test_what_the_child_writes_is_kept_small_however_much_it_is() -> None:
    """The worker keeps the last 64 KiB of the child's stderr and its first lines of advice,
    each cut short: 60 MB, in long lines and short ones, cost it no more."""
    keep = _graphs._Said()  # pyright: ignore[reportPrivateUsage]
    long_advice = b"warning: " + b"a" * (1 << 20) + b"\n"
    for _ in range(40):
        for start in range(0, len(long_advice), 65536):
            keep.feed(long_advice[start : start + 65536])
        keep.feed(b"x\n" * 250_000)
    keep.feed(b"error: the last line")
    assert keep.last() == "error: the last line"
    assert len(keep.advice) == _graphs._ADVICE_MAX  # pyright: ignore[reportPrivateUsage]
    assert all(len(a) <= _graphs._SAID for a in keep.advice)  # pyright: ignore[reportPrivateUsage]
    assert keep.advice[0] == "a" * (_graphs._SAID - 3) + "..."  # pyright: ignore[reportPrivateUsage]
    held = keep.__dict__
    assert len(held["_tail"]) <= _graphs._TAIL  # pyright: ignore[reportPrivateUsage]
    assert len(held["_line"]) <= _graphs._LINE_MAX  # pyright: ignore[reportPrivateUsage]


def test_a_chatty_child_s_advice_is_logged_bounded(
    home: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, warnings: pytest.LogCaptureFixture
) -> None:
    """A child that draws, then writes far more advice than anyone reads (seller code that
    logs to stderr in a loop): the worker logs its first lines, each cut short; and a child
    that floods stderr and fails is quoted by its last line, cut short."""
    w = _X
    with_boot(
        monkeypatch,
        f"""\
        import os
        from pathlib import Path
        from kontra._graphs import save_static
        save_static(
            Path(os.environ["KONTRA_HOME"]), {w.task_queue!r}, offering={w.offering!r},
            contract_hash={w.contract_hash!r}, code_hash={w.code_hash!r}, runs=8, seed=0,
            graph={{"workflow": {{}}, "nodes": [], "edges": []}},
        )
        line = b"warning: " + b"b" * 200_000 + b"\\n"
        for _ in range(100):
            os.write(2, line)
        """,
    )
    assert run(Refresh(home, tmp_path, [w])) == "drawn", said(warnings, logging.WARNING)
    advice = [m for m in said(warnings, logging.INFO) if m.startswith("the mock runs said: ")]
    assert len(advice) == _graphs._ADVICE_MAX  # pyright: ignore[reportPrivateUsage]
    assert all(len(m) <= len("the mock runs said: ") + _graphs._SAID for m in advice)  # pyright: ignore[reportPrivateUsage]

    warnings.clear()
    with_boot(
        monkeypatch,
        """\
        import os, sys
        for _ in range(20):
            os.write(2, b"noise " * 200_000 + b"\\n")
        os.write(2, b"error: " + b"z" * 5000 + b"\\n")
        sys.exit(3)
        """,
    )
    other = Wanted("acme/y@1.0.0", "acme/y@1.0.0", "sha256:y", "sha256:code")
    assert run(Refresh(home, tmp_path, [other])) == "failed"
    [warning] = said(warnings, logging.WARNING)
    assert "the mock runs exited with status 3 (error: zzz" in warning
    assert len(warning) < 500


def test_one_refresh_at_a_time_per_home_and_the_second_finds_the_first_s_graph(
    home: Path, tmp_path: Path
) -> None:
    """While another refresh of this home holds the lock, a refresh waits; when the graph
    it wanted was drawn meanwhile, it spawns nothing."""
    w = _X
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
            code_hash=w.code_hash,
            runs=8,
            seed=0,
            graph={"workflow": {}, "nodes": [], "edges": []},
        )
    assert waiting.wait(10)
    assert (waiting.outcome, waiting.pid) == ("current", None)
    assert os.stat(home / "graphs" / ".drawing.lock").st_mode & 0o077 == 0
