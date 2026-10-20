"""The graph page's static graphs (decisions I27, I28, I35): where they are kept, and how a
worker serving under a dev profile draws them.

Temporal keeps no definition of a workflow (I26), and the page is served by a process that
has no access to the seller's project, so the whole workflow -- the branches a run never
takes included -- comes from mock runs of the seller's code, saved under
``$KONTRA_HOME/graphs/`` by the task queue ``kontra serve`` serves the offering on. Each file
records the contract hash of the code it was drawn from.

``kontra serve`` (and ``python worker.py``) under a dev profile is the one process that
holds the seller's code anyway, so once its hash lock passed, :class:`~kontra.plugin.KontraPlugin`
compares each workflow offering's ``contract_hash()`` with the one stored for its task queue
and, when they differ or nothing is stored, starts a :class:`Refresh`: a daemon thread that
runs the hidden ``kontra graph`` command in a child process, at low priority and with a hard
time limit. The mock runs (the time-skipping test server, the sandboxed re-imports, the
sampled answers) never share the process that serves paid jobs; a failure is one log line,
never a refusal to serve. A handler offering has no static graph and is left out.

Nothing here imports Temporal, click or the serving side at module level: ``kontra
--help`` imports this module, and so does the page's server.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import logging
import os
import re
import signal
import stat
import subprocess
import sys
import tempfile
import threading
import time
from collections.abc import Generator, Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from kontra.graph import Graph

__all__ = [
    "GRAPHS_DIR",
    "RUNS",
    "RUN_TIMEOUT",
    "SEED",
    "STATIC_VERSION",
    "Refresh",
    "StaticGraph",
    "Wanted",
    "child_argv",
    "load_static",
    "out_of_date",
    "read_static",
    "save_static",
    "static_path",
]

log = logging.getLogger("kontra.serve")

#: Where the static graphs live, under ``$KONTRA_HOME``.
GRAPHS_DIR = "graphs"
#: The document's version: 2 records the contract hash (I35), so a version-1 file reads as
#: absent and is drawn again.
STATIC_VERSION = 2
#: The largest static graph :func:`read_static` reads.
_STATIC_MAX = 16 * 1024 * 1024
#: The mock runs a refresh asks for: ``kontra graph``'s defaults, so what it stores is the
#: graph every example's README shows.
RUNS = 8
SEED = 0
#: Wall-clock seconds one mock run may take (``kontra graph --timeout``).
RUN_TIMEOUT = 5.0
#: Seconds a child may take beyond its runs' own deadlines -- the imports, the test
#: server's start and, the first time, its download -- before it is stopped.
START_GRACE = 120.0
#: Seconds a child asked to stop has to shut its test server down before it is killed.
STOP_GRACE = 5.0
#: How much lower than the worker's the child's scheduling priority is.
_NICE = 10
#: One refresh at a time per home: the lock file under :data:`GRAPHS_DIR`, and how often a
#: refresh waiting for it looks again.
_LOCK_FILE = ".drawing.lock"
_LOCK_POLL = 0.5
#: How ``kontra graph`` starts each line of advice on stderr.
_ADVICE = "warning: "
#: The longest line of the child's a warning quotes.
_SAID = 300
_CONTROL = re.compile(r"[\x00-\x1f\x7f-\x9f]")
#: The child: ``kontra graph`` (the CLI's own ``main``), at a lower priority. ``-P`` keeps
#: the working directory off ``sys.path``, as the ``kontra`` script does.
_BOOT = (
    "import os\n"
    "if hasattr(os, 'nice'):\n"
    f"    os.nice({_NICE})\n"
    "from kontra.cli import main\n"
    "main()\n"
)


# --------------------------------------------------------------------------- the store


@dataclass(frozen=True)
class StaticGraph:
    """A saved static graph: what :func:`read_static` gives back."""

    task_queue: str
    offering: str
    #: The contract hash of the code the mock runs ran (``BoundOffering.contract_hash()``).
    contract_hash: str
    runs: int
    seed: int
    graph: Graph


def static_path(home: Path, task_queue: str) -> Path:
    """Where the static graph of the offering served on ``task_queue`` lives: named by the
    queue's SHA-256, so no part of the name comes from anyone's text."""
    return home / GRAPHS_DIR / f"{hashlib.sha256(task_queue.encode()).hexdigest()}.json"


def _graphs_dir(home: Path) -> Path:
    """``home``'s graphs directory, made 0700 when it is created."""
    directory = home / GRAPHS_DIR
    if not directory.is_dir():
        directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        os.chmod(directory, 0o700)  # mkdir's mode is filtered by the umask
    return directory


def save_static(
    home: Path,
    task_queue: str,
    *,
    offering: str,
    contract_hash: str,
    runs: int,
    seed: int,
    graph: Graph,
) -> Path:
    """Write the static graph of the offering served on ``task_queue`` (``kontra serve``'s
    task queue for it, exactly), drawn from code whose contract hash is ``contract_hash``,
    atomically: a temporary file, mode 0600, renamed into place, in a directory made 0700.
    Two writers at once each replace the whole file: the last one wins, and the file is
    always one of them."""
    path = static_path(home, task_queue)
    _graphs_dir(home)
    document = {
        "version": STATIC_VERSION,
        "task_queue": task_queue,
        "offering": offering,
        "contract_hash": contract_hash,
        "runs": runs,
        "seed": seed,
        "graph": graph,
    }
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}-")
    try:
        os.chmod(tmp, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(document, f, indent=2, sort_keys=True)
            f.write("\n")
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    finally:
        with contextlib.suppress(FileNotFoundError):
            os.unlink(tmp)
    return path


def _is_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def read_static(home: Path, task_queue: str) -> StaticGraph | None:
    """The static graph saved for ``task_queue``, with what it was drawn from, or None: no
    file, one that is not a regular file, too large, not JSON, not this version, or saved
    for another task queue (the stored ``task_queue`` must equal the given one exactly; the
    file name is only its hash)."""
    path = static_path(home, task_queue)
    flags = os.O_RDONLY | getattr(os, "O_NONBLOCK", 0) | getattr(os, "O_CLOEXEC", 0)
    try:
        fd = os.open(path, flags)  # non-blocking: a FIFO planted there cannot hang the page
    except OSError:
        return None
    try:
        with os.fdopen(fd, "rb") as f:
            if not stat.S_ISREG(os.fstat(f.fileno()).st_mode):
                return None
            raw = f.read(_STATIC_MAX + 1)
    except OSError:
        return None
    if len(raw) > _STATIC_MAX:
        return None
    try:
        document: object = json.loads(raw)
    except (ValueError, RecursionError):
        return None
    if not isinstance(document, dict):
        return None
    found: dict[str, Any] = document  # pyright: ignore[reportUnknownVariableType]
    saved_graph, offering = found.get("graph"), found.get("offering")
    drawn_from, runs, seed = found.get("contract_hash"), found.get("runs"), found.get("seed")
    if (
        found.get("version") != STATIC_VERSION
        or found.get("task_queue") != task_queue
        or not isinstance(saved_graph, dict)
        or not isinstance(offering, str)
        or not isinstance(drawn_from, str)
        or not drawn_from
        or not _is_int(runs)
        or not _is_int(seed)
    ):
        return None
    return StaticGraph(
        task_queue=task_queue,
        offering=offering,
        contract_hash=drawn_from,
        runs=runs,  # pyright: ignore[reportArgumentType]
        seed=seed,  # pyright: ignore[reportArgumentType]
        graph=saved_graph,  # pyright: ignore[reportUnknownArgumentType]
    )


def load_static(home: Path, task_queue: str) -> Graph | None:
    """The graph alone of :func:`read_static` (what the page paints a run on)."""
    found = read_static(home, task_queue)
    return found.graph if found is not None else None


# --------------------------------------------------------------------------- the refresh


@dataclass(frozen=True)
class Wanted:
    """A workflow offering whose static graph should be the one of its current code."""

    #: ``acme/hello@0.1.0``: what ``kontra graph --offering`` picks it by.
    offering: str
    task_queue: str
    #: ``BoundOffering.contract_hash()`` of the code being served.
    contract_hash: str


def out_of_date(home: Path, wanted: Iterable[Wanted]) -> list[Wanted]:
    """Those of ``wanted`` with no static graph stored, or one drawn from another contract."""
    out: list[Wanted] = []
    for w in wanted:
        found = read_static(home, w.task_queue)
        if found is None or found.contract_hash != w.contract_hash:
            out.append(w)
    return out


def child_argv(project: Path, offerings: Sequence[str], *, timeout: float) -> list[str]:
    """The child a refresh runs: ``kontra graph --dir <project> --offering <each>`` with the
    defaults the READMEs' graphs are drawn with, in this interpreter."""
    argv = [sys.executable, "-P", "-c", _BOOT, "graph", "--dir", str(project)]
    argv += ["--runs", str(RUNS), "--seed", str(SEED), "--timeout", repr(float(timeout))]
    for key in offerings:
        argv += ["--offering", key]
    return argv


def _child_env(home: Path) -> dict[str, str]:
    """The worker's environment, its home made explicit and with no API key: the mock runs
    talk to no gate."""
    from kontra.auth import API_KEY_ENV

    env = {k: v for k, v in os.environ.items() if k != API_KEY_ENV}
    env["KONTRA_HOME"] = str(home)
    return env


def _last_line(err: bytes | None) -> str:
    """The child's last line on stderr (its ``error: ...``, or an exception's last line),
    printable and cut short."""
    lines = [ln.strip() for ln in (err or b"").decode("utf-8", "replace").splitlines()]
    said = _CONTROL.sub("", next((ln for ln in reversed(lines) if ln), "no output"))
    return said if len(said) <= _SAID else said[: _SAID - 3] + "..."


def _said(e: BaseException) -> str:
    said = _CONTROL.sub("", str(e)).strip()
    return said[:_SAID] or type(e).__name__


@contextlib.contextmanager
def _one_at_a_time(home: Path, stopping: threading.Event) -> Generator[bool]:
    """Hold ``home``'s refresh lock for the block (yields False when ``stopping`` was set
    while it waited): two workers of one home draw one after the other, so one test server
    runs at a time, and the second finds the graph the first drew. Without ``fcntl`` there
    is no lock."""
    try:
        import fcntl
    except ImportError:  # pragma: no cover -- not POSIX
        yield True
        return
    flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(_graphs_dir(home) / _LOCK_FILE, flags, 0o600)
    try:
        held = False
        while not held:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                held = True
            except BlockingIOError:
                if stopping.wait(_LOCK_POLL):
                    break
        try:
            yield held
        finally:
            if held:
                fcntl.flock(fd, fcntl.LOCK_UN)
    finally:
        os.close(fd)


def _signal_group(proc: subprocess.Popen[bytes], sig: int) -> None:
    """``sig`` to the child's process group (its test server is in it), while the child is
    not reaped yet (so the group is still ours); to the child alone without process groups."""
    if proc.poll() is not None:
        return
    with contextlib.suppress(ProcessLookupError, PermissionError):
        if hasattr(os, "killpg"):
            os.killpg(proc.pid, sig)
        else:  # pragma: no cover -- not POSIX
            proc.kill()


def _end(proc: subprocess.Popen[bytes]) -> None:
    """Stop the child: SIGTERM, which ``kontra graph`` takes as Ctrl-C (it shuts its test
    server down), then, after :data:`STOP_GRACE`, SIGKILL to its whole group."""
    if proc.poll() is not None:
        return
    with contextlib.suppress(ProcessLookupError):
        proc.terminate()
    try:
        proc.wait(STOP_GRACE)
    except subprocess.TimeoutExpired:
        _signal_group(proc, getattr(signal, "SIGKILL", signal.SIGTERM))
        with contextlib.suppress(subprocess.TimeoutExpired):
            proc.wait(STOP_GRACE)


class Refresh:
    """The static graphs of ``wanted``, drawn by ``kontra graph`` in a child process from a
    daemon thread. :meth:`start` returns at once; the thread waits for this home's refresh
    lock, looks again at what is out of date (another worker may have drawn it meanwhile),
    spawns the child and waits for it, up to ``limit`` seconds (default: :data:`START_GRACE`
    plus every run's deadline), then stops it. What became of it is one log line: INFO when
    drawn, WARNING when not -- never an exception. :meth:`stop` ends the child; the thread is
    never joined."""

    def __init__(
        self,
        home: Path,
        project: Path,
        wanted: Sequence[Wanted],
        *,
        timeout: float = RUN_TIMEOUT,
        limit: float | None = None,
    ) -> None:
        self.home = home
        self.project = project
        self.wanted = tuple(wanted)
        self.timeout = timeout
        self.limit = limit if limit is not None else START_GRACE + RUNS * timeout * len(self.wanted)
        #: ``drawn``, ``current`` (drawn meanwhile), ``failed`` or ``stopped``; None until the
        #: thread is done.
        self.outcome: str | None = None
        #: The child's pid, once spawned.
        self.pid: int | None = None
        self._mutex = threading.Lock()
        self._stopping = threading.Event()
        self._done = threading.Event()
        self._proc: subprocess.Popen[bytes] | None = None

    def start(self) -> None:
        threading.Thread(target=self._run, name="kontra-graph-refresh", daemon=True).start()

    def wait(self, timeout: float | None = None) -> bool:
        """Whether the thread is done within ``timeout`` seconds (for tests)."""
        return self._done.wait(timeout)

    def stop(self) -> None:
        """End the child, if it runs (blocks up to twice :data:`STOP_GRACE`); a refresh not
        spawned yet never spawns."""
        with self._mutex:
            self._stopping.set()
            proc = self._proc
        if proc is not None:
            _end(proc)

    def _what(self, wanted: Sequence[Wanted] | None = None) -> str:
        keys = ", ".join(w.offering for w in (wanted if wanted is not None else self.wanted))
        return f"the graph page's static graph of {keys}"

    def _run(self) -> None:
        try:
            self.outcome = self._draw()
        except Exception as e:  # a log line, never a crash
            self.outcome = "failed"
            log.warning("%s was not drawn: %s", self._what(), _said(e))
        finally:
            self._done.set()

    def _draw(self) -> str:
        with _one_at_a_time(self.home, self._stopping) as held:
            if not held:
                return "stopped"
            wanted = out_of_date(self.home, self.wanted)
            if not wanted:
                return "current"
            what = self._what(wanted)
            argv = child_argv(self.project, [w.offering for w in wanted], timeout=self.timeout)
            with self._mutex:
                if self._stopping.is_set():
                    return "stopped"
                try:
                    proc = subprocess.Popen(  # noqa: S603 -- this interpreter, argv, no shell
                        argv,
                        stdin=subprocess.DEVNULL,
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.PIPE,
                        env=_child_env(self.home),
                        start_new_session=hasattr(os, "setsid"),
                    )
                except OSError as e:
                    log.warning(
                        "%s was not drawn: the mock runs did not start (%s)", what, _said(e)
                    )
                    return "failed"
                self._proc, self.pid = proc, proc.pid
            log.info(
                "drawing %s in the background: mock runs in a child process (pid %d)",
                what,
                proc.pid,
            )
            began = time.monotonic()
            try:
                _, err = proc.communicate(timeout=self.limit)
            except subprocess.TimeoutExpired:
                _end(proc)
                proc.communicate()
                if self._stopping.is_set():
                    return "stopped"
                log.warning(
                    "%s was not drawn: the mock runs took longer than %gs and were stopped",
                    what,
                    self.limit,
                )
                return "failed"
            if self._stopping.is_set():
                return "stopped"
            if proc.returncode != 0:
                log.warning(
                    "%s was not drawn: the mock runs exited with status %d (%s)",
                    what,
                    proc.returncode,
                    _last_line(err),
                )
                return "failed"
            if out_of_date(self.home, wanted):
                log.warning("%s was not drawn: the mock runs saved no graph for it", what)
                return "failed"
            log.info("drew %s (%d mock runs each, %.1fs)", what, RUNS, time.monotonic() - began)
            # What the runs could not show (never scheduled, open runs, unsampleable
            # returns): advice for the seller, as kontra graph prints it.
            for line in (err or b"").decode("utf-8", "replace").splitlines():
                if line.startswith(_ADVICE):
                    log.info("the mock runs said: %s", _CONTROL.sub("", line[len(_ADVICE) :]))
            return "drawn"
