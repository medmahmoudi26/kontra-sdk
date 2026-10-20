"""An agent whose loop is a process, not workflow code: Claude Code (`claude -p`) runs the
whole loop -- model calls and tool calls -- and this one activity drives it. No agent SDK,
and no `kontra`: a plain Temporal activity. The shape fits any agent that is a binary.

Durable the Temporal way. The activity heartbeats once per line the binary prints (its
`stream-json` events; the buyer sees them as progress) and returns on the `result` event. The
binary keeps its own session on this machine, named after the job's workflow, so a retry
does not start over: attempt 1 starts it with `--session-id`, and a retry -- after the worker
crashed, say -- passes `--resume` with the same id, and the binary carries on from its last
turn. Whether this machine holds the session is the binary's to say: when it refuses the
one, the attempt runs the other. A retry on a machine that never saw the session (another
worker of a fleet, a home that did not survive) starts it there; an attempt 1 that finds it
(a reset workflow) resumes it.

The buyer writes part of the prompt, so the binary reads it on stdin (the command line is
any local user's to read), gets one tool that touches nothing on this machine (`WebSearch`;
`--tools` takes the others away, Bash and Read among them), none of your MCP servers
(`--strict-mcp-config`), none of your own Claude Code setup -- CLAUDE.md, hooks, plugins,
skills (`--safe-mode`) -- and a turn limit, and runs in a directory only you can write. On
Linux it dies with the worker, even a SIGKILLed one."""

import asyncio
import contextlib
import json
import os
import shutil
import sys
import uuid
from pathlib import Path
from typing import Any

from models import CookInput, Dish
from temporalio import activity
from temporalio.exceptions import ApplicationError

#: What the binary may use, and is allowed to without asking.
TOOLS = "WebSearch"
MAX_TURNS = 8
#: Where it runs. Its sessions are kept per working directory, so every attempt runs here, and
#: it would run the hooks of a `.claude/settings.json` it found here: a directory this user
#: owns and no one else may write, never one under /tmp.
KITCHEN = Path.home() / ".kontra-chef-claude"
#: While the binary is quiet (a long model turn), a heartbeat this often: it is alive...
KEEPALIVE = 5.0
#: ...until it has been quiet this long: then it is stuck, and the attempt fails (a retry
#: resumes the session).
QUIET = 300.0
#: The longest line it may print (a tool result can be long).
LINE_LIMIT = 16 * 1024 * 1024
#: How much of what it says on stderr, or in a failed result, an error keeps.
SAID = 2000
#: How long it may take to exit after its result (it writes its session down).
GRACE = 10.0
PANTRY = (
    "fridge: eggs, butter, parmesan, lemons; dry: spaghetti, rice, flour; "
    "spices: black pepper, salt, chili flakes"
)
#: What a resumed session is told: the conversation so far is the binary's.
RESUME = "You were interrupted. Carry on from where you stopped and give the dish."
#: On Linux the binary starts through this, which has the kernel kill it when the worker dies
#: (PR_SET_PDEATHSIG), even by SIGKILL: an orphan would go on spending tokens, and writing the
#: session the retry resumes.
TIED = (
    "import ctypes, os, signal, sys\n"
    "ctypes.CDLL(None).prctl(1, int(signal.SIGKILL))\n"
    "if os.getppid() != int(sys.argv[1]):\n"
    "    sys.exit(1)\n"  # the worker died before that: nothing to tie to
    "os.execv(sys.argv[2], sys.argv[2:])\n"
)


class Refused(Exception):
    """The binary did not take the session: `--resume` of one this machine does not hold, or
    `--session-id` of one it does. It never said `init`."""


def claude() -> str:
    """The binary: `claude` on this worker's PATH, else where its installer puts it."""
    return shutil.which("claude") or "/usr/local/bin/claude"


def session_id(workflow_id: str) -> str:
    """The job's session: the same for every attempt of it, and for no other job."""
    return str(uuid.uuid5(uuid.NAMESPACE_URL, workflow_id))


def prompt(inp: CookInput) -> str:
    return (
        "You are a chef. The pantry holds exactly this -- "
        f"{PANTRY}. Propose one dish for the craving below that uses only what is there, "
        f"in a few short steps.\n\nCraving: {inp.craving}"
    )


def kitchen() -> Path:
    """KITCHEN, made if need be; refused unless it is this user's and no one else may write
    it."""
    KITCHEN.mkdir(mode=0o700, exist_ok=True)
    found = KITCHEN.stat()
    if found.st_uid != os.getuid() or found.st_mode & 0o022:
        raise ApplicationError(
            f"{KITCHEN} must be this user's and writable by no one else: chmod 700 it"
        )
    return KITCHEN


@activity.defn
async def cook(inp: CookInput) -> Dish:
    """Run `claude -p` on the craving until its `result` event."""
    info = activity.info()
    session = session_id(info.workflow_id or info.activity_id)  # run alone: the activity's
    where = kitchen()
    resume = info.attempt > 1
    try:
        result = await drive(where, session, resume, RESUME if resume else prompt(inp))
    except Refused:  # this machine holds the session after all, or does not: the other way
        resume = not resume
        try:
            result = await drive(where, session, resume, RESUME if resume else prompt(inp))
        except Refused as e:
            raise ApplicationError(f"claude took session {session} neither way: {e}") from None
    return _dish(result)


async def drive(cwd: Path, session: str, resume: bool, said: str) -> dict[str, Any]:
    """One `claude -p`, told `said` on stdin, until its `result` event."""
    argv = [
        claude(),
        "-p",
        "--safe-mode",
        "--output-format",
        "stream-json",
        "--verbose",
        "--tools",
        TOOLS,
        "--allowedTools",
        TOOLS,
        "--strict-mcp-config",
        "--max-turns",
        str(MAX_TURNS),
        "--resume" if resume else "--session-id",
        session,
    ]
    if sys.platform == "linux":
        argv = [sys.executable, "-c", TIED, str(os.getpid()), *argv]
    proc = await asyncio.create_subprocess_exec(
        *argv,
        cwd=cwd,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        limit=LINE_LIMIT,
    )
    assert proc.stdin is not None  # piped, above
    assert proc.stdout is not None
    assert proc.stderr is not None
    errors = asyncio.create_task(_tail(proc.stderr))
    loop = asyncio.get_running_loop()
    progress: dict[str, Any] = {"events": 0, "last": None}
    began = False
    try:
        with contextlib.suppress(ConnectionError):  # it may exit unread: a refusal
            proc.stdin.write(said.encode())
            await proc.stdin.drain()
        proc.stdin.close()
        heard = loop.time()
        while True:
            try:
                line = await asyncio.wait_for(proc.stdout.readline(), KEEPALIVE)
            except TimeoutError:
                if loop.time() - heard > QUIET:
                    raise ApplicationError(f"claude was silent for {QUIET:.0f}s") from None
                activity.heartbeat(progress)  # quiet, not dead
                continue
            if not line:
                break  # it closed its output without a result
            heard = loop.time()
            try:
                event = json.loads(line)
            except ValueError:
                continue
            kind = event.get("type") if isinstance(event, dict) else None
            began = began or (kind == "system" and event.get("subtype") == "init")
            progress = {"events": progress["events"] + 1, "last": kind}
            activity.heartbeat(progress)
            if kind == "result":
                if not began:
                    raise Refused(_said(event))
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(proc.wait(), GRACE)
                return event
        code = await proc.wait()
        tail = (await errors).decode(errors="replace")
        if not began:
            raise Refused(tail)
        raise ApplicationError(f"claude exited {code} without a result: {tail}")
    finally:
        if proc.returncode is None:
            proc.kill()  # cancelled, timed out, stuck, or done: never leave it running
        await proc.wait()
        errors.cancel()


async def _tail(stream: asyncio.StreamReader) -> bytes:
    """All of `stream`, keeping the last SAID bytes of it."""
    kept = b""
    while chunk := await stream.read(65536):
        kept = (kept + chunk)[-SAID:]
    return kept


def _said(result: dict[str, Any]) -> str:
    """What a `result` event says, for an error."""
    return str(result.get("errors") or result.get("result") or "")[:SAID]


def _dish(result: dict[str, Any]) -> Dish:
    """The `result` event: the dish, or why there is none."""
    if result.get("subtype") == "error_max_turns":
        raise ApplicationError(
            f"claude needed more than {MAX_TURNS} turns", type="MaxTurns", non_retryable=True
        )
    if result.get("is_error") or result.get("subtype") != "success":
        raise ApplicationError(f"claude stopped ({result.get('subtype')}): {_said(result)}")
    return Dish(recipe=str(result.get("result", "")))
