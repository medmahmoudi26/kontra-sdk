"""``kontra mcp`` end to end (PRD §7.3, acceptance 12): an MCP client session drives the
server, which hires through :class:`kontra.hire.Kontra` from a marketplace (dev server, fake
gate) where a real :class:`kontra.serve.Worker` sells ``acme/words``.

The in-process client speaks JSON-RPC over memory streams (``mode="legacy"``) and checks
every structured answer against the tool's published output schema; one test runs
``python -m kontra.mcp`` as a subprocess over stdio, as an agent's host would."""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import os
import stat
import sys
import uuid
from collections.abc import AsyncGenerator, AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any, TypeVar

import httpx
import jsonschema
import pytest
import pytest_asyncio
from mcp import Client, StdioServerParameters
from mcp.client.stdio import stdio_client
from mcp.types import CallToolResult
from mcp_flows import FAIL_MESSAGE
from mcp_seller import (
    DOCS,
    NAME,
    REJECT_REASON,
    REJECTED,
    docs_offering,
    words_offering,
    words_worker,
)

from kontra.gate_client import models as m
from kontra.hire import Kontra
from kontra.ledger import PLATFORM_PARTY
from kontra.mcp import KontraTools, server
from kontra.mcp import _server as mcp_server
from kontra.profile import Profile
from kontra.serve.progress import INTERVAL_ENV
from kontra.testing import Marketplace, TestParty
from kontra.testing.devserver import DevServer
from kontra.testing.ledger_contract import assert_invariants

pytestmark = [pytest.mark.temporal, pytest.mark.asyncio(loop_scope="module")]

_T = TypeVar("_T")
#: PRD §7.3's eight, and a seller's ``workers`` (kontra-cloud#6, decisions I21).
TOOLS = {"search", "describe", "quote", "hire", "status", "result", "accept", "reject", "workers"}
START = 10_000  # every buyer starts with 100.00 USD
TEXT = "word " * 20  # 100 characters: 1.00 USD, a 0.05 fee
EXAMPLE = {"text": "the quick brown fox jumps over the lazy dog the end"}


# --------------------------------------------------------------------------- the world


@dataclass
class World:
    mp: Marketplace
    seller: TestParty
    key: str  # the exact offering, acme/words@<version>

    async def buyer(self, balance: str = "100.00") -> TestParty:
        return await self.mp.party(f"bob-{uuid.uuid4().hex[:8]}", balance=balance)

    def kontra(self, party: TestParty) -> Kontra:
        return party.kontra(client=self.mp.client(party.namespace))

    async def ledger(self, party: str) -> int:
        gate = self.mp.gate

        async def call() -> int:
            return (await gate.ledger.balance(party, "USD")).available_minor

        return await gate.run(call)


@pytest_asyncio.fixture(loop_scope="module", scope="module")
async def world(
    dev_server: DevServer, tmp_path_factory: pytest.TempPathFactory
) -> AsyncIterator[World]:
    previous = os.environ.get(INTERVAL_ENV)
    os.environ[INTERVAL_ENV] = "0"  # forward every heartbeat at once
    try:
        async with Marketplace(dev_server=dev_server, progress_rate_per_s=10_000) as mp:
            acme = await mp.party("acme")
            root = tmp_path_factory.mktemp("acme")
            bound = words_offering(root)
            async with mp.serve(words_worker(bound, docs_offering(root)), acme):
                yield World(mp, acme, bound.key)
            gate = mp.gate

            async def audit() -> None:
                await assert_invariants(gate.ledger)

            await gate.run(audit)  # sum(postings) == balances after everything here
    finally:
        if previous is None:
            os.environ.pop(INTERVAL_ENV, None)
        else:
            os.environ[INTERVAL_ENV] = previous


@contextlib.asynccontextmanager
async def connect(k: Kontra, **options: Any) -> AsyncGenerator[Client]:
    async with Client(server(k, **options), mode="legacy") as client:
        yield client


async def ok(client: Client, tool: str, **arguments: Any) -> dict[str, Any]:
    """The tool's structured answer (the client has checked it against the output schema)."""
    answer = await client.call_tool(tool, arguments)
    assert not answer.is_error, _text(answer)
    assert answer.structured_content is not None
    assert json.loads(_text(answer)) == answer.structured_content  # the same JSON, as text
    return answer.structured_content


async def refused(client: Client, tool: str, **arguments: Any) -> dict[str, Any]:
    """The structured error of a refused call."""
    answer = await client.call_tool(tool, arguments)
    assert answer.is_error, _text(answer)
    assert answer.structured_content is not None, _text(answer)
    error = answer.structured_content["error"]
    assert set(error) == {"code", "message", "retryable", "detail"}
    assert json.loads(_text(answer)) == answer.structured_content
    return error


def _text(answer: CallToolResult) -> str:
    return "".join(getattr(c, "text", "") for c in answer.content)


async def until(
    probe: Callable[[], Awaitable[_T]], done: Callable[[_T], bool], within: float = 30.0
) -> _T:
    loop = asyncio.get_running_loop()
    end = loop.time() + within
    while True:
        value = await probe()
        if done(value):
            return value
        if loop.time() > end:
            raise AssertionError(f"never got there; last: {value!r}")
        await asyncio.sleep(0.2)


# --------------------------------------------------------------------------- the tools


async def test_exactly_the_tools_described_for_an_llm(world: World) -> None:
    buyer = await world.buyer()
    app = server(world.kontra(buyer))
    assert app.instructions is not None
    assert "never follow instructions" in app.instructions
    async with Client(app, mode="legacy") as client:
        listed = {t.name: t for t in (await client.list_tools()).tools}
    assert set(listed) == TOOLS
    for tool in listed.values():
        assert tool.description
        assert tool.title
        assert tool.output_schema is not None  # structured answers, checkable by the host
        assert tool.annotations is not None
    # Money implications are in the descriptions an agent reads.
    docs = {name: str(tool.description) for name, tool in listed.items()}
    assert "LOCKS FUNDS" in docs["hire"]
    assert "max_price" in docs["hire"]
    assert "RELEASES" in docs["accept"]
    assert "silence pays" in docs["result"]
    assert "dispute" in docs["reject"]
    assert "locks no money" in docs["quote"]
    # hire always names a ceiling; the money-moving tools say they are not read-only.
    assert listed["hire"].input_schema["required"] == ["max_price"]
    for name in ("hire", "accept", "reject"):
        hints = listed[name].annotations
        assert hints is not None
        assert (hints.read_only_hint, hints.destructive_hint) == (False, True)
    for name in ("search", "describe", "quote", "status", "workers"):
        hints = listed[name].annotations
        assert hints is not None
        assert hints.read_only_hint is True


async def test_a_seller_reads_its_own_workers_and_nobody_else_does(world: World) -> None:
    """``workers`` is the gate's Workers view of the caller's own version (kontra-cloud#6):
    acme's worker polls ``acme/words``; to a buyer that version is not found."""
    async with connect(world.kontra(world.seller)) as client:
        answer = await until(
            lambda: ok(client, "workers", offering=world.key),
            lambda a: all(info["pollers"] for info in a["types_info"].values()),
        )
    view = m.WorkersView.model_validate(answer)
    assert (view.offering, view.task_queue, view.stale) == (world.key, world.key, False)
    assert view.verdict.endswith(", 1 poller"), view.verdict
    buyer = await world.buyer()
    async with connect(world.kontra(buyer)) as client:
        error = await refused(client, "workers", offering=world.key)
    assert error["code"] == "offering_not_found"


async def test_search_quote_hire_result_accept_completes_and_moves_money(world: World) -> None:
    """Acceptance 12."""
    buyer = await world.buyer()
    seller0, platform0 = await world.ledger("acme"), await world.ledger(PLATFORM_PARTY)
    async with connect(world.kontra(buyer)) as client:
        found = await ok(client, "search", query="words")
        [summary] = [o for o in found["offerings"] if o["offering"] == world.key]
        assert summary["pricing_hint"] == "1 cent per character"
        assert summary["seller"]["party"] == "acme"

        quoted = await ok(client, "quote", offering=NAME, input={"text": TEXT})
        assert (quoted["status"], quoted["offering"]) == ("ok", world.key)
        assert (quoted["price"], quoted["currency"]) == ("1.00", "USD")
        assert quoted["max_duration_seconds"] == 120
        assert quoted["quote_id"]
        assert quoted["expires_at"]
        assert (await buyer.balance()).held_minor == 0  # a quote locks nothing

        hired = await ok(client, "hire", quote_id=quoted["quote_id"], max_price=1)
        assert (hired["offering"], hired["price"], hired["currency"]) == (world.key, "1.00", "USD")
        assert hired["escrow"] == "held"
        assert hired["state"] in ("submitted", "working", "completed")
        assert hired["balance"] == {"currency": "USD", "available": "99.00", "held": "1.00"}
        job_id = hired["job_id"]

        got = await ok(client, "result", job_id=job_id)
        assert got["ready"] is True
        assert got["output"] == {
            "upper": TEXT.upper(),
            "words": 20,
            "counts": [{"word": "word", "n": 20}],
            "file": None,
            "files": {},
        }
        assert (got["artifacts"], got["output_file"]) == ([], None)
        assert (got["state"], got["escrow"]) == ("completed", "held")
        assert got["review_deadline"] is not None  # the window an agent must act in

        accepted = await ok(client, "accept", job_id=job_id)
        assert (accepted["state"], accepted["escrow"]) == ("completed", "released")
        assert accepted["delivery_sha256"] is not None

    after = await buyer.balance()
    assert (after.available_minor, after.held_minor) == (START - 100, 0)
    assert await world.ledger("acme") - seller0 == 95  # the price minus the 5% fee
    assert await world.ledger(PLATFORM_PARTY) - platform0 == 5


async def test_describe_returns_schemas_that_validate_the_example_input(world: World) -> None:
    buyer = await world.buyer()
    async with connect(world.kontra(buyer)) as client:
        described = await ok(client, "describe", offering=NAME)
        assert (described["offering"], described["seller"]) == (world.key, "acme")
        assert described["description"] == "Upper-cases a text and counts its words"
        assert described["tags"] == ["text", "words"]
        assert described["contract_hash"].startswith("sha256:")
        assert described["input_takes_files"] is False
        assert described["limits"] == {
            "max_duration": "2m",
            "max_duration_seconds": 120,
            "review_window": "1h",
            "review_window_seconds": 3600,
            "max_concurrent_jobs": None,
            "max_output_bytes": 1_000_000,
            "artifact_retention": "1d",
            "artifact_retention_seconds": 86_400,
        }

        inputs = jsonschema.Draft202012Validator(described["input_schema"])
        inputs.validate(EXAMPLE)  # an agent can build a valid input from it
        inputs.validate({"text": "x", "mode": "beats", "beats": 3})
        for bad in ({}, {"text": 5}, {"text": "x", "colour": "red"}, {"text": "x", "mode": "?"}):
            assert not inputs.is_valid(bad), bad

        # The output schema describes what result returns.
        hired = await ok(client, "hire", offering=NAME, input=EXAMPLE, max_price="1")
        got = await ok(client, "result", job_id=hired["job_id"])
        jsonschema.Draft202012Validator(described["output_schema"]).validate(got["output"])
        assert got["output"]["words"] == 11

        # An offering whose input is a file says so: this server cannot upload one.
        docs = await ok(client, "describe", offering=DOCS)
        assert docs["input_takes_files"] is True
        assert "KontraArtifact" in docs["input_schema"]["$defs"]

        # An input the schema refuses is refused before anything is sent.
        error = await refused(client, "quote", offering=NAME, input={"text": 5})
        assert error["code"] == "invalid_input"
        assert error["detail"]["path"] == "input.text"


async def test_reject_opens_a_dispute(world: World) -> None:
    buyer = await world.buyer()
    async with connect(world.kontra(buyer)) as client:
        hired = await ok(client, "hire", offering=NAME, input={"text": "meh"}, max_price="0.03")
        await ok(client, "result", job_id=hired["job_id"])
        rejected = await ok(
            client, "reject", job_id=hired["job_id"], reason="every letter is a capital"
        )
        assert (rejected["escrow"], rejected["reason"]) == ("disputed", "every letter is a capital")
        status = await ok(client, "status", job_id=hired["job_id"])
        assert status["escrow"] == "disputed"
    after = await buyer.balance()
    assert (after.available_minor, after.held_minor) == (START - 3, 3)  # held for the resolver


async def test_status_shows_the_latest_progress_decrypted(world: World) -> None:
    buyer = await world.buyer()
    k = world.kontra(buyer)
    async with connect(k) as client:
        # A job still running: its one heartbeat, read without waiting for the job to end.
        stuck = await ok(
            client, "hire", offering=NAME, input={"text": "wait", "mode": "stuck"}, max_price=1
        )

        async def heartbeat() -> dict[str, Any] | None:
            status = await ok(client, "status", job_id=stuck["job_id"])
            assert status["escrow"] == "held"
            beat = status["progress"]
            if beat is None or beat["source"] != "heartbeat":
                beat = status["last_heartbeat"]
            return beat

        beat = await until(heartbeat, lambda b: b is not None)
        assert beat is not None
        assert (beat["percent"], beat["message"]) == (50.0, "halfway there")
        assert beat["summary"] == "50% halfway there"
        assert beat["details"] == {"percent": 50, "message": "halfway there"}
        status = await ok(client, "status", job_id=stuck["job_id"])
        assert status["state"] == "working"
        assert status["deadline"] is not None
        await (await k.job(stuck["job_id"])).cancel()  # the test's cleanup, not a tool

        # A job that heartbeat five times and finished: the last of them, in order.
        beats = await ok(
            client,
            "hire",
            offering=NAME,
            input={"text": "count", "mode": "beats", "beats": 5},
            max_price=1,
        )
        await ok(client, "result", job_id=beats["job_id"])
        await until(
            lambda: ok(client, "status", job_id=beats["job_id"]),
            lambda s: s["state"] == "completed",
        )
        status = await ok(client, "status", job_id=beats["job_id"])
        last = status["progress"] if status["progress"]["source"] == "heartbeat" else None
        last = last or status["last_heartbeat"]
        assert (last["percent"], last["message"], last["summary"]) == (
            100.0,
            "beat 5",
            "100% beat 5",
        )


# --------------------------------------------------------------------------- refusals


async def test_max_price_refuses_to_lock(world: World) -> None:
    buyer = await world.buyer()
    async with connect(world.kontra(buyer)) as client:
        quoted = await ok(client, "quote", offering=NAME, input={"text": TEXT})
        error = await refused(client, "hire", quote_id=quoted["quote_id"], max_price="0.99")
        assert error["code"] == "price_above_max"
        assert error["retryable"] is False
        assert error["detail"]["price"] == "1.00"
        assert error["detail"]["max_price"] == "0.99"
        assert "nothing was locked" in error["message"]
        error = await refused(client, "hire", offering=NAME, input={"text": TEXT}, max_price=0.5)
        assert error["code"] == "price_above_max"
        held = await buyer.balance()
        assert (held.available_minor, held.held_minor) == (START, 0)

        # The quote is still good: a high enough ceiling hires it.
        hired = await ok(client, "hire", quote_id=quoted["quote_id"], max_price="1.00")
        assert hired["escrow"] == "held"
        # ... once: a hired quote is gone from this server.
        error = await refused(client, "hire", quote_id=quoted["quote_id"], max_price=5)
        assert error["code"] == "unknown_quote"


async def test_a_hire_whose_answer_was_lost_is_retried_by_its_quote_never_twice(
    world: World, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The gate locks the price, then the answer is lost. The agent must not hire again from
    a new quote (a second lock): the error names the quote, and hiring that quote again is
    refused by the gate with the job it already made -- whose key, the one the lost call
    locked it with, is filed under the job."""
    buyer = await world.buyer()
    k = world.kontra(buyer)
    real_hire = k.gate.hire
    sent: list[str] = []

    async def lost(
        quote_id: str, *, buyer_key: str, max_price_minor: int | None = None
    ) -> m.HireView:
        sent.append(buyer_key)
        await real_hire(quote_id, buyer_key=buyer_key, max_price_minor=max_price_minor)
        raise httpx.ReadError("the connection dropped before the answer")

    async with connect(k) as client:
        monkeypatch.setattr(k.gate, "hire", lost)
        error = await refused(client, "hire", offering=NAME, input={"text": TEXT}, max_price=1)
        assert (error["code"], error["retryable"]) == ("gate_unreachable", True)
        quote_id = error["detail"]["quote_id"]
        monkeypatch.setattr(k.gate, "hire", real_hire)

        error = await refused(client, "hire", quote_id=quote_id, max_price=1)
        assert error["code"] == "quote_used"
        job_id = error["detail"]["job_id"]
        assert Profile.job_identity(job_id, k.session.identity_home)[1] == sent[0]
        status = await ok(client, "status", job_id=job_id)
        assert (status["escrow"], status["price"]) == ("held", "1.00")
        held = await buyer.balance()
        assert (held.available_minor, held.held_minor) == (START - 100, 100)  # locked once
        cancelled = await k.gate.cancel(job_id)  # never started: the buyer's to cancel
        assert cancelled.escrow is m.Escrow.REFUNDED
    after = await buyer.balance()
    assert (after.available_minor, after.held_minor) == (START, 0)


async def test_a_rejected_quote(world: World) -> None:
    buyer = await world.buyer()
    async with connect(world.kontra(buyer)) as client:
        quoted = await ok(client, "quote", offering=NAME, input={"text": REJECTED})
        assert (quoted["status"], quoted["reason"]) == ("rejected", REJECT_REASON)
        assert (quoted["quote_id"], quoted["price"]) == (None, None)
        error = await refused(client, "hire", offering=NAME, input={"text": REJECTED}, max_price=1)
        assert error["code"] == "quote_rejected"
        assert error["detail"]["reason"] == REJECT_REASON
    assert (await buyer.balance()).held_minor == 0


async def test_unknown_jobs_quotes_and_offerings(world: World) -> None:
    buyer = await world.buyer()
    stranger = uuid.uuid4().hex  # a well-formed id no job has
    async with connect(world.kontra(buyer)) as client:
        for tool, extra in (
            ("status", {}),
            ("result", {}),
            ("accept", {}),
            ("reject", {"reason": "no"}),
        ):
            error = await refused(client, tool, job_id=stranger, **extra)
            assert (error["code"], error["detail"]["status"]) == ("job_not_found", 404), tool
            error = await refused(client, tool, job_id="../../etc", **extra)
            assert error["code"] == "invalid_job_id", tool

        error = await refused(client, "hire", quote_id="no-such-quote", max_price=1)
        assert error["code"] == "unknown_quote"
        error = await refused(client, "hire", offering=NAME, input={"text": TEXT}, max_price=0.5)
        assert error["code"] == "price_above_max"
        assert error["detail"]["quote_id"]  # a higher ceiling can hire this very quote
        both = {"quote_id": "q", "offering": NAME, "input": EXAMPLE}
        assert (await refused(client, "hire", max_price=1, **both))["code"] == "invalid_arguments"
        assert (await refused(client, "hire", max_price=1))["code"] == "invalid_arguments"

        error = await refused(client, "describe", offering="acme/nothing-here")
        assert error["code"] == "offering_not_found"
        error = await refused(client, "describe", offering="Not An Offering")
        assert error["code"] == "invalid_offering"

        # Arguments the tool's own schema refuses never reach the tool.
        answer = await client.call_tool("hire", {"quote_id": "q"})  # no max_price
        assert answer.is_error
        answer = await client.call_tool("hire", {"quote_id": "q", "max_price": -1})
        assert answer.is_error
    assert (await buyer.balance()).held_minor == 0


async def test_a_failed_job_is_refunded(world: World) -> None:
    buyer = await world.buyer()
    async with connect(world.kontra(buyer)) as client:
        hired = await ok(
            client, "hire", offering=NAME, input={"text": "boom", "mode": "fail"}, max_price=1
        )
        error = await refused(client, "result", job_id=hired["job_id"])
        assert error["code"] == "job_failed"
        assert error["detail"]["reason"] == FAIL_MESSAGE  # the seller's words, decrypted here
        assert error["detail"]["state"] == "failed"
        status = await until(
            lambda: ok(client, "status", job_id=hired["job_id"]),
            lambda s: s["escrow"] == "refunded",
        )
        assert status["state"] == "failed"
    after = await buyer.balance()
    assert (after.available_minor, after.held_minor) == (START, 0)


async def test_result_does_not_wait_for_ever(world: World) -> None:
    buyer = await world.buyer()
    k = world.kontra(buyer)
    async with connect(k) as client:
        hired = await ok(
            client, "hire", offering=NAME, input={"text": "zzz", "mode": "stuck"}, max_price=1
        )
        pending = await ok(client, "result", job_id=hired["job_id"], wait_seconds=1)
        assert pending["ready"] is False
        assert pending["state"] in ("submitted", "working")
        assert (pending["output"], pending["artifacts"]) == (None, [])
        assert "call result again" in pending["note"]
        await (await k.job(hired["job_id"])).cancel()


# --------------------------------------------------------------------------- files


async def test_files_are_downloaded_or_described_never_inlined(
    world: World, tmp_path: Path
) -> None:
    buyer = await world.buyer()
    async with connect(world.kontra(buyer)) as client:
        hired = await ok(
            client, "hire", offering=NAME, input={"text": "two files", "mode": "files"}, max_price=1
        )
        described = await ok(client, "result", job_id=hired["job_id"])
        where = {a["at"]: a for a in described["artifacts"]}
        assert set(where) == {"output.file", "output.files.report", "output.files.extra"}
        report = where["output.file"]
        assert where["output.files.report"]["key"] == report["key"]  # one file, twice
        assert all(a["path"] is None for a in described["artifacts"])
        assert report["content_type"] == "text/plain"
        assert described["output"]["file"] == {
            k: report[k] for k in ("key", "size", "sha256", "content_type")
        }
        assert "pass download_dir" in described["note"]

        assert described["artifact_count"] == 3

        target = tmp_path / "downloads"
        fetched = await ok(client, "result", job_id=hired["job_id"], download_dir=str(target))
        paths = {a["at"]: Path(a["path"]) for a in fetched["artifacts"]}
        assert paths["output.file"] == paths["output.files.report"]
        assert paths["output.file"].parent == target
        assert paths["output.file"].name == report["key"] + ".txt"
        # Decrypted plaintext: the directory this server made, and every file, are ours alone.
        assert stat.S_IMODE(target.stat().st_mode) == 0o700
        assert {stat.S_IMODE(p.stat().st_mode) for p in paths.values()} == {0o600}
        content = paths["output.file"].read_bytes()
        assert content == b"report line 0\nreport line 1\nreport line 2\n"
        assert hashlib.sha256(content).hexdigest() == report["sha256"]
        assert paths["output.files.extra"].read_text() == "extra line 0\nextra line 1\n"
        assert sorted(p.name for p in target.iterdir()) == sorted({p.name for p in paths.values()})

        error = await refused(client, "result", job_id=hired["job_id"], download_dir="relative/dir")
        assert error["code"] == "invalid_download_dir"
        # A seller's text cannot steer its files into a directory a program loads them from.
        for where in ("/etc/cron.d", "~/.bashrc.d", str(tmp_path / ".config" / "autostart")):
            error = await refused(client, "result", job_id=hired["job_id"], download_dir=where)
            assert error["code"] == "invalid_download_dir", where
        assert not (tmp_path / ".config").exists()


async def test_a_large_output_goes_to_a_file(world: World, tmp_path: Path) -> None:
    buyer = await world.buyer()
    async with connect(world.kontra(buyer), inline_limit=64) as client:
        hired = await ok(client, "hire", offering=NAME, input=EXAMPLE, max_price=1)
        withheld = await ok(client, "result", job_id=hired["job_id"])
        assert withheld["output"] is None
        assert withheld["output_bytes"] > 64
        assert "download_dir" in withheld["note"]
        saved = await ok(client, "result", job_id=hired["job_id"], download_dir=str(tmp_path))
        assert saved["output"] is None
        here = await asyncio.to_thread(tmp_path.resolve)
        assert saved["output_file"] == str(here / f"{hired['job_id']}.json")
        mode = (await asyncio.to_thread(os.stat, saved["output_file"])).st_mode
        assert stat.S_IMODE(mode) == 0o600
        output = json.loads(await asyncio.to_thread(Path(saved["output_file"]).read_text))
        assert output["upper"] == EXAMPLE["text"].upper()
        assert len(json.dumps(output, separators=(",", ":"))) == saved["output_bytes"]


# --------------------------------------------------------------------------- cancel safety
#
# No Temporal and no gate: stand-ins for the stream and the job make a cancellation land in
# the loop iteration where the awaited thing arrives. On Python 3.11 ``asyncio.wait_for``
# returns the result then and drops the cancellation, so an aborted tool call would go on
# (reading progress, asking the gate); ``asyncio.timeout`` keeps it. Red on 3.11 with
# ``wait_for``. CI runs them on 3.11 and 3.12; locally, without touching ``.venv``:
# ``uv run --isolated --python 3.11 pytest tests/mcp/test_mcp_server.py -k cancel``.

_JOB = "0" * 32


class _Stream:
    """A progress stream whose first event arrives when the test resolves ``arrival``, and
    that ends after it."""

    def __init__(self) -> None:
        self.arrival: asyncio.Future[m.ProgressEvent] = asyncio.get_running_loop().create_future()
        self.waiting = asyncio.Event()
        self.reads = 0
        self.cancelled = False
        self.closed = False

    def __aiter__(self) -> _Stream:
        return self

    async def __anext__(self) -> m.ProgressEvent:
        self.reads += 1
        if self.reads > 1:
            raise StopAsyncIteration
        self.waiting.set()
        try:
            return await self.arrival
        except asyncio.CancelledError:
            self.cancelled = True
            raise

    async def aclose(self) -> None:
        self.closed = True


class _StreamKontra:
    """The two calls ``status`` makes on its client to read progress."""

    def __init__(self, stream: _Stream) -> None:
        def progress(job_id: str, *, after: int) -> _Stream:
            return stream

        self.gate = SimpleNamespace(progress=progress)
        self.read: list[m.ProgressEvent] = []

    def _progress(self, event: m.ProgressEvent) -> SimpleNamespace:
        self.read.append(event)
        return SimpleNamespace(source=event.source)


class _DeliveredJob:
    """A job whose gate records the delivery when the test resolves ``arrival``."""

    job_id = _JOB

    def __init__(self) -> None:
        self.arrival: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self.waiting = asyncio.Event()
        self.status_calls = 0

    async def _gate_saw_delivery(self) -> None:
        self.waiting.set()
        await self.arrival

    async def status(self) -> str:
        self.status_calls += 1
        return "view"


def _event() -> m.ProgressEvent:
    return m.ProgressEvent(seq=1, at=datetime.now(UTC), source=m.ProgressSource.HISTORY)


async def test_cancel_of_status_that_lands_as_progress_arrives_is_kept() -> None:
    stream = _Stream()
    k = _StreamKontra(stream)
    task = asyncio.create_task(KontraTools(k)._progress(_JOB))  # type: ignore[arg-type]
    await stream.waiting.wait()
    stream.arrival.set_result(_event())  # in the same loop iteration as the cancel
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert k.read == []  # the event that arrived with the cancel was not taken
    assert stream.closed  # and the stream is closed on the way out


async def test_cancel_of_result_that_lands_as_the_gate_catches_up_is_kept() -> None:
    job = _DeliveredJob()
    tools = KontraTools(_StreamKontra(_Stream()))  # type: ignore[arg-type]
    task = asyncio.create_task(tools._delivered_view(job))  # type: ignore[arg-type]
    await job.waiting.wait()
    job.arrival.set_result(None)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert job.status_calls == 0  # it did not go on to ask the gate


async def test_cancel_safe_waits_still_time_out(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(mcp_server, "_PROGRESS_FIRST", 0.05)
    monkeypatch.setattr(mcp_server, "_GATE_CATCH_UP", 0.05)
    stream = _Stream()  # never yields
    tools = KontraTools(_StreamKontra(stream))  # type: ignore[arg-type]
    seen = await tools._progress(_JOB)
    assert (seen.seq, seen.latest) == (0, None)
    assert stream.cancelled  # the read was stopped at the deadline
    assert stream.closed
    job = _DeliveredJob()  # the gate never catches up: the view is read anyway
    assert await tools._delivered_view(job) == "view"  # type: ignore[arg-type]
    assert job.status_calls == 1
    assert asyncio.current_task().cancelling() == 0  # type: ignore[union-attr]


async def test_progress_still_reads_every_event_it_is_given() -> None:
    stream = _Stream()
    k = _StreamKontra(stream)
    task = asyncio.create_task(KontraTools(k)._progress(_JOB))  # type: ignore[arg-type]
    await stream.waiting.wait()
    event = _event()
    stream.arrival.set_result(event)
    seen = await task
    assert (seen.seq, k.read) == (1, [event])
    assert stream.closed


# --------------------------------------------------------------------------- kontra mcp


async def test_kontra_mcp_over_stdio(world: World, tmp_path: Path) -> None:
    """Acceptance 12 as an agent's host runs it: the ``kontra mcp`` command (the console
    script's code, ``python -m kontra.cli mcp``) on the buyer's profile, over stdio, with its
    own connections to the gate and Temporal."""
    buyer = await world.buyer()
    params = StdioServerParameters(
        command=sys.executable, args=["-m", "kontra.cli", "mcp"], env=buyer.env(), cwd=tmp_path
    )
    log = tmp_path / "stderr.log"
    with log.open("w") as errlog:
        try:
            async with Client(stdio_client(params, errlog=errlog)) as client:
                assert {t.name for t in (await client.list_tools()).tools} == TOOLS
                found = await ok(client, "search", query="words")
                assert world.key in {o["offering"] for o in found["offerings"]}
                quoted = await ok(client, "quote", offering=world.key, input={"text": TEXT})
                hired = await ok(client, "hire", quote_id=quoted["quote_id"], max_price="1.00")
                got = await ok(client, "result", job_id=hired["job_id"], wait_seconds=60)
                assert got["output"]["upper"] == TEXT.upper()
                accepted = await ok(client, "accept", job_id=hired["job_id"])
                assert accepted["escrow"] == m.Escrow.RELEASED.value
        except BaseException:
            print(log.read_text(), file=sys.stderr)
            raise
    after = await buyer.balance()
    assert (after.available_minor, after.held_minor) == (START - 100, 0)
