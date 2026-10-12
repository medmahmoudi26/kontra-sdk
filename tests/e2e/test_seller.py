"""The seller's path against the real stack: from ``kontra init`` to a hireable offering
(acceptance 1), the hash lock (17), what a seller's handler refuses before any workflow
starts (7, 18), and what the catalog refuses to publish (19).

The handler's checks are proved cell by cell in ``tests/serve``; here each runs once through
the real gate (its hold tokens, its catalog), Kontra's Temporal server and a seller's worker
in its own process, and the outcome is looked at where it lands: the seller's namespace."""

from __future__ import annotations

import base64
import json
import os
import re
import sys
import time
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

import pytest
from temporalio.service import RPCError, RPCStatusCode

from kontra.auth import Session
from kontra.contract import seal
from kontra.contract.names import job_workflow_id
from kontra.gate_client import GateError
from kontra.gate_client import models as m
from kontra.hire import JobFailed, Kontra
from kontra.profile import Profile

from .support import Party, Stack, Tap, handle, run, run_kontra, scaffold, serving

pytestmark = [pytest.mark.e2e]

TEXT = "the quick brown fox jumps over the lazy dog\nthe end\n"
OCCUPIED = "kontra.occupied"  # kontra._control.OCCUPY_WORKFLOW_TYPE: the gate's placeholder


def drift(project: Path) -> None:
    """Change a type's schema after publishing (a field's description), as a seller editing
    models.py would: the contract, and so its hash, changes."""
    models = project / "models.py"
    text = models.read_text()
    edited = text.replace('description="The text to count."', 'description="Any text at all."')
    assert edited != text
    models.write_text(edited)


async def seller_runs(seller: Party, workflow_id: str) -> tuple[str | None, list[str]]:
    """In the seller's own namespace, through its worker token: the type of the latest run
    of ``workflow_id`` (None when there is no run), and the types of every run visibility
    lists."""
    async with seller.temporal() as client:
        try:
            latest: str | None = (
                await client.get_workflow_handle(workflow_id).describe()
            ).workflow_type
        except RPCError as err:
            if err.status is not RPCStatusCode.NOT_FOUND:
                raise
            latest = None
        listed = [
            wf.workflow_type async for wf in client.list_workflows(f"WorkflowId = '{workflow_id}'")
        ]
    return latest, listed


def tampered(token: str, **claims: Any) -> str:
    """The token with ``claims`` changed in its payload and its signature left as it was."""
    head, body, signature = token.split(".")
    payload = json.loads(base64.urlsafe_b64decode(body + "=" * (-len(body) % 4)))
    payload.update(claims)
    raw = json.dumps(payload, separators=(",", ":")).encode()
    return f"{head}.{base64.urlsafe_b64encode(raw).rstrip(b'=').decode()}.{signature}"


def hire_with(k: Kontra, change: Callable[[m.HireView], m.HireView]) -> None:
    """Have ``k`` start its next job with the hold the gate issued changed by ``change``."""
    original: Callable[..., Awaitable[m.HireView]] = k.gate.hire

    async def hire(*args: Any, **kwargs: Any) -> m.HireView:
        return change(await original(*args, **kwargs))

    k.gate.hire = hire  # type: ignore[method-assign]


# --------------------------------------------------------------------------- acceptance 1


@pytest.mark.timeout(1800)
async def test_ac01_init_dev_serve_yields_a_hireable_offering(stack: Stack, tmp_path: Path) -> None:
    """``kontra init && kontra dev && kontra serve`` from a new home on this machine, timed;
    the scaffold's own test passes; a buyer then hires the offering through the CLI and
    accepts the delivery. The home is new but for the stack's ``dev.env`` (its credentials),
    so ``kontra dev`` finds the stack that is running instead of starting a second one."""
    timings: dict[str, float] = {}
    started = time.monotonic()
    home = tmp_path / "home"
    home.mkdir(mode=0o700)
    (home / "dev.env").write_bytes((stack.home / "dev.env").read_bytes())
    os.chmod(home / "dev.env", 0o600)
    seller_handle = handle("ac01")

    t = time.monotonic()
    up = await run_kontra("dev", "--handle", seller_handle, env=stack.dev_env(home), within=900)
    timings["kontra dev"] = time.monotonic() - t
    assert up.returncode == 0, up.stdout + up.stderr
    assert f"logged in as {seller_handle}" in up.stdout

    seller = Party(seller_handle, home, Profile.load(home), stack)
    work = tmp_path / "work"
    work.mkdir()
    t = time.monotonic()
    made = await run_kontra(
        "init", "wc", env=seller.env(), cwd=work
    )  # the party comes from the profile
    timings["kontra init"] = time.monotonic() - t
    assert made.returncode == 0, made.stderr
    project = work / "wc"
    key = f"{seller_handle}/wc@0.1.0"

    # No Kontra import in the job's code, and no Python symbol in offering.yaml (D1, D11).
    for name in ("workflows.py", "activities.py"):
        source = (project / name).read_text()
        assert re.search(r"^\s*(from|import)\s+kontra\b", source, re.M) is None, name
    yaml_text = (project / "offering.yaml").read_text()
    for symbol in ("WcFlow", "count_words", "quote:", "workflows.", "activities."):
        assert symbol not in yaml_text, symbol
    from kontra.contract import load_offering

    assert load_offering(project / "offering.yaml").offering_id.key == key  # it refuses symbols

    # The scaffold's own test: its whole lifecycle with no containers.
    env = {k: v for k, v in seller.env().items() if not k.startswith("PYTEST")}
    t = time.monotonic()
    tested = await run(
        [sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider"],
        cwd=project,
        env=env,
        within=900,
    )
    timings["scaffold pytest"] = time.monotonic() - t
    assert tested.returncode == 0, tested.stdout[-4000:] + tested.stderr[-4000:]
    assert "2 passed" in tested.stdout

    buyer = await stack.party("ac01-buyer")
    t = time.monotonic()
    async with serving(project, seller, [key], log=tmp_path / "serve.log"):
        timings["kontra serve until serving"] = time.monotonic() - t
        t = time.monotonic()
        hired = await run_kontra(
            "hire",
            f"{seller_handle}/wc",
            "-f",
            str(project / "input.json"),
            "--max-price",
            "1",
            "--accept",
            "--json",
            env=buyer.env(),
            within=300,
        )
        timings["kontra hire --accept"] = time.monotonic() - t
    assert hired.returncode == 0, hired.stdout + hired.stderr
    assert '"words": 11' in hired.stdout.replace("\n", " ") or '"words":11' in hired.stdout
    total = time.monotonic() - started

    k = buyer.kontra()
    try:
        (job,) = await k.jobs(limit=1)
        assert (job.offering, job.state, job.escrow) == (
            key,
            m.JobState.COMPLETED,
            m.Escrow.RELEASED,
        )
    finally:
        await k.aclose()
    # On a fresh machine `kontra dev` also starts the stack, which here the session did
    # first (`make e2e`'s `kontra dev up`, from nothing after a `down --volumes`): that
    # time counts too. Pulling the images does not (they are local).
    with_stack = total + stack.up_seconds
    print(
        "\n[acceptance 1] "
        + ", ".join(f"{step} {secs:.1f}s" for step, secs in timings.items())
        + f"; total {total:.1f}s; with the session's `kontra dev up` "
        f"({stack.up_seconds:.1f}s) {with_stack:.1f}s"
    )
    assert with_stack < 3600


# --------------------------------------------------------------------------- acceptance 17


@pytest.mark.timeout(600)
async def test_ac17_a_type_edited_after_publish_makes_serve_refuse(
    stack: Stack, tmp_path: Path
) -> None:
    """Publish (``kontra serve`` on a dev profile publishes), edit a type without
    republishing, and ``kontra serve`` refuses to start that version, naming both hashes."""
    seller = await stack.party("ac17")
    project, key = await scaffold(seller, tmp_path)
    async with serving(project, seller, [key], log=tmp_path / "first.log"):
        pass
    k = seller.kontra()
    try:
        published = (await k.offering(key)).contract_hash
    finally:
        await k.aclose()

    drift(project)
    derived = await run_kontra("contract", "--json", "--dir", str(project), env=seller.env())
    assert derived.returncode == 0, derived.stderr
    edited = json.loads(derived.stdout)["x-kontra"]["contract_hash"]
    assert edited != published

    refused = await run_kontra("serve", "--dir", str(project), env=seller.env(), within=120)
    out = refused.stdout + refused.stderr
    assert refused.returncode != 0, out
    assert f"serving {key}" not in out
    assert (
        f"{key}: this code's contract hash is {edited} but the catalog's is {published}" in out
    ), out
    assert "republish or bump the version" in out


# --------------------------------------------------------------------------- acceptance 18


@pytest.mark.timeout(600)
async def test_ac18_a_hold_for_one_contract_is_refused_by_a_handler_serving_another(
    stack: Stack, tmp_path: Path
) -> None:
    """The gate mints the hold for the catalog's contract X. The seller's worker serves the
    same offering with edited types, hash Y -- started with the hash lock taken out, as a
    seller's modified or stale worker could be. The handler refuses the hold before any
    workflow starts: in the seller's namespace ``job-<id>`` never runs the seller's workflow
    (the only run there is the gate's placeholder, which keeps the token from starting one
    later), and the buyer is refunded at once."""
    seller = await stack.party("ac18")
    buyer = await stack.party("ac18-buyer")
    project, key = await scaffold(seller, tmp_path)
    async with serving(project, seller, [key], log=tmp_path / "publish.log"):
        pass  # published: hash X
    drift(project)
    (project / "drifted.py").write_text(
        "from kontra.serve import worker as w\n"
        "\n"
        "async def no_hash_lock(self, gate, profile, public_key):\n"
        "    return {b.key: b.contract_hash(public_key) for b in self.offerings}\n"
        "\n"
        "w.Worker._hash_lock = no_hash_lock\n"
        "\n"
        "from worker import worker  # noqa: E402\n"
        "\n"
        "worker.run()\n"
    )
    k = buyer.kontra()
    try:
        x = (await k.offering(key)).contract_hash
        async with serving(
            project,
            seller,
            [key],
            log=tmp_path / "drifted.log",
            argv=[sys.executable, str(project / "drifted.py")],
        ) as drifted:
            assert x not in drifted.text()  # it serves Y
            q = await k.quote(key, text=TEXT)
            assert q.ok, q.reason
            assert q.contract_hash == x
            with pytest.raises(JobFailed) as failed:
                await q.hire(max_price="1.00")
        job_id = failed.value.job_id
        assert job_id is not None
        message = str(failed.value)
        assert "the seller refused to start the job" in message
        assert f"hold token refused: it is for contract {x}, but this worker serves" in message

        latest, listed = await seller_runs(seller, job_workflow_id(job_id))
        print(
            f"\n[acceptance 18] {message}\n  {job_workflow_id(job_id)}: latest run {latest}, runs {listed}"
        )
        assert latest in (None, OCCUPIED), latest
        assert set(listed) <= {OCCUPIED}, listed
        view = await k.gate.job(job_id)
        assert view.escrow is m.Escrow.REFUNDED, view
        assert view.started_at is None
        balance = await k.balance()
        assert (balance.available, balance.held_minor) == ("100.00", 0)
    finally:
        await k.aclose()


# --------------------------------------------------------------------------- acceptance 7


@pytest.mark.timeout(600)
async def test_ac07_tampered_hold_tokens_are_refused_without_the_gate(
    stack: Stack, tmp_path: Path
) -> None:
    """A smoke of acceptance 7 through the real gate: holds the gate issued, with the amount,
    the seller or the expiry changed, are refused by the seller's handler, and the seller's
    worker sends nothing to the gate while it refuses them (its every connection to the gate
    goes through a recording relay). No seller workflow runs; each buyer is refunded."""
    seller = await stack.party("ac07")
    project, key = await scaffold(seller, tmp_path)
    gate = re.fullmatch(r"https?://([^:/]+):([0-9]+)", stack.gate_url)
    assert gate is not None, stack.gate_url
    other = await stack.party("ac07-other")
    with Tap(gate.group(1), int(gate.group(2))) as tap:
        seller.point_gate_at(tap.url)
        async with serving(project, seller, [key], log=tmp_path / "serve.log"):
            assert tap.sent(), "the seller's worker did not reach the gate through the relay"
            for claim, value in (
                ("amount_minor", 0),
                ("seller_ns", other.handle),
                ("exp", int(time.time()) + 30 * 86_400),
            ):
                buyer = await stack.party("ac07-buyer")
                k = buyer.kontra()
                try:
                    hire_with(
                        k,
                        lambda view, c=claim, v=value: view.model_copy(
                            update={"hold_token": tampered(view.hold_token, **{c: v})}
                        ),
                    )
                    q = await k.quote(key, text=TEXT)
                    assert q.ok, q.reason
                    began = time.monotonic()
                    with pytest.raises(JobFailed) as failed:
                        await q.hire(max_price="1.00")
                    ended = time.monotonic()
                    message = str(failed.value)
                    assert "hold token refused: the signature does not verify" in message, (
                        claim,
                        message,
                    )
                    assert tap.sent(began, ended) == [], (claim, tap.sent(began, ended))
                    job_id = failed.value.job_id
                    assert job_id is not None
                    latest, listed = await seller_runs(seller, job_workflow_id(job_id))
                    print(
                        f"\n[acceptance 7] {claim} changed: {message}; the seller sent the gate "
                        f"{len(tap.sent(began, ended))} chunks meanwhile ({len(tap.sent())} in all);"
                        f" job-{job_id}: latest run {latest}, runs {listed}"
                    )
                    assert latest in (None, OCCUPIED), (claim, latest)
                    assert set(listed) <= {OCCUPIED}, (claim, listed)
                    balance = await k.balance()
                    assert (balance.available, balance.held_minor) == ("100.00", 0), claim
                finally:
                    await k.aclose()


# --------------------------------------------------------------------------- acceptance 19


@pytest.mark.timeout(300)
async def test_ac19_the_catalog_refuses_external_refs_and_open_objects(
    stack: Stack, tmp_path: Path
) -> None:
    """A smoke of acceptance 19 through the real gate: a sealed document (its hash is right)
    with an external ``$ref``, or with ``additionalProperties`` left unset on an object, is
    refused by the catalog with the offending path named; the untouched document publishes."""
    seller = await stack.party("ac19")
    project, key = await scaffold(seller, tmp_path)
    derived = await run_kontra("contract", "--json", "--dir", str(project), env=seller.env())
    assert derived.returncode == 0, derived.stderr
    document: dict[str, Any] = json.loads(derived.stdout)
    defs = document["$defs"]
    (inp,) = [name for name in defs if name.endswith("Input")]

    def variant(edit: Callable[[dict[str, Any]], None]) -> dict[str, Any]:
        doc = json.loads(json.dumps(document))
        edit(doc)
        doc["x-kontra"].pop("contract_hash", None)
        return seal(doc)

    def external_ref(doc: dict[str, Any]) -> None:
        doc["$defs"][inp]["properties"]["text"] = {"$ref": "https://schemas.example/text.json"}

    def open_object(doc: dict[str, Any]) -> None:
        del doc["$defs"][inp]["additionalProperties"]

    async with Session(Profile.load(seller.home), home=seller.home) as session:
        for edit, path in (
            (external_ref, f"$defs.{inp}.properties.text"),
            (open_object, f"$defs.{inp}"),
        ):
            with pytest.raises(GateError) as refused:
                await session.gate.publish(variant(edit))
            err = refused.value
            assert (err.status, err.code) == (422, "invalid_contract"), err
            assert (err.detail or {}).get("path", "").startswith(path), (edit.__name__, err)
            assert path in err.message, err.message
        with pytest.raises(GateError) as missing:
            await session.gate.get_offering(key)
        assert missing.value.status == 404  # nothing refused was published
        entry = await session.gate.publish(document)
        assert (entry.offering, entry.contract_hash) == (
            key,
            document["x-kontra"]["contract_hash"],
        )
