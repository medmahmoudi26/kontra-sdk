"""The CLI against a whole marketplace (``kontra.testing.Marketplace``: the fake gate and a
Temporal dev server, no containers), as a seller and buyers would use it: ``login --dev``,
``init``, ``publish``, ``serve`` in the background (a real process), then ``quote``,
``hire``, ``jobs``, ``job``, ``balance``, ``topup``, ``contract`` and ``pull``."""

from __future__ import annotations

import asyncio
import dataclasses
import json
import os
import re
import shutil
import stat
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest
import pytest_asyncio

from kontra.contract import contract_hash, from_yaml, nexgen_view, seal
from kontra.gate_client import GateClient
from kontra.profile import Profile
from kontra.testing import Marketplace
from kontra.testing.devserver import DevServer

from .support import Cli, explain, handle, serve_process

REPO = Path(__file__).resolve().parents[2]

pytestmark = [pytest.mark.temporal, pytest.mark.asyncio(loop_scope="module")]

INPUT = {"text": "the quick brown fox jumps over the lazy dog\nthe end\n"}


@dataclass
class Shop:
    mp: Marketplace
    root: Path
    seller: Cli
    party: str
    project: Path
    key: str
    published: dict[str, Any]
    input_file: Path

    def cli(self, name: str) -> Cli:
        return Cli(self.root / name, cwd=self.root)

    async def buyer(self, topup: str | None = "25.00") -> Cli:
        """A new party, logged in with ``kontra login --dev`` and topped up."""
        who = handle("buyer")
        cli = self.cli(who)
        await cli.ok("login", "--dev", "--handle", who, "--gate", self.mp.base_url)
        if topup is not None:
            await cli.ok("topup", topup)
        return cli

    def write(self, name: str, value: Any) -> Path:
        path = self.root / name
        path.write_text(json.dumps(value))
        return path


@pytest_asyncio.fixture(loop_scope="module", scope="module")
async def shop(
    dev_server: DevServer, tmp_path_factory: pytest.TempPathFactory
) -> AsyncIterator[Shop]:
    root = tmp_path_factory.mktemp("cli-market")
    async with Marketplace(dev_server=dev_server) as mp:
        party = handle("seller")
        seller = Cli(root / "seller-home", cwd=root)
        await seller.ok("login", "--dev", "--handle", party, "--gate", mp.base_url)
        await seller.ok("init", "shop")  # the party comes from the profile
        project = root / "shop"
        published = await seller.json("publish", "--dir", str(project))
        [entry] = published
        input_file = root / "input.json"
        input_file.write_text(json.dumps(INPUT))
        with serve_process(project, root / "seller-home", root / "serve.log") as proc:
            yield Shop(mp, root, seller, party, project, entry["offering"], entry, input_file)
        log = (root / "serve.log").read_text()
        assert proc.returncode == 0, f"exit {proc.returncode}:\n{log}"  # a graceful stop


# --------------------------------------------------------------------------- login


async def test_login_dev_writes_a_dev_profile(shop: Shop) -> None:
    profile = Profile.load(shop.seller.home)
    assert (profile.party, profile.namespace, profile.dev, profile.tls) == (
        shop.party,
        shop.party,
        True,
        False,
    )
    assert profile.gate_url == shop.mp.base_url
    key = shop.seller.home / "key"
    assert stat.S_IMODE(key.stat().st_mode) == 0o600
    assert profile.public_key is not None
    assert profile.public_key in key.read_text()

    who = handle("json")
    out = await shop.cli(who).json("login", "--dev", "--handle", who, "--gate", shop.mp.base_url)
    assert out["party"] == who
    assert "refresh_token" not in out


async def test_login_device_flow(shop: Shop) -> None:
    """Without --dev: RFC 8628 (the dev gate approves the code itself)."""
    who = handle("device")
    cli = shop.cli(who)
    result = await cli.ok("login", "--gate", shop.mp.base_url, "--handle", who, "--no-browser")
    assert "To log in, open" in result.stderr
    assert f"as {who}" in result.stdout
    assert Profile.load(cli.home).party == who


# --------------------------------------------------------------------------- publish & contract


async def test_publish_prints_the_contract_hash_and_endpoint(shop: Shop) -> None:
    entry = shop.published
    assert entry["offering"] == f"{shop.party}/shop@0.1.0"
    assert re.fullmatch(r"k[0-9a-f]{32}", entry["endpoint"])
    async with GateClient(shop.mp.base_url) as gate:
        catalog = await gate.get_offering(shop.key)
    assert (catalog.contract_hash, catalog.endpoint) == (entry["contract_hash"], entry["endpoint"])

    again = await shop.seller.ok("publish", "--dir", str(shop.project))
    assert f"contract  {entry['contract_hash']}" in again.stdout
    assert f"endpoint  {entry['endpoint']}" in again.stdout  # the same version, the same endpoint

    contract = await shop.seller.ok("contract", "--dir", str(shop.project))
    assert from_yaml(contract.stdout)["x-kontra"]["contract_hash"] == entry["contract_hash"]


async def test_status_lists_the_served_worker_from_temporal(shop: Shop) -> None:
    """kontra-cloud#6 end to end: the gate asks the dev server's DescribeTaskQueue about
    the version worker.py names, and `kontra serve`'s worker is listed as its poller."""
    view: dict[str, Any] = {}
    for _ in range(100):
        [view] = await shop.seller.json("status", "--workers", "--dir", str(shop.project))
        types = view["types_info"]
        if types["workflow"]["pollers"] and types["nexus"]["pollers"]:
            break
        await asyncio.sleep(0.2)
    assert view["offering"] == view["task_queue"] == shop.key
    identities = {p["identity"] for t in view["types_info"].values() for p in t["pollers"]}
    assert len(identities) == 1, identities  # the one `kontra serve` process
    assert view["stale"] is False
    assert "no pollers" not in view["verdict"]


async def test_publish_refuses_an_offering_of_another_party(shop: Shop) -> None:
    buyer = await shop.buyer(topup=None)
    result = await buyer.run("publish", "--dir", str(shop.project))
    assert result.exit_code == 1, explain(result)
    assert f"names the party {shop.party!r}" in result.stderr


async def test_a_stale_dev_login_confirms_at_once_and_publishes(
    dev_server: DevServer, tmp_path: Path
) -> None:
    """Past the gate's hour a change needs a confirmation (decisions I23); a `kontra dev`
    login gets one at once, without a prompt or a mail (``tests/cli/test_confirm.py`` has
    the email login, which is asked in place)."""
    async with Marketplace(dev_server=dev_server, confirm_ttl=timedelta(seconds=2)) as mp:
        party = handle("stale")
        cli = Cli(tmp_path / "home", cwd=tmp_path)
        await cli.ok("login", "--dev", "--handle", party, "--gate", mp.base_url)
        await cli.ok("init", "late")
        mp.gate.advance(5)  # the login is now older than the gate's hour (two seconds here)
        result = await cli.ok("publish", "--dir", str(tmp_path / "late"))
        assert f"published {party}/late@0.1.0" in result.stdout
        assert "Confirm it's you" not in result.stderr
        assert mp.gate.outbox == []


async def test_serve_refuses_code_that_drifted_from_the_catalog(shop: Shop) -> None:
    """Acceptance 17 through the CLI: a type edited after publishing, not republished."""
    drift = shop.root / "drift"
    shutil.copytree(shop.project, drift)
    models = drift / "models.py"
    models.write_text(models.read_text().replace("max_length=100_000", "max_length=50_000"))
    result = await shop.seller.run("serve", "--dir", str(drift))
    assert result.exit_code == 1, explain(result)
    assert result.stderr.startswith("error: refusing to start:")
    assert "republish or bump the version" in result.stderr


# --------------------------------------------------------------------------- buying


async def test_quote(shop: Shop) -> None:
    buyer = await shop.buyer()
    result = await buyer.ok("quote", shop.key, "-f", str(shop.input_file))
    assert result.stdout.startswith(f"{shop.key}: 0.01 USD")
    q = await buyer.json("quote", f"{shop.party}/shop", "-f", "-", stdin=json.dumps(INPUT))
    assert (q["status"], q["price"], q["currency"], q["offering"]) == (
        "ok",
        "0.01",
        "USD",
        shop.key,
    )
    assert q["contract_hash"] == shop.published["contract_hash"]

    blank = shop.write("blank.json", {"text": "   "})
    rejected = await buyer.ok("quote", shop.key, "-f", str(blank))
    assert "rejected this input: the text is blank" in rejected.stdout


async def test_an_input_that_breaks_the_contract_is_never_sent(shop: Shop) -> None:
    buyer = await shop.buyer()
    for value, where in (({"text": "hi", "extra": 1}, "extra"), ({"text": ""}, "text")):
        result = await buyer.run("quote", shop.key, "-f", str(shop.write("bad.json", value)))
        assert result.exit_code == 1, explain(result)
        assert result.stderr.startswith("error: the input does not match the contract")
        assert where in result.stderr


async def test_hire_wait_accept_then_jobs_and_job(shop: Shop) -> None:
    buyer = await shop.buyer()
    result = await buyer.ok(
        "hire", shop.key, "-f", str(shop.input_file), "--max-price", "1", "--wait", "--accept"
    )
    match = re.search(r"job ([0-9a-f]{32}) at 0.01 USD", result.stdout)
    assert match, result.stdout
    job_id = match.group(1)
    body = result.stdout[result.stdout.index("{") : result.stdout.rindex("}") + 1]
    delivered = json.loads(body)
    assert (delivered["words"], delivered["lines"], delivered["top"]["the"]) == (11, 2, 3)
    assert "accepted: escrow released" in result.stdout

    bal = await buyer.json("balance")
    assert (bal["available"], bal["held"]) == ("24.99", "0.00")
    mine = await buyer.json("jobs")
    assert [(j["job_id"], j["state"], j["escrow"]) for j in mine] == [
        (job_id, "completed", "released")
    ]
    sold = await shop.seller.json("jobs", "--as", "seller", "--state", "completed")
    assert job_id in {j["job_id"] for j in sold}
    shown = await buyer.ok("job", job_id)
    assert "state     completed, escrow released" in shown.stdout
    listed = await buyer.ok("jobs")
    assert listed.stdout.startswith(job_id)


async def test_hire_refuses_above_max_price(shop: Shop) -> None:
    buyer = await shop.buyer()
    result = await buyer.run("hire", shop.key, "-f", str(shop.input_file), "--max-price", "0.009")
    assert result.exit_code == 1, explain(result)
    assert "above your maximum" in result.stderr
    assert (await buyer.json("balance"))["held"] == "0.00"
    assert await buyer.json("jobs") == []


async def test_job_result_progress_and_reject(shop: Shop) -> None:
    buyer = await shop.buyer()
    hired = await buyer.json("hire", shop.key, "-f", str(shop.input_file))
    job_id = hired["job_id"]
    out = shop.root / f"result-{job_id}"
    result = await buyer.ok("job", job_id, "--result", str(out))
    assert f"wrote {out / 'result.json'}" in result.stdout
    assert json.loads((out / "result.json").read_text())["words"] == 11

    events = await buyer.json("job", job_id, "--progress")  # ends: the job is closed
    assert isinstance(events, list)
    assert [e["seq"] for e in events] == sorted(e["seq"] for e in events)

    disputed = await buyer.json("job", job_id, "--reject", "the counts look wrong")
    assert (disputed["state"], disputed["escrow"]) == ("completed", "disputed")
    view = await buyer.json("job", job_id)
    assert view["escrow"] == "disputed"


async def test_job_result_still_reads_once_the_version_is_retired(shop: Shop) -> None:
    """A job outlives its version (decisions I22): retired after the delivery, the version
    still answers its exact lookup, so `kontra job --result` reads the job; nobody can quote
    it again."""
    buyer = await shop.buyer()
    job_id = (await buyer.json("hire", shop.key, "-f", str(shop.input_file)))["job_id"]
    await buyer.ok("job", job_id, "--result", str(shop.root / f"result-{job_id}"))
    row = shop.mp.gate.offerings[shop.key]
    row.retired = True
    try:
        out = shop.root / f"retired-{job_id}"
        await buyer.ok("job", job_id, "--result", str(out))
        assert json.loads((out / "result.json").read_text())["words"] == 11
        refused = await buyer.run("quote", shop.key, "-f", str(shop.input_file))
        assert refused.exit_code == 1, explain(refused)
        assert f"{shop.key} is retired" in refused.stderr
    finally:
        row.retired = False


async def test_job_cancel_refunds(shop: Shop) -> None:
    """A slow version of the shop (a timer before the work): cancel before delivery."""
    slow = shop.root / "slow"
    shutil.copytree(shop.project, slow)
    flows = slow / "workflows.py"
    flows.write_text(
        flows.read_text().replace(
            "        return await workflow.execute_activity(",
            "        await workflow.sleep(timedelta(minutes=10))\n"
            "        return await workflow.execute_activity(",
        )
    )
    offering = slow / "offering.yaml"
    offering.write_text(offering.read_text().replace("version: 0.1.0", "version: 0.2.0"))
    await shop.seller.ok("publish", "--dir", str(slow))
    buyer = await shop.buyer()
    with serve_process(slow, shop.seller.home, shop.root / "slow.log"):
        hired = await buyer.json("hire", f"{shop.party}/shop@0.2.0", "-f", str(shop.input_file))
        assert (await buyer.json("balance"))["held"] == "0.01"
        canceled = await buyer.json("job", hired["job_id"], "--cancel")
    assert (canceled["state"], canceled["escrow"]) == ("canceled", "refunded")
    bal = await buyer.json("balance")
    assert (bal["available"], bal["held"]) == ("25.00", "0.00")


async def test_balance_and_topup(shop: Shop) -> None:
    buyer = await shop.buyer(topup=None)
    assert (await buyer.json("balance"))["available"] == "0.00"
    credited = await buyer.json("topup", "12.5")
    assert credited["credited"] is True
    assert credited["balance"]["available"] == "12.50"
    shown = await buyer.ok("balance")
    assert shown.stdout.strip().endswith("12.50 USD available, 0.00 held")


# --------------------------------------------------------------------------- pull


def _stand_in_nexgen(path: Path) -> Path:
    """A nexgen that records its arguments and writes one file: the plumbing of `pull`
    without docker (the real one is tests/cli/test_pull_ts.py's)."""
    path.write_text(
        "#!/bin/sh\n"
        'if [ "$1" = "--version" ]; then echo "nexgen 0.2.5"; exit 0; fi\n'
        'out=""; prev=""\n'
        'for a in "$@"; do [ "$prev" = "--output" ] && out="$a"; prev="$a"; done\n'
        'printf "%s\\n" "$@" > "$out/args.txt"\n'
        'echo "// generated" > "$out/index.ts"\n'
    )
    path.chmod(0o755)
    return path


async def test_pull_verifies_and_hands_nexgen_the_view(shop: Shop) -> None:
    nexgen = _stand_in_nexgen(shop.root / "fake-nexgen")
    anyone = Cli(shop.root / "nobody", env={"KONTRA_NEXGEN": str(nexgen)})  # no login needed
    out = shop.root / "client-ts"
    summary = await anyone.json(
        "pull", f"{shop.party}/shop@0.1", "--lang", "ts", "-o", str(out), "--gate", shop.mp.base_url
    )
    assert summary["contract_hash"] == shop.published["contract_hash"]
    assert summary["language"] == "typescript"
    async with GateClient(shop.mp.base_url) as gate:
        entry = await gate.get_offering(shop.key)
    assert from_yaml((out / "contract.nexusrpc.yaml").read_text()) == nexgen_view(entry.document)
    args = (out / "args.txt").read_text().split()
    assert args == ["typescript", str(out / "contract.nexusrpc.yaml"), "--output", str(out)]
    again = await anyone.run(
        "pull", shop.key, "--lang", "ts", "-o", str(out), "--gate", shop.mp.base_url
    )
    assert again.exit_code == 0, explain(again)  # a client pull wrote: regenerated in place

    # The default output is named after the offering: here, the seller's own project
    # (`kontra init shop` made ./shop). nexgen would replace its models.py; pull refuses.
    models = (shop.project / "models.py").read_bytes()
    here = Cli(shop.root / "nobody", env={"KONTRA_NEXGEN": str(nexgen)}, cwd=shop.root)
    clobber = await here.run("pull", shop.key, "--lang", "python", "--gate", shop.mp.base_url)
    assert clobber.exit_code == 1, explain(clobber)
    assert "did not write" in clobber.stderr
    assert (shop.project / "models.py").read_bytes() == models
    assert not (shop.project / "contract.nexusrpc.yaml").exists()

    java = await anyone.run("pull", shop.key, "--lang", "java", "--gate", shop.mp.base_url)
    assert "needs --package" in java.stderr
    wrong = await anyone.run(
        "pull",
        shop.key,
        "--lang",
        "java",
        "--package",
        "com.example.shop",
        "-o",
        str(shop.root / "elsewhere"),
        KONTRA_GATE=shop.mp.base_url,
    )
    assert "must be named 'shop'" in wrong.stderr
    missing = await anyone.run(
        "pull", f"{shop.party}/nothing", "--lang", "go", "--gate", shop.mp.base_url
    )
    assert missing.exit_code == 1
    assert missing.stderr.startswith("error: the gate answered 404")


async def test_pull_of_a_retired_version_warns_and_still_pulls(shop: Shop) -> None:
    """A retired version takes no new jobs, but the jobs hired under it still read against
    its contract (decisions I22): `pull` says so on stderr and writes the client all the
    same -- also from a document spec v1 sealed, as was every version the 0.2.0 release
    retired."""
    nexgen = _stand_in_nexgen(shop.root / "fake-nexgen-retired")
    anyone = Cli(shop.root / "nobody", env={"KONTRA_NEXGEN": str(nexgen)})
    row = shop.mp.gate.offerings[shop.key]
    info = row.info
    seller_key = Profile.load(shop.seller.home).public_key
    v1 = seal(
        {**info.document, "x-kontra": {**info.document["x-kontra"], "public_key": seller_key}}
    )
    try:
        row.retired = True
        for spec, document in (("v2", info.document), ("v1", v1)):
            row.info = dataclasses.replace(
                info, document=document, contract_hash=contract_hash(document)
            )
            out = shop.root / f"retired-client-{spec}"
            pulled = await anyone.run(
                "pull", shop.key, "--lang", "ts", "-o", str(out), "--gate", shop.mp.base_url,
                "--json",
            )  # fmt: skip
            assert pulled.exit_code == 0, (spec, explain(pulled))
            assert json.loads(pulled.stdout)["contract_hash"] == contract_hash(document), spec
            assert f"warning: {shop.key} is retired: it takes no new jobs" in pulled.stderr, spec
            assert (out / "index.ts").is_file(), spec
        row.info, row.retired = info, False
        fresh = await anyone.run(
            "pull", shop.key, "--lang", "ts", "-o", str(shop.root / "live-client"),
            "--gate", shop.mp.base_url,
        )  # fmt: skip
        assert fresh.exit_code == 0, explain(fresh)
        assert "retired" not in fresh.stderr
    finally:
        row.info, row.retired = info, False


async def test_job_result_downloads_the_artifacts(shop: Shop) -> None:
    """The enrich example (an Artifact output) under this seller: `job --result` writes
    result.json and the decrypted artifact, named by its key."""
    enrich = shop.root / "enrich"
    shutil.copytree(
        REPO / "examples" / "enrich", enrich, ignore=shutil.ignore_patterns("__pycache__")
    )
    offering = enrich / "offering.yaml"
    offering.write_text(
        offering.read_text().replace("name: acme/enrich", f"name: {shop.party}/enrich")
    )
    await shop.seller.ok("publish", "--dir", str(enrich))
    buyer = await shop.buyer()
    urls = shop.write("urls.json", {"urls": ["http://127.0.0.1:9/a", "http://127.0.0.1:9/b"]})
    with serve_process(enrich, shop.seller.home, shop.root / "enrich.log"):
        hired = await buyer.json("hire", f"{shop.party}/enrich", "-f", str(urls))
        out = shop.root / "enrich-result"
        written = await buyer.json("job", hired["job_id"], "--result", str(out))
    result = json.loads((out / "result.json").read_text())
    assert result["summary"] == {"urls": 2, "ok": 0, "failed": 2}
    artifact = result["records"]
    assert written["artifacts"] == [str(out / artifact["key"])]
    lines = (out / artifact["key"]).read_text().splitlines()
    assert [json.loads(line)["url"] for line in lines] == [
        "http://127.0.0.1:9/a",
        "http://127.0.0.1:9/b",
    ]
    assert (out / artifact["key"]).stat().st_size == artifact["size"]
    # The decrypted delivery is the buyer's alone: the directory made for it is 0700, and
    # every file in it 0600.
    assert stat.S_IMODE(out.stat().st_mode) == 0o700
    for written_file in (out / "result.json", out / artifact["key"]):
        assert stat.S_IMODE(written_file.stat().st_mode) == 0o600, written_file


async def test_job_result_into_an_existing_directory_is_the_buyers_alone(shop: Shop) -> None:
    """`job --result DIR` into a directory that exists (0755, as most do): what it writes
    there is decrypted plaintext, so result.json is 0600 whatever the umask, and the
    directories it creates below are 0700 each, parents included (``mkdir -p`` would make
    them 0755). The existing directory itself is left as it is."""
    buyer = await shop.buyer()
    hired = await buyer.json("hire", shop.key, "-f", str(shop.input_file))
    shared = shop.root / f"shared-{hired['job_id']}"
    shared.mkdir(mode=0o755)
    shared.chmod(0o755)
    old = os.umask(0o022)
    try:
        await buyer.ok("job", hired["job_id"], "--result", str(shared))
        nested = shared / "new" / "deeper"
        await buyer.ok("job", hired["job_id"], "--result", str(nested))
    finally:
        os.umask(old)
    assert stat.S_IMODE(shared.stat().st_mode) == 0o755
    for made in (shared / "new", nested):
        assert stat.S_IMODE(made.stat().st_mode) == 0o700, made
    for result in (shared / "result.json", nested / "result.json"):
        assert stat.S_IMODE(result.stat().st_mode) == 0o600, result
        assert json.loads(result.read_text())["words"] == 11
    assert [p.name for p in shared.iterdir() if p.name.endswith(".part")] == []


async def test_serve_says_in_one_line_that_temporal_is_unreachable(shop: Shop) -> None:
    """`kontra serve` before the stack is up: one line naming Temporal, no traceback. (A
    party of its own: reusing the seller's refresh token would revoke the seller's session.)"""
    party = await shop.buyer(topup=None)
    profile = Profile.load(party.home)
    profile.model_copy(update={"temporal_address": "127.0.0.1:1"}).save(party.home)
    result = await party.run("serve", "--dir", str(shop.project))
    assert result.exit_code == 1, explain(result)
    assert result.stderr.startswith("error: cannot reach Temporal (Failed client connect"), (
        result.stderr
    )
    assert "Traceback" not in result.output


async def test_login_dev_to_a_gate_that_is_not_dev(tmp_path: Path) -> None:
    async with Marketplace(temporal=False, dev=False) as mp:
        cli = Cli(tmp_path / "home")
        result = await cli.run("login", "--dev", "--handle", "carol", "--gate", mp.base_url)
        assert result.exit_code == 1, explain(result)
        assert "is not a `kontra dev` gate" in result.stderr
        assert not (tmp_path / "home" / "profile").exists()
