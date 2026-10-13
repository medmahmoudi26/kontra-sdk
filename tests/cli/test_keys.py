"""``kontra keys``, ``kontra login --email`` and the CLI with ``KONTRA_API_KEY``, against a
served fake gate (no Temporal): keys are made, listed and revoked by a person's login; a
program's key hires and reads but publishing with it is 403 ``fresh_login_required``."""

from __future__ import annotations

import asyncio
import json
import re
import shutil
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
import pytest_asyncio

from kontra.gate_client import models as m
from kontra.profile import KEY_FILE, Profile
from kontra.testing import Marketplace
from kontra.testing.fake_gate import SentMail

from .support import Cli, explain, handle

pytestmark = pytest.mark.asyncio(loop_scope="module")

_KEY_ID = re.compile(r"key_[0-9a-f]{16}")


@dataclass
class Market:
    mp: Marketplace
    root: Path

    @property
    def url(self) -> str:
        return self.mp.base_url

    async def person(self, prefix: str = "person") -> tuple[str, Cli]:
        who = handle(prefix)
        cli = Cli(self.root / who, cwd=self.root)
        await cli.ok("login", "--dev", "--handle", who, "--gate", self.url)
        return who, cli

    async def key_for(self, cli: Cli, label: str = "program") -> tuple[str, str]:
        made = await cli.json("keys", "create", "--label", label)
        return made["key"], made["id"]

    def program(self, person: Cli, key: str) -> Cli:
        """A machine with the party's key file and an API key: no profile, no login."""
        home = self.root / f"program-{handle('p')}"
        home.mkdir(mode=0o700)
        shutil.copy2(person.home / KEY_FILE, home / KEY_FILE)
        return Cli(home, {"KONTRA_API_KEY": key, "KONTRA_GATE": self.url}, cwd=self.root)

    async def gate(self, fn: Any) -> Any:
        async def call() -> Any:
            return fn(self.mp.gate)

        return await self.mp.gate.run(call)


@pytest_asyncio.fixture(loop_scope="module", scope="module")
async def market(tmp_path_factory: pytest.TempPathFactory) -> AsyncIterator[Market]:
    async with Marketplace(temporal=False) as mp:
        yield Market(mp, tmp_path_factory.mktemp("keys"))


async def test_create_list_revoke(market: Market) -> None:
    _, cli = await market.person()
    assert "no API keys" in (await cli.ok("keys", "list")).stdout

    made = await cli.ok("keys", "create", "--label", "ci prod")
    key = made.stdout.strip()
    assert made.stdout == key + "\n"  # the key alone: `export KONTRA_API_KEY=$(...)`
    assert m.is_api_key(key)
    found = _KEY_ID.search(made.stderr)
    assert found is not None
    key_id = found.group()
    assert "shown this once" in made.stderr
    assert key not in made.stderr

    listed = await cli.json("keys", "list")
    assert [(k["id"], k["label"], k["revoked_at"]) for k in listed["keys"]] == [
        (key_id, "ci prod", None)
    ]
    text = (await cli.ok("keys", "list")).stdout
    assert key_id in text
    assert "live" in text
    assert key not in text + json.dumps(listed)

    assert (await cli.ok("keys", "revoke", key_id)).stdout == f"revoked {key_id}\n"
    [view] = (await cli.json("keys", "list"))["keys"]
    assert view["revoked_at"] is not None
    assert "revoked" in (await cli.ok("keys", "list")).stdout


async def test_the_default_label_names_this_machine(market: Market) -> None:
    _, cli = await market.person()
    made = await cli.json("keys", "create")
    assert made["label"].startswith("cli@") or made["label"] == "cli"
    assert m.is_api_key(made["key"])


async def test_revoke_never_echoes_what_it_was_given(market: Market) -> None:
    _, cli = await market.person()
    key, _ = await market.key_for(cli)
    for given, says in ((key, "an API key itself"), ("key_123", "not a key id")):
        result = await cli.run("keys", "revoke", given)
        assert result.exit_code == 1
        assert says in result.stderr
        assert given not in result.stdout + result.stderr
    result = await cli.run("keys", "revoke", "key_" + "0" * 16)
    assert result.exit_code == 1
    assert "404 not_found" in result.stderr


async def test_a_label_the_gate_would_refuse_is_refused_here(market: Market) -> None:
    _, cli = await market.person()
    for label in ("", "x" * 65, "tab\there"):
        result = await cli.run("keys", "create", "--label", label)
        assert result.exit_code == 1, explain(result)
        assert "--label" in result.stderr


async def test_a_program_reads_and_tops_up_with_its_key(market: Market) -> None:
    who, cli = await market.person()
    key, _ = await market.key_for(cli)
    program = market.program(cli, key)
    assert (await program.json("balance"))["party"] == who
    assert (await program.json("topup", "5"))["credited"] is True
    assert await program.json("jobs") == []
    assert sorted(x.name for x in program.home.iterdir()) == [KEY_FILE]  # nothing written


async def test_a_key_never_publishes_or_makes_keys(market: Market) -> None:
    """Acceptance (PRD §10 P3): `kontra publish` with a key is 403."""
    who, cli = await market.person("seller")
    await cli.ok("init", "shop", "--party", who)
    project = market.root / "shop"
    key, _ = await market.key_for(cli)
    program = market.program(cli, key)

    for args in (("publish", "--dir", str(project)), ("keys", "create"), ("keys", "list")):
        result = await program.run(*args)
        assert result.exit_code == 1, explain(result)
        assert "403 fresh_login_required" in result.stderr
        assert "unset KONTRA_API_KEY" in result.stderr
        assert key not in result.stdout + result.stderr

    published = await cli.json("publish", "--dir", str(project))  # the person can
    assert published[0]["offering"] == f"{who}/shop@0.1.0"
    shutil.rmtree(project)


async def test_a_bad_key_in_the_environment_is_one_line_without_the_key(market: Market) -> None:
    _, cli = await market.person()
    key, _ = await market.key_for(cli)
    program = market.program(cli, key)
    for bad in (key + " ", key[:-1]):
        result = await program.run("balance", KONTRA_API_KEY=bad)
        assert result.exit_code == 1
        assert "KONTRA_API_KEY is not a Kontra API key" in result.stderr
        assert key[:-1] not in result.stdout + result.stderr
    revoked = await cli.json("keys", "create")
    await cli.ok("keys", "revoke", revoked["id"])
    result = await program.run("balance", KONTRA_API_KEY=revoked["key"])
    assert result.exit_code == 1
    assert "does not accept this API key" in result.stderr
    assert revoked["key"] not in result.stdout + result.stderr


# --------------------------------------------------------------------------- login --email


async def _mail_to(market: Market, address: str) -> SentMail:
    for _ in range(500):
        sent: list[SentMail] = await market.gate(lambda g: [x for x in g.outbox if x.to == address])
        if sent:
            return sent[-1]
        await asyncio.sleep(0.02)
    raise AssertionError(f"no mail to {address}")


async def test_login_by_email(market: Market) -> None:
    gate = market.mp.gate
    gate.device_auto_approve = False  # a person approves, through the mailed link
    who = handle("mail")
    address = f"{who}@example.com"
    cli = Cli(market.root / who, cwd=market.root)
    login = asyncio.create_task(
        cli.run("login", "--email", address, "--handle", who, "--gate", market.url)
    )
    codes: list[str] = []
    try:
        mail = await _mail_to(market, address)
        codes = await market.gate(lambda g: g.pending_user_codes())  # what the terminal shows
        [code] = codes
        assert code not in mail.text

        async def type_code() -> str:  # on the link's page: the code, and the handle form
            return await gate.use_magic_link(mail.link, code, handle=who)

        assert await gate.run(type_code) == "approved"
        result = await login
    finally:
        gate.device_auto_approve = True
        if not login.done():  # a failure above: end the command's polling, then the test

            async def deny() -> None:
                for c in codes or gate.pending_user_codes():
                    gate.deny_device(c)

            await gate.run(deny)
            await login
    assert result.exit_code == 0, explain(result)
    assert f"Check {address} for a sign-in link; when the page asks, type {code}" in result.stderr
    assert f"as {who}" in result.stdout
    profile = Profile.load(cli.home)
    assert profile.party == who
    assert abs(datetime.now(UTC) - profile.auth_time) < timedelta(minutes=5)


async def test_login_email_is_not_for_dev(market: Market) -> None:
    cli = Cli(market.root / handle("x"))
    result = await cli.run("login", "--dev", "--email", "a@b.co", "--gate", market.url)
    assert result.exit_code == 1
    assert "--email" in result.stderr


async def test_login_with_a_bad_address_says_so(market: Market) -> None:
    cli = Cli(market.root / handle("x"))
    result = await cli.run("login", "--email", "Ann <ann@example.com>", "--gate", market.url)
    assert result.exit_code == 1
    assert "not a plain email address" in result.stderr
