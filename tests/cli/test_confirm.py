"""Confirm it's you from the terminal (decisions I23), against a served production-like fake
gate (no Temporal, not dev, sign-up by invitation): a change past the hour asks in place --
the code on this terminal, the link in the mail -- and carries on once the code is typed; a
read never asks; nothing that is not a terminal makes the gate mail anyone; ``kontra
confirm`` asks on purpose; a key never confirms."""

from __future__ import annotations

import asyncio
import re
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest
import pytest_asyncio
from click.testing import Result

from kontra.codec.age import generate_identity
from kontra.gate_client import models as m
from kontra.testing import Marketplace
from kontra.testing.fake_gate import SentMail

from .support import Cli, explain, handle

pytestmark = pytest.mark.asyncio(loop_scope="module")

HOUR = timedelta(hours=1)
SUBJECT = "Confirm it's you on Kontra"
_UNTIL = re.compile(r"Confirmed: this login can make changes until \d\d:\d\d UTC\.")


@dataclass
class Prod:
    mp: Marketplace
    root: Path

    @property
    def url(self) -> str:
        return self.mp.base_url

    async def gate(self, fn: Any) -> Any:
        async def call() -> Any:
            return fn(self.mp.gate)

        return await self.mp.gate.run(call)

    async def type_code(self, link: str, code: str) -> str:
        """The person, on the link's page, types ``code``."""
        return await self.mp.gate.run(lambda: self.mp.gate.use_magic_link(link, code))

    async def mail(self, address: str, subject: str, after: int = 0) -> SentMail:
        for _ in range(500):
            sent: list[SentMail] = await self.gate(
                lambda g: [x for x in g.outbox if x.to == address and x.subject == subject]
            )
            if len(sent) > after:
                return sent[-1]
            await asyncio.sleep(0.02)
        raise AssertionError(f"no mail {subject!r} to {address}")

    async def confirm_mails(self, address: str) -> int:
        return len(
            await self.gate(
                lambda g: [x for x in g.outbox if x.to == address and x.subject == SUBJECT]
            )
        )

    async def person(self) -> tuple[str, Cli]:
        """``kontra login --email`` as a new invited person; returns their address."""
        who = handle("mail")
        address = f"{who}@example.com"
        await self.gate(lambda g: g.allow_email(address))
        cli = Cli(self.root / who, cwd=self.root)
        login = asyncio.create_task(
            cli.run("login", "--email", address, "--handle", who, "--gate", self.url)
        )
        mail = await self.mail(address, "Your Kontra sign-in link")
        [code] = await self.gate(lambda g: g.pending_user_codes())
        approved = await self.mp.gate.run(
            lambda: self.mp.gate.use_magic_link(mail.link, code, handle=who)
        )
        assert approved == "approved"
        result = await login
        assert result.exit_code == 0, explain(result)
        return address, cli

    async def asked(self, address: str, before: int) -> tuple[SentMail, str]:
        """The confirmation mail a command just caused, and the code its terminal shows."""
        mail = await self.mail(address, SUBJECT, after=before)
        [code] = await self.gate(lambda g: g.pending_confirm_codes())
        return mail, code


@pytest_asyncio.fixture(loop_scope="module", scope="module")
async def prod(tmp_path_factory: pytest.TempPathFactory) -> AsyncIterator[Prod]:
    async with Marketplace(temporal=False, dev=False, confirm_interval=1) as mp:
        yield Prod(mp, tmp_path_factory.mktemp("confirm"))


@pytest.fixture
def at_a_terminal(monkeypatch: pytest.MonkeyPatch) -> None:
    """stderr is a person's terminal (CliRunner's is not)."""
    monkeypatch.setattr("kontra.cli._confirm.interactive", lambda: True)


async def test_a_change_past_the_hour_asks_in_place(prod: Prod, at_a_terminal: None) -> None:
    address, cli = await prod.person()
    prod.mp.gate.advance(HOUR)
    before = await prod.confirm_mails(address)
    command = asyncio.create_task(cli.run("keys", "create", "--label", "late"))
    mail, code = await prod.asked(address, before)
    assert code not in mail.text
    assert await prod.type_code(mail.link, code) == "confirmed"
    result: Result = await command
    assert result.exit_code == 0, explain(result)
    key = result.stdout.strip()
    assert m.is_api_key(key)  # stdout is still the key alone
    assert "Confirm it's you to create an API key: Kontra emailed a link" in result.stderr
    assert f"\n    {code}\n" in result.stderr
    assert "nothing has changed yet" in result.stderr
    assert mail.link not in result.stderr
    assert _UNTIL.search(result.stderr)
    # Within the hour that follows, the next change asks nothing.
    listed = await cli.json("keys", "list")
    [made] = [k for k in listed["keys"] if k["label"] == "late"]
    assert (await cli.ok("keys", "revoke", made["id"])).stdout == f"revoked {made['id']}\n"
    assert await prod.confirm_mails(address) == before + 1


async def test_a_read_never_asks(prod: Prod, at_a_terminal: None) -> None:
    address, cli = await prod.person()
    prod.mp.gate.advance(HOUR)
    assert "no API keys" in (await cli.ok("keys", "list")).stdout
    assert (await cli.ok("recipients", "list")).stdout
    assert await prod.confirm_mails(address) == 0


async def test_without_a_terminal_nobody_is_mailed(prod: Prod) -> None:
    address, cli = await prod.person()
    prod.mp.gate.advance(HOUR)
    result = await cli.run("keys", "create")
    assert result.exit_code == 1, explain(result)
    assert "403 confirmation_required" in result.stderr
    assert "run `kontra confirm` in a terminal, then this command again" in result.stderr
    assert await prod.confirm_mails(address) == 0


async def test_kontra_confirm_then_a_script(prod: Prod) -> None:
    """``kontra confirm`` asks even without a terminal (an explicit request); the script
    that follows within the hour needs nothing."""
    address, cli = await prod.person()
    prod.mp.gate.advance(HOUR)
    command = asyncio.create_task(cli.run("confirm"))
    mail, code = await prod.asked(address, 0)
    assert await prod.type_code(mail.link, code) == "confirmed"
    result = await command
    assert result.exit_code == 0, explain(result)
    assert f"\n    {code}\n" in result.stderr
    assert _UNTIL.search(result.stderr.strip().splitlines()[-1])
    assert m.is_api_key((await cli.ok("keys", "create")).stdout.strip())  # no terminal needed


#: What `kontra confirm` (or a change asking in place) says when the poll reads `expired`:
#: the link ran out, or the gate could not send it and ended the confirmation at once
#: (kontra-cloud#36). One instruction, said once; the spam folder only as a "no mail?".
_EXPIRED_WORDS = (
    "error: the link expired or could not be sent; nothing was changed. Run the command again "
    "for a new one (no mail? check your spam folder; an address gets only so many links an "
    "hour)\n"
)


async def test_an_expired_link_says_so_and_changes_nothing(prod: Prod, at_a_terminal: None) -> None:
    address, cli = await prod.person()
    prod.mp.gate.advance(HOUR)
    command = asyncio.create_task(cli.run("keys", "create", "--label", "never"))
    await prod.asked(address, 0)
    prod.mp.gate.advance(timedelta(minutes=10))
    result = await command
    assert result.exit_code == 1, explain(result)
    assert result.stderr.endswith(_EXPIRED_WORDS), explain(result)
    assert (await cli.json("keys", "list"))["keys"] == []


async def test_a_link_the_gate_could_not_send_says_so(prod: Prod) -> None:
    """The gate ends a confirmation whose link it could not send, and the poll reads
    ``expired`` at once, minutes before the link's own ten (kontra-cloud#36): `kontra
    confirm` must not blame the person for a link that never came."""
    _, cli = await prod.person()
    prod.mp.gate.advance(HOUR)
    command = asyncio.create_task(cli.run("confirm"))
    for _ in range(500):
        if await prod.gate(lambda g: g.pending_confirm_codes()):
            break
        await asyncio.sleep(0.02)
    else:
        raise AssertionError("`kontra confirm` never asked")

    def unsent(g: Any) -> None:  # what the gate's sender does when the mail fails
        for c in g._confirmations.values():
            if c.status == "pending":
                c.expires_at = g.now()

    await prod.gate(unsent)
    result = await command
    assert result.exit_code == 1, explain(result)
    assert result.stderr.endswith(_EXPIRED_WORDS), explain(result)
    assert "expired before the code was typed" not in result.stderr


async def test_a_refused_sign_in_link_says_how_long_to_wait(prod: Prod) -> None:
    """`kontra login --email` past the limits: the gate's 429 carries the wait, and the
    command says it in minutes (kontra-cloud#36)."""
    who = handle("mail")
    address = f"{who}@example.com"
    await prod.gate(lambda g: g.allow_email(address))
    cli = Cli(prod.root / who, cwd=prod.root)
    limit = prod.mp.gate.magic_link_rate_per_client
    prod.mp.gate.magic_link_rate_per_client = 0  # this client asked too often
    try:
        result = await cli.run("login", "--email", address, "--handle", who, "--gate", prod.url)
    finally:
        prod.mp.gate.magic_link_rate_per_client = limit
        # Its device login was started before the link was refused: nobody types that code.
        await prod.gate(lambda g: [g.deny_device(c) for c in g.pending_user_codes()])
    assert result.exit_code == 1, explain(result)
    assert result.stderr == (
        "error: too many sign-in links asked for; try again later; you can ask again in 60 "
        "minutes\n"
    )


async def test_three_wrong_codes_say_so(prod: Prod, at_a_terminal: None) -> None:
    address, cli = await prod.person()
    prod.mp.gate.advance(HOUR)
    command = asyncio.create_task(cli.run("recipients", "revoke", generate_identity()[1]))
    mail, code = await prod.asked(address, 0)
    wrong = [c for c in ("BCDF-GHJK", "BCDF-GHJL", "ZZZZ-ZZZZ", "XXXX-XXXX") if c != code][:3]
    for typed in wrong:
        assert await prod.type_code(mail.link, typed) == "wrong_code"
    result = await command
    assert result.exit_code == 1, explain(result)
    assert "three wrong codes" in result.stderr
    assert "Confirm it's you to revoke a recipient" in result.stderr


async def test_a_key_never_confirms(prod: Prod) -> None:
    address, cli = await prod.person()
    made = await cli.json("keys", "create")
    program = Cli(
        prod.root / f"program-{handle('p')}",
        {"KONTRA_API_KEY": made["key"], "KONTRA_GATE": prod.url},
    )
    result = await program.run("confirm")
    assert result.exit_code == 1
    assert "an API key cannot confirm: unset KONTRA_API_KEY" in result.stderr
    result = await program.run("keys", "create")
    assert result.exit_code == 1
    assert "403 fresh_login_required" in result.stderr
    assert made["key"] not in result.stdout + result.stderr
    assert await prod.confirm_mails(address) == 0


async def test_publish_asks_in_place_too(prod: Prod, at_a_terminal: None) -> None:
    address, cli = await prod.person()
    who = address.partition("@")[0]
    await cli.ok("init", "late", "--party", who)
    prod.mp.gate.advance(HOUR)
    command = asyncio.create_task(cli.run("publish", "--dir", str(prod.root / "late")))
    mail, code = await prod.asked(address, 0)
    assert await prod.type_code(mail.link, code) == "confirmed"
    result = await command
    assert result.exit_code == 0, explain(result)
    assert f"published {who}/late@0.1.0" in result.stdout
    assert "Confirm it's you to publish: " in result.stderr
