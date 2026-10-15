"""``kontra recipients``: the age keys a party can be encrypted to (add, list, revoke).

Not ``kontra keys``, which is API keys (``kontra/cli/_keys.py``). The two are deliberately
separate words for deliberately separate things: **an API key authenticates, a recipient
receives ciphertext.** An API key reaches the gate; a recipient never leaves the machine
that holds its secret half.

A party holds a *set* of recipients -- one per machine, one per fleet -- and adding never
replaces (``docs/decisions.md`` I7). So a second laptop is one more recipient rather than a
conflict, and nothing anyone adds can make an in-flight delivery to another machine
unreadable. Revoking stops future deliveries reaching that key; it cannot reach back.

Managing them needs a recent human login, like publishing: a recipient added here can read
every delivery made after it.
"""

from __future__ import annotations

from datetime import datetime

import click

from kontra.cli._common import KontraGroup, emit_json, fail, printable, run

__all__ = ["recipients"]


def _when(value: datetime | None) -> str:
    return value.strftime("%Y-%m-%d %H:%MZ") if value is not None else "-"


@click.group(cls=KontraGroup)
def recipients() -> None:
    """Encryption recipients: who your deliveries can be read by (needs a recent login)."""


@recipients.command("add")
@click.option(
    "--label",
    metavar="L",
    help="Which machine or fleet holds the key, 1 to 64 printable characters "
    "(default: this machine's hostname).",
)
@click.option(
    "--public-key",
    metavar="AGE1",
    help="Register a key made elsewhere (a fleet's mounted secret). Default: this "
    "machine's key, made on first use.",
)
@click.option("--json", "as_json", is_flag=True, help="Machine-readable output.")
def add(label: str | None, public_key: str | None, as_json: bool) -> None:
    """Add a recipient. With no options: this machine's own key.

    For a fleet, make the key once, mount it into every worker as KONTRA_KEY_FILE, and
    register its public half here with --public-key. One key per fleet, never one per pod:
    pod churn then never changes the set, so an input encrypted a minute ago is still
    readable by whichever worker picks the task up.
    """
    from kontra.auth import Session, this_machine_label
    from kontra.codec.age import is_recipient
    from kontra.gate_client import models as m
    from kontra.profile import Profile

    if label is not None and (
        not 0 < len(label) <= 64 or not label.isprintable() or label != label.strip()
    ):
        fail("--label must be 1 to 64 printable characters, no surrounding spaces")

    async def go() -> tuple[m.Me, str]:
        async with Session.from_env() as session:
            key = public_key
            if key is None:
                _, key = Profile.ensure_identity(session.identity_home)
            elif not is_recipient(key):
                fail("--public-key must be an age X25519 recipient (age1...)")
            chosen = label
            if chosen is None:  # this machine's name, unique among the party's labels
                chosen = this_machine_label(k.label for k in (await session.gate.me()).keys)
            return await session.gate.add_recipient(chosen, key), key

    me, key = run(go)
    if as_json:
        emit_json(me.model_dump(mode="json"))
        return
    added = next((k for k in me.keys if k.public_key == key), None)
    name = printable(added.label) if added is not None else "that key"
    click.echo(f"{name} can now read {me.party}'s deliveries: {printable(key)}")
    click.echo(f"{len(me.keys)} recipient(s); every new delivery is encrypted to all of them")


@recipients.command("list")
@click.option("--json", "as_json", is_flag=True, help="Machine-readable output.")
def list_(as_json: bool) -> None:
    """Your recipients: every key a counterparty encrypts to."""
    from kontra.auth import Session
    from kontra.gate_client import models as m

    async def go() -> m.Me:
        async with Session.from_env() as session:
            return await session.gate.me()

    me = run(go)
    if as_json:
        emit_json(me.model_dump(mode="json"))
        return
    if not me.keys:
        click.echo(
            "no recipients. Nothing can be delivered to this party until one is added: "
            "`kontra recipients add`. (Hiring needs none -- a job makes its own key.)"
        )
        return
    for k in me.keys:  # the gate's answer: printed, never trusted to be printable
        click.echo(f"{printable(k.public_key)}  added {_when(k.added_at)}  {printable(k.label)}")


@recipients.command("revoke")
@click.argument("public_key", metavar="AGE1")
def revoke(public_key: str) -> None:
    """Stop encrypting to AGE1: deliveries after this are unreadable on that host.

    What is already encrypted to it stays encrypted to it -- revoking is not a reach back
    into ciphertext that has left. Revoke a lost laptop's key and its future is closed; its
    past is only closed by the jobs it holds being accepted and shredded.
    """
    from kontra.auth import Session
    from kontra.codec.age import is_recipient

    if not is_recipient(public_key):
        fail("that is not an age X25519 recipient (age1...); see `kontra recipients list`")

    async def go() -> None:
        async with Session.from_env() as session:
            await session.gate.revoke_recipient(public_key)

    run(go)
    click.echo(f"revoked {printable(public_key)}; later deliveries are not encrypted to it")
