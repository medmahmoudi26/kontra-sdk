"""``kontra keys create|list|revoke``: API keys for programs (decisions I19).

A key is what a script, a CI job or a server uses instead of a person's login
(``KONTRA_API_KEY``): it can quote, hire, read and ``kontra serve``, and never publish, change
recipients or manage keys. Listing keys therefore needs a person's login (``kontra login``),
never a key; making and revoking one are changes, which ask the person to confirm it's them
in place when the login is more than an hour old (decisions I23).
"""

from __future__ import annotations

import re
import socket
from datetime import datetime

import click

from kontra.cli._common import KontraGroup, cli_session, emit_json, fail, printable, run

__all__ = ["keys"]

#: What a key's label may hold: 1 to 64 printable ASCII characters (the gate's rule).
_LABEL = re.compile(r"[\x20-\x7e]{1,64}", re.ASCII)
_NOT_PRINTABLE = re.compile(r"[^\x20-\x7e]+", re.ASCII)


def _default_label() -> str:
    """``cli@<this machine's name>``: where the key was made, for the list later."""
    try:
        host = socket.gethostname()
    except OSError:  # pragma: no cover - no hostname at all
        host = ""
    host = _NOT_PRINTABLE.sub("", host)
    return f"cli@{host}"[:64] if host else "cli"


def _when(value: datetime | None) -> str:
    return value.strftime("%Y-%m-%d %H:%MZ") if value is not None else "-"


@click.group(cls=KontraGroup)
def keys() -> None:
    """API keys for programs: they hire and serve, never publish (needs your login)."""


@keys.command("create")
@click.option(
    "--label",
    metavar="L",
    help="What the key is for, 1 to 64 printable characters (default: cli@<this machine>).",
)
@click.option("--json", "as_json", is_flag=True, help="The key and its id as JSON.")
def create(label: str | None, as_json: bool) -> None:
    """Make a key and print it, once: it is never shown again."""
    from kontra.gate_client import models as m

    chosen = label if label is not None else _default_label()
    if _LABEL.fullmatch(chosen) is None:
        fail("--label must be 1 to 64 printable ASCII characters")

    async def go() -> m.ApiKeyCreated:
        async with cli_session() as session:
            return await session.gate.create_api_key(chosen)

    made = run(go)
    if as_json:
        emit_json(made.model_dump(mode="json"))
        return
    # stdout holds the key alone: `export KONTRA_API_KEY=$(kontra keys create)` works.
    click.echo(made.key)
    click.echo(
        f"created API key {made.id} ({printable(made.label)}). It is shown this once: keep it "
        "secret, e.g. as KONTRA_API_KEY for a program. It can hire and serve, never publish "
        f"or manage keys; `kontra keys revoke {made.id}` ends it.",
        err=True,
    )


@keys.command("list")
@click.option("--json", "as_json", is_flag=True, help="Machine-readable output.")
def list_(as_json: bool) -> None:
    """Your keys: live ones first, then the latest revoked."""
    from kontra.gate_client import models as m

    async def go() -> m.ApiKeyList:
        async with cli_session() as session:
            return await session.gate.api_keys()

    found = run(go)
    if as_json:
        emit_json(found)
        return
    if not found.keys:
        click.echo("no API keys; `kontra keys create` makes one")
        return
    for k in found.keys:  # the gate's answer: printed, never trusted to be printable
        state = f"revoked {_when(k.revoked_at)}" if k.revoked_at is not None else "live"
        click.echo(
            f"{printable(k.id)}  {state:<24}  created {_when(k.created_at)}  "
            f"last used {_when(k.last_used_at)}  {printable(k.label)}"
        )


@keys.command("revoke")
@click.argument("key_id", metavar="ID")
def revoke(key_id: str) -> None:
    """Revoke key ID (key_...): it stops working at once, everywhere."""
    from kontra.gate_client import models as m

    if not m.is_api_key_id(key_id):
        # Never echoed: the argument may be the secret itself, pasted by mistake.
        what = "an API key itself" if key_id.startswith(m.API_KEY_PREFIX) else "not a key id"
        fail(f"that is {what}: give the id (key_ and 16 hex digits; see `kontra keys list`)")

    async def go() -> None:
        async with cli_session() as session:
            await session.gate.revoke_api_key(key_id)

    run(go)
    click.echo(f"revoked {key_id}")
