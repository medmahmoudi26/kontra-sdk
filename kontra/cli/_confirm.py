"""Confirm it's you, in the terminal (``docs/decisions.md`` I23).

A change (``kontra publish``, ``kontra keys create|revoke``, ``kontra recipients add|revoke``)
needs the login confirmed within the gate's hour; a sign-in counts. When the gate refuses one
for want of it, :func:`terminal_confirmer` asks in place: the gate mails a link to the
address the person signs in with, this terminal prints the code that link's page asks for
(the mail never carries it), and once it is typed the command sends its request again and
carries on. Nothing was changed by the refused request: the gate refuses before it acts.

Only a person at a terminal is asked. When stderr is not a terminal (a cron job, CI, a pipe)
the gate is never made to mail anyone: the command fails with a hint to run ``kontra confirm``
in a terminal first. A ``kontra dev`` login confirms at once, silently.
"""

from __future__ import annotations

import sys
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import click

from kontra.cli._common import interruptible

if TYPE_CHECKING:
    from kontra.gate_client import GateClient
    from kontra.gate_client import models as m

__all__ = ["ROUTE_WORDS", "confirm_here", "terminal_confirmer"]

#: What the refused change was, for "Confirm it's you to ...".
ROUTE_WORDS = {
    "create_api_key": "create an API key",
    "revoke_api_key": "revoke an API key",
    "add_recipient": "add a recipient",
    "revoke_recipient": "revoke a recipient",
    "end_session": "end a sign-in",
    "publish": "publish",
    "resolve": "resolve a dispute",
}
_WAITING = "Waiting for the code to be typed... (Ctrl-C stops; nothing has changed yet)"
_STOPPED = "stopped: nothing was changed"
#: The CLI's words for a confirmation that did not complete (``ConfirmError.code``).
_FAILED = {
    # Also a confirmation the gate ended because it could not send its link (its limits, a
    # mail server's failure): the poll reads `expired` then too (kontra-cloud#36).
    "expired": "the link expired or could not be sent; nothing was changed. Run the command "
    "again for a new one (no mail? check your spam folder; an address gets only so many "
    "links an hour)",
    "burned": "the link took three wrong codes and no longer works; nothing was changed. Run "
    "the command again for a new one",
    "unavailable": "your account cannot confirm it's you by email on this gate: run `kontra "
    "login`, then the command again",
    "api_key": "an API key cannot confirm: unset KONTRA_API_KEY to use your login",
}


def interactive() -> bool:
    """Whether a person is at this terminal: stderr is one (stdout may well be a pipe, as in
    ``export KONTRA_API_KEY=$(kontra keys create)``)."""
    try:
        return sys.stderr.isatty()
    except (AttributeError, ValueError):  # replaced, or closed
        return False


def _utc(when: datetime) -> str:
    return when.astimezone(UTC).strftime("%H:%M")


async def confirm_here(
    gate: GateClient, what: str | None = None, *, extend: bool = False
) -> datetime:
    """:func:`kontra.auth.confirm_session` with this terminal's prompt and words; returns
    until when the login may make changes. ``what`` names the change that waits for it."""
    from kontra.auth import ConfirmError, confirm_prompt, confirm_session

    def show(start: m.ConfirmStart) -> None:
        click.echo(f"{confirm_prompt(start, what)}\n{_WAITING}", err=True)

    with interruptible(_STOPPED):
        try:
            until = await confirm_session(gate, show=show, extend=extend)
        except ConfirmError as e:
            raise ConfirmError(e.code, _FAILED.get(e.code, e.message)) from e
    click.echo(f"Confirmed: this login can make changes until {_utc(until)} UTC.", err=True)
    return until


async def terminal_confirmer(gate: GateClient, route: str, need: m.ConfirmationRequired) -> bool:
    """The CLI's :data:`kontra.auth.Confirmer`: a dev login confirms at once and silently; an
    email login is asked in place when a person is at this terminal, and otherwise the
    change fails as the gate answered (no mail nobody waits for); an account that only a new
    sign-in can confirm gets that said."""
    from kontra.auth import ConfirmError, _confirm_now, confirm_if_instant

    if need.confirm_with == "dev":
        return await confirm_if_instant(gate, route, need)
    if need.confirm_with == "sign_in":
        raise ConfirmError("unavailable", _FAILED["unavailable"])
    if not interactive():
        return False
    await confirm_here(gate, ROUTE_WORDS.get(route), extend=_confirm_now(route))
    return True
