"""``kontra devices``: the browsers and computers that read the jobs you hire (decisions I25).

Each job's key is made where the job is hired. Every device of the account gets a copy of
it, encrypted so that only that device can open it; Kontra keeps the encrypted copies and
cannot read them. So a job hired in the console reads here with ``kontra job <id> --result``
and nothing to export or import. This machine's device key is its ``~/.kontra/key``:
``kontra login`` adds it (``--no-device`` does not), right after the sign-in, which is the
confirmation adding a device needs. A key removed as a device never comes back: ``kontra
devices add --new-key`` then gives this machine a device key of its own
(``~/.kontra/device.key``), never a new ``~/.kontra/key`` (a recipient, which reads older
jobs and what others encrypt to you).

Not ``kontra recipients``: a recipient receives what *other* parties encrypt to you (seller
input, deliveries before per-job keys) and is public; a device receives copies of your own
jobs' keys and is listed only to you.

Adding a device needs a confirmation of the last five minutes (a device also gets the keys of
past jobs); removing one is a change. Removing a device stops new copies and deletes the
copies Kontra kept for it. What it has already opened cannot be taken back.
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

import click

from kontra.cli._common import KontraGroup, cli_session, emit_json, fail, printable, run

if TYPE_CHECKING:
    from kontra.gate_client import models as m

__all__ = ["devices"]

_KIND = {"browser": "browser", "cli": "command line"}
_REMOVED = (
    "it gets no key of a new job, and Kontra deleted the copies it kept for it. Keys it has "
    "already opened can't be taken back; if it was taken from you, also sign that sign-in out "
    "in the console (API keys > Where you are signed in)."
)


def _when(value: datetime | None) -> str:
    return value.strftime("%Y-%m-%d %H:%MZ") if value is not None else "-"


def _this_key() -> str | None:
    """This machine's device key: its own device key, else its key file."""
    from kontra.codec.age import public_key_of
    from kontra.profile import Profile, ProfileError

    try:
        return public_key_of(Profile.device_identity())
    except ProfileError:
        return None


@click.group(cls=KontraGroup)
def devices() -> None:
    """Devices: the browsers and computers that read the jobs you hire."""


@devices.command("list")
@click.option("--json", "as_json", is_flag=True, help="Machine-readable output.")
def list_(as_json: bool) -> None:
    """Your devices, oldest first; this machine is marked."""
    from kontra.auth import Session

    async def go() -> m.DeviceList:
        async with Session.from_env() as session:
            return await session.gate.devices()

    found = run(go)
    if as_json:
        emit_json(found)
        return
    mine = _this_key()
    if not found.devices:
        click.echo(
            "no devices: only the machine or browser that hired a job reads it. "
            "`kontra devices add` adds this machine."
        )
        return
    for d in found.devices:  # the gate's answer: printed, never trusted to be printable
        here = "  (this machine)" if d.public_key == mine else ""
        by = f" by sign-in {d.added_by}" if d.added_by else ""
        click.echo(
            f"{printable(d.public_key)}  {_KIND.get(d.kind, d.kind):12}  "
            f"added {_when(d.added_at)}{by}  last used {_when(d.last_used_at)}  "
            f"{printable(d.label)}{here}"
        )


@devices.command("add")
@click.option(
    "--label",
    metavar="L",
    help="A name for this machine, 1 to 64 printable ASCII characters (default: its hostname).",
)
@click.option(
    "--new-key",
    is_flag=True,
    help="Add this machine with a device key of its own (~/.kontra/device.key): for a machine "
    "whose key was removed as a device, which never comes back.",
)
@click.option("--json", "as_json", is_flag=True, help="Machine-readable output.")
def add(label: str | None, new_key: bool, as_json: bool) -> None:
    """Add this machine: it reads the jobs you hire anywhere, past ones too.

    It needs you to confirm it's you now (asked in place in a terminal); `kontra login` does
    this by itself. Your other devices copy it the keys of your jobs the next time they are
    online. A machine removed as a device by mistake comes back with --new-key.
    """
    import re

    from kontra import _devices
    from kontra.codec.age import generate_identity, public_key_of
    from kontra.profile import Profile

    if label is not None and not re.fullmatch(r"[\x20-\x7e]{1,64}", label):
        fail("--label must be 1 to 64 printable ASCII characters")

    async def go() -> m.DeviceView:
        async with cli_session() as session:
            profile = await session.resolve()
            home = session.identity_home
            Profile.ensure_identity(home)
            secret: str | None = None
            key = public_key_of(Profile.device_identity(home))
            if new_key:
                if key in {d.public_key for d in (await session.gate.devices()).devices}:
                    fail(f"this machine is already one of your devices ({key})")
                secret, key = generate_identity()
            view = await session.gate.add_device(label or _devices.device_label(), key, "cli")
            if secret is not None:  # only once the gate took it: never a key nobody added
                Profile.save_device_identity(secret, home)
            state = _devices.DeviceState.load(home, profile.gate_url, profile.party)
            state.device, state.seen = "added", state.seen | {key}
            state.save(home, profile.gate_url, profile.party)
            return view

    view = run(go)
    if as_json:
        emit_json(view)
        return
    click.echo(
        f"this machine ({printable(view.label)}) reads the jobs you hire anywhere: "
        f"{printable(view.public_key)}"
    )
    click.echo("your other devices copy it the keys of your jobs when they are next online")


@devices.command("remove")
@click.argument("public_key", metavar="AGE1")
def remove(public_key: str) -> None:
    """Remove the device AGE1 (see `kontra devices list`)."""
    from kontra.codec.age import is_recipient

    if not is_recipient(public_key):
        fail("that is not a device key (age1...); see `kontra devices list`")

    async def go() -> None:
        async with cli_session() as session:
            await session.gate.remove_device(public_key)

    run(go)
    which = "this machine" if public_key == _this_key() else printable(public_key)
    click.echo(f"removed {which}: {_REMOVED}")
