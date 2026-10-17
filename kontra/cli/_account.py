"""``kontra login``, ``kontra confirm``, ``kontra balance``, ``kontra topup`` (PRD §10.3,
§7.4; decisions I23)."""

from __future__ import annotations

import os
from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING

import click

from kontra.cli._common import default_handle, emit_json, fail, printable, run

if TYPE_CHECKING:
    from kontra.gate_client import models as m

__all__ = [
    "GATE_ENV",
    "balance",
    "configured_gate",
    "confirm",
    "gate_url",
    "login",
    "notice",
    "topup",
]

#: The gate to log in to when ``--gate`` is not given (else the profile's, else
#: :data:`kontra.auth.DEFAULT_GATE_URL`). The same name as ``kontra.auth.GATE_ENV``, spelled
#: here so ``kontra --help`` does not import the SDK.
GATE_ENV = "KONTRA_GATE"


def configured_gate(given: str | None) -> str | None:
    """``--gate``, else ``$KONTRA_GATE``, else the gate of the profile on this machine."""
    from kontra.profile import Profile, ProfileError

    if given:
        return given
    env = os.environ.get(GATE_ENV)
    if env:
        return env
    try:
        return Profile.load().gate_url
    except ProfileError:
        return None


def gate_url(given: str | None) -> str:
    """:func:`configured_gate`, else Kontra's own (``https://api.kontra.run``). ``kontra
    dev`` passes its local gate itself."""
    from kontra.auth import DEFAULT_GATE_URL

    return configured_gate(given) or DEFAULT_GATE_URL


@click.command()
@click.option(
    "--gate",
    "gate",
    metavar="URL",
    help=f"The gate (default: ${GATE_ENV}, else the profile's, else https://api.kontra.run).",
)
@click.option(
    "--email",
    metavar="ADDR",
    help="Sign in through a link mailed to ADDR instead of a browser here; the page asks "
    "for the code this command prints.",
)
@click.option(
    "--dev", is_flag=True, help="Log in to a `kontra dev` gate at once, without a browser."
)
@click.option(
    "--handle", metavar="H", help="The party handle to claim (first login) or use (--dev)."
)
@click.option(
    "--tls/--no-tls",
    default=None,
    help="TLS to Temporal (default: on, unless the gate is a dev gate).",
)
@click.option(
    "--no-browser", is_flag=True, help="Print the device-code URL; do not open a browser."
)
@click.option(
    "--no-device",
    is_flag=True,
    help="Do not make this machine one of your devices: it then reads only the jobs it hires "
    "itself (a seller's box).",
)
@click.option("--json", "as_json", is_flag=True, help="Print the profile as JSON.")
def login(
    gate: str | None,
    email: str | None,
    dev: bool,
    handle: str | None,
    tls: bool | None,
    no_browser: bool,
    no_device: bool,
    as_json: bool,
) -> None:
    """Log in: device flow (approved in a browser, or with --email through a mailed link;
    or --dev), add this machine to the party's recipients and to its devices, save the
    profile.

    Logging in on a second machine adds that machine. It never replaces a key, so nothing
    a login does can make another machine's deliveries unreadable. As one of your devices,
    this machine reads the jobs you hire anywhere -- in the console, on another machine --
    with nothing to export: each job's key is copied to it, encrypted to it alone."""
    from kontra._devices import DeviceOutcome
    from kontra.auth import API_KEY_ENV
    from kontra.auth import login as do_login
    from kontra.gate_client import GateError
    from kontra.profile import Profile, kontra_home

    if dev and email is not None:
        fail("--email is for a real gate; a `kontra dev` login (--dev) needs no mail")
    url = configured_gate(gate) if dev else gate_url(gate)
    if url is None:  # a dev login only makes sense against a `kontra dev` gate
        fail(f"which gate? pass --gate URL (or set {GATE_ENV}); `kontra dev` runs a local one")
    if dev and handle is None:
        handle = default_handle()
    outcomes: list[DeviceOutcome | None] = []
    try:
        profile = run(
            lambda: do_login(
                url,
                handle=handle,
                dev=dev,
                email=email,
                open_browser=not no_browser,
                tls=tls,
                device=not no_device,
                on_device=outcomes.append,
            )
        )
    except GateError as e:
        if dev and e.status == 404:
            fail(f"{url} is not a `kontra dev` gate (no dev login there); log in without --dev")
        raise
    outcome = "declined" if no_device else (outcomes[0] if outcomes else None)
    if outcome != "unsupported":
        _catch_up()
    if as_json:
        emit_json(profile.model_dump(mode="json", exclude={"refresh_token"}))
        return
    home = profile.home or kontra_home()
    click.echo(
        f"logged in to {profile.gate_url} as {profile.party} (namespace {profile.namespace})"
    )
    tls_state = "on" if profile.tls else "off"
    click.echo(f"  temporal  {printable(profile.temporal_address)} (tls {tls_state})")
    click.echo(f"  key       {profile.public_key} ({Profile.key_path(home)})")
    click.echo("              one of this party's recipients; `kontra recipients list`")
    device_line = _DEVICE_LINES.get(outcome)
    if device_line:
        click.echo(f"  device    {device_line}")
    if profile.dev:
        click.echo("  this is a dev gate: play money, `kontra topup` credits at once")
    if os.environ.get(API_KEY_ENV):
        click.echo(
            f"note: {API_KEY_ENV} is set, and commands use it instead of this login "
            "(unset it to publish or manage keys)",
            err=True,
        )


#: ``kontra login``'s line about this machine as a device (decisions I25).
_DEVICE_LINES: dict[str | None, str] = {
    "added": "this machine reads the jobs you hire anywhere (`kontra devices list`)",
    "declined": "not one of your devices (--no-device): it reads only the jobs it hires",
    "revoked": "this machine's key was removed as a device: it reads only the jobs it hires "
    "(`kontra devices add --new-key` adds it again, with a key of its own)",
    "full": "not added: your account holds the most devices it may; remove one with "
    "`kontra devices remove`, then `kontra devices add`",
    "refused": "not added this time; `kontra devices add` adds it",
    None: "not added this time; `kontra devices add` adds it",
}


def _catch_up() -> None:
    """Right after a login: serve the account's other new devices from the job keys this
    machine holds, and keep its own jobs' keys for itself (decisions I25). Never fails."""
    from kontra import _devices
    from kontra.auth import Session
    from kontra.profile import Profile, ProfileError

    async def go() -> None:
        async with Session.load() as session:
            profile = session.profile
            try:
                identity: str | None = Profile.device_identity(session.identity_home)
            except ProfileError:
                identity = None
            await _devices.sync_job_keys(
                session.gate,
                home=session.identity_home,
                identity=identity,
                gate_url=profile.gate_url,
                party=profile.party,
                notify=notice,
            )

    try:
        run(go)
    except Exception as err:
        click.echo(f"note: your devices were not caught up ({printable(err)})", err=True)


def notice(device: m.DeviceView) -> None:
    """A device this machine sees for the first time, said once on stderr."""
    from kontra._devices import notice_text

    click.echo(f"note: {printable(notice_text(device))}", err=True)


@click.command()
def confirm() -> None:
    """Confirm it's you, for an hour of changes from this login (publish, API keys,
    recipients): the gate mails a link to the address you sign in with, and its page asks
    for the code printed here. A command that needs it asks by itself in a terminal; run this
    first for a script or a batch of changes."""
    from kontra.auth import API_KEY_ENV, Session
    from kontra.cli._confirm import confirm_here

    if os.environ.get(API_KEY_ENV):
        fail(f"an API key cannot confirm: unset {API_KEY_ENV} to use your login")

    async def go() -> None:
        from kontra import _devices

        async with Session.load() as session:  # the person's login, never a key
            await confirm_here(session.gate, extend=True)
            profile = session.profile
            # A login from before devices existed becomes one now, asking nothing more.
            await _devices.register_if_undecided(
                session.gate,
                home=session.identity_home,
                gate_url=profile.gate_url,
                party=profile.party,
            )

    run(go)


def _amount(text: str, currency: str) -> Decimal:
    """A positive amount in major units that is at least one minor unit of ``currency``."""
    from kontra.ledger import CURRENCIES, Money

    if currency not in CURRENCIES:
        fail(f"unsupported currency {currency!r} (one of {', '.join(sorted(CURRENCIES))})")
    try:
        value = Decimal(text)
    except InvalidOperation:
        fail(f"{text!r} is not an amount (e.g. 25 or 25.00)")
    if not value.is_finite() or Money.from_decimal(value, currency).amount_minor <= 0:
        fail(f"the amount must be at least one minor unit of {currency}, got {text}")
    return value


@click.command()
@click.option("--currency", default="USD", show_default=True)
@click.option("--json", "as_json", is_flag=True, help="Machine-readable output.")
def balance(currency: str, as_json: bool) -> None:
    """Your available and held balance."""
    from kontra.hire import Kontra

    async def go() -> m.Balance:
        async with Kontra() as k:
            return await k.balance(currency)

    bal = run(go)
    if as_json:
        emit_json(bal)
        return
    click.echo(f"{bal.party}: {bal.available} {bal.currency} available, {bal.held} held")


@click.command()
@click.argument("amount")
@click.option("--currency", default="USD", show_default=True)
@click.option("--json", "as_json", is_flag=True, help="Machine-readable output.")
def topup(amount: str, currency: str, as_json: bool) -> None:
    """Add AMOUNT (e.g. 25.00) to your balance: play money under `kontra dev`, a checkout
    link elsewhere."""
    from kontra.hire import Kontra

    value = _amount(amount, currency)

    async def go() -> m.TopupResult:
        async with Kontra() as k:
            return await k.topup(value, currency)

    result = run(go)
    if as_json:
        emit_json(result)
        return
    if result.credited and result.balance is not None:
        b = result.balance
        click.echo(f"credited; {b.party}: {b.available} {b.currency} available, {b.held} held")
    elif result.checkout_url:
        click.echo(f"complete the payment at {printable(result.checkout_url)}")
    else:
        click.echo("the gate accepted the top-up; it is not credited yet")
