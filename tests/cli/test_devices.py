"""``kontra devices`` and ``kontra login``'s device half (decisions I25), against a served fake
gate (no Temporal): a login makes this machine a device and says so, ``--no-device`` does
not, the list marks this machine, ``add`` and ``remove`` are a person's changes, and a
removed key never comes back."""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from dataclasses import dataclass
from pathlib import Path

import pytest
import pytest_asyncio

from kontra.profile import Profile
from kontra.testing import Marketplace

from .support import Cli, handle

pytestmark = pytest.mark.asyncio(loop_scope="module")

ADDED = "device    this machine reads the jobs you hire anywhere"


@dataclass
class Market:
    mp: Marketplace
    root: Path

    def cli(self, who: str, machine: str = "") -> Cli:
        return Cli(self.root / f"{who}{machine}", cwd=self.root)


@pytest_asyncio.fixture(loop_scope="module", scope="module")
async def market(tmp_path_factory: pytest.TempPathFactory) -> AsyncIterator[Market]:
    async with Marketplace(temporal=False) as mp:
        yield Market(mp, tmp_path_factory.mktemp("devices"))


async def test_login_makes_this_machine_a_device(market: Market) -> None:
    who = handle("dev")
    cli = market.cli(who)
    out = await cli.ok("login", "--dev", "--handle", who, "--gate", market.mp.base_url)
    assert ADDED in out.stdout
    me = Profile.load(cli.home).public_key
    [device] = (await cli.json("devices", "list"))["devices"]
    assert (device["public_key"], device["kind"]) == (me, "cli")
    listed = (await cli.ok("devices", "list")).stdout
    assert "(this machine)" in listed
    assert "command line" in listed
    state = json.loads((cli.home / "devices.json").read_text())
    [entry] = state.values()
    assert entry["device"] == "added"

    again = await cli.ok("login", "--dev", "--handle", who, "--gate", market.mp.base_url)
    assert ADDED in again.stdout  # the same live key: the same device, no second row
    assert len((await cli.json("devices", "list"))["devices"]) == 1


async def test_no_device_keeps_a_seller_box_out(market: Market) -> None:
    who = handle("box")
    cli = market.cli(who)
    out = await cli.ok(
        "login", "--dev", "--handle", who, "--gate", market.mp.base_url, "--no-device"
    )
    assert "not one of your devices (--no-device)" in out.stdout
    assert (await cli.json("devices", "list"))["devices"] == []
    assert "no devices" in (await cli.ok("devices", "list")).stdout
    # on purpose, later: a person's change, asked of a recent sign-in (this one is)
    added = await cli.json("devices", "add", "--label", "build box")
    assert (added["label"], added["kind"]) == ("build box", "cli")
    assert added["public_key"] == Profile.load(cli.home).public_key


async def test_remove_a_device_and_it_never_comes_back(market: Market) -> None:
    who = handle("rm")
    laptop, desktop = market.cli(who), market.cli(who, "-desktop")
    for cli in (laptop, desktop):
        await cli.ok("login", "--dev", "--handle", who, "--gate", market.mp.base_url)
    desktop_key = Profile.load(desktop.home).public_key
    assert desktop_key is not None
    assert len((await laptop.json("devices", "list"))["devices"]) == 2

    out = await laptop.ok("devices", "remove", desktop_key)
    assert f"removed {desktop_key}" in out.stdout
    assert "can't be taken back" in out.stdout
    [left] = (await laptop.json("devices", "list"))["devices"]
    assert left["public_key"] == Profile.load(laptop.home).public_key

    refused = await desktop.run("devices", "add")
    assert refused.exit_code == 1
    assert "device_revoked" in refused.stderr
    assert "can never be added again" in refused.stderr
    login = await desktop.ok("login", "--dev", "--handle", who, "--gate", market.mp.base_url)
    assert "was removed as a device" in login.stdout  # a login says so, and still works

    mine = Profile.load(laptop.home).public_key
    assert mine is not None
    out = await laptop.ok("devices", "remove", mine)
    assert out.stdout.startswith("removed this machine:")


async def test_an_api_key_changes_no_device(market: Market) -> None:
    who = handle("prog")
    cli = market.cli(who)
    await cli.ok("login", "--dev", "--handle", who, "--gate", market.mp.base_url)
    key = (await cli.json("keys", "create", "--label", "ci"))["key"]
    program = Cli(
        cli.home, {"KONTRA_API_KEY": key, "KONTRA_GATE": market.mp.base_url}, cwd=market.root
    )
    assert len((await program.json("devices", "list"))["devices"]) == 1  # it may list them
    refused = await program.run("devices", "add")
    assert refused.exit_code == 1
    assert "fresh_login_required" in refused.stderr
    assert "changing recipients or devices" in refused.stderr


async def test_a_bad_key_never_reaches_the_gate(market: Market) -> None:
    who = handle("bad")
    cli = market.cli(who)
    await cli.ok("login", "--dev", "--handle", who, "--gate", market.mp.base_url)
    refused = await cli.run("devices", "remove", "age1nope")
    assert refused.exit_code == 1
    assert "not a device key" in refused.stderr
    refused = await cli.run("devices", "add", "--label", "café")
    assert refused.exit_code == 1
    assert "printable ASCII" in refused.stderr


async def test_a_machine_removed_by_mistake_comes_back_with_a_new_key(market: Market) -> None:
    """L8: a removed key never comes back, so `kontra devices add --new-key` gives this machine
    a device key of its own ($KONTRA_HOME/device.key, 0600) -- never a new ~/.kontra/key,
    which is also this party's recipient and reads its older jobs. From then on it is this
    machine's device: listed as this machine, registered again by `kontra login`."""
    import stat

    from kontra.codec.age import public_key_of

    who = handle("back")
    laptop, desktop = market.cli(who), market.cli(who, "-desktop")
    for cli in (laptop, desktop):
        await cli.ok("login", "--dev", "--handle", who, "--gate", market.mp.base_url)
    login_key = Profile.load(desktop.home).public_key
    assert login_key is not None
    await laptop.ok("devices", "remove", login_key)
    refused = await desktop.run("devices", "add")
    assert "--new-key" in refused.stderr

    out = await desktop.ok("devices", "add", "--new-key")
    device_file = desktop.home / "device.key"
    assert stat.S_IMODE(device_file.stat().st_mode) == 0o600
    new_key = public_key_of(Profile.device_identity(desktop.home))
    assert new_key != login_key
    assert new_key in out.stdout
    listed = {d["public_key"] for d in (await laptop.json("devices", "list"))["devices"]}
    assert new_key in listed
    assert login_key not in listed
    assert "(this machine)" in (await desktop.ok("devices", "list")).stdout
    assert Profile.load(desktop.home).public_key == login_key  # the recipient stays as it was

    again = await desktop.ok("login", "--dev", "--handle", who, "--gate", market.mp.base_url)
    assert ADDED in again.stdout  # the new key, still a device
    refused = await desktop.run("devices", "add", "--new-key")
    assert refused.exit_code == 1
    assert "already one of your devices" in refused.stderr


async def test_a_read_prints_before_it_catches_up(
    market: Market, monkeypatch: pytest.MonkeyPatch
) -> None:
    """M2: `kontra jobs` prints what was asked first, then catches up the account's devices
    with a small budget (a hire or a login gets the larger one)."""
    import click

    from kontra.hire import Kontra

    budgets: list[float] = []

    async def sync(self: Kontra, *, budget: float = 5.0, notify: object = None) -> None:
        budgets.append(budget)
        click.echo("<caught up>")

    monkeypatch.setattr(Kontra, "sync_job_keys", sync)
    who = handle("read")
    cli = market.cli(who)
    await cli.ok("login", "--dev", "--handle", who, "--gate", market.mp.base_url)
    out = (await cli.ok("jobs")).stdout
    assert out.index("no jobs as buyer") < out.index("<caught up>")
    from kontra._devices import READ_BUDGET

    assert budgets == [READ_BUDGET]
