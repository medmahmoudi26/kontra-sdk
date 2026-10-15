"""A program on an API key, end to end on a dev server (PRD §10 P3, decisions I19): it
hires, gets the result and pays with ``KONTRA_API_KEY`` and only the key file on disk (and
the job keys it makes); a
seller's worker serves with a key; and neither can publish with one."""

from __future__ import annotations

import shutil
from collections.abc import AsyncIterator
from pathlib import Path

import pytest
import pytest_asyncio

from kontra.auth import API_KEY_ENV, GATE_ENV, Session
from kontra.gate_client import GateError
from kontra.gate_client import models as m
from kontra.hire import Kontra
from kontra.profile import JOBS_DIR, KEY_FILE
from kontra.serve import ServeError, offering
from kontra.testing.devserver import DevServer

from .flows import TextFlow
from .sellers import quote_text, text_worker, write_offering
from .support import World, handle, open_world

pytestmark = [pytest.mark.temporal, pytest.mark.asyncio(loop_scope="module")]


@pytest_asyncio.fixture(loop_scope="module", scope="module")
async def world(
    dev_server: DevServer, tmp_path_factory: pytest.TempPathFactory
) -> AsyncIterator[World]:
    async with open_world(dev_server, tmp_path_factory.mktemp("acme-keys")) as w:
        yield w


def _program_home(tmp_path: Path, party_home: Path) -> Path:
    """The program's machine: the party's key file (to decrypt), nothing else."""
    home = tmp_path / "program"
    home.mkdir(mode=0o700)
    shutil.copy2(party_home / KEY_FILE, home / KEY_FILE)
    return home


async def test_a_program_hires_with_a_key(
    world: World, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    party = await world.mp.party(handle("program"), balance="10.00")
    made = await party.gate.create_api_key("nightly job")
    home = _program_home(tmp_path, party.home)
    monkeypatch.setenv(API_KEY_ENV, made.key)
    monkeypatch.setenv(GATE_ENV, world.mp.base_url)
    monkeypatch.setenv("KONTRA_HOME", str(home))

    async with Kontra() as k:  # no profile anywhere: the key, and the key file
        assert k.party == party.handle
        assert k.session.uses_api_key
        job = await k.hire(world.key(), text="hello", max_price="1.00")
        result = await job.result()
        assert result.text == "HELLO"
        view = await job.accept()
        assert (view.state, view.escrow) == (m.JobState.COMPLETED, m.Escrow.RELEASED)
        assert (await k.balance()).available_minor == 1000 - 5

        entry = await k.offering(world.key())
        with pytest.raises(GateError) as e:  # a key never publishes
            await k.gate.publish(entry.document)
        assert (e.value.status, e.value.code) == (403, "fresh_login_required")
    # No profile or session: only the job's own key (decisions I7), shredded by accept.
    assert sorted(x.name for x in home.iterdir()) == [JOBS_DIR, KEY_FILE]
    assert list((home / JOBS_DIR).iterdir()) == []

    keys = (await party.gate.api_keys()).keys
    assert keys[0].last_used_at is not None


async def test_a_seller_serves_with_a_key_but_never_publishes(world: World, tmp_path: Path) -> None:
    seller = await world.mp.party(handle("keyed"))
    published = offering(
        write_offering(tmp_path, f"{seller.handle}/text"), workflow=TextFlow, quote=quote_text
    )
    await seller.gate.publish(published.document())  # a person publishes
    made = await seller.gate.create_api_key("server")
    session = await Session.from_api_key(world.mp.base_url, made.key, home=seller.home)
    try:
        assert session.profile.dev  # a dev profile publishes what it serves -- not with a key:
        unpublished = offering(
            write_offering(tmp_path, f"{seller.handle}/text"), workflow=TextFlow, quote=quote_text
        )
        with pytest.raises(ServeError, match="an API key cannot"):
            async with world.mp.serve(text_worker(unpublished), seller, session=session):
                pass

        async with world.mp.serve(text_worker(published), seller, session=session):
            buyer = await world.buyer()
            job = await buyer.k.hire(published.key, text="abc", max_price="1.00")
            assert (await job.result()).text == "ABC"
            await job.accept()
    finally:
        await session.aclose()
