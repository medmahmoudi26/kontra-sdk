"""Share to my account, end to end on a dev server (decisions I25): a hire wraps its job's key
to every device of the account, this machine included; another machine reads that job with no
key file copied; a share that fails never fails the hire and is retried by the next catch-up;
a device added later is served by a machine that holds the keys; and a machine that can read
nothing says why."""

from __future__ import annotations

import asyncio
import stat
from collections.abc import AsyncIterator
from pathlib import Path

import pytest
import pytest_asyncio

from kontra import _devices
from kontra import _jobkeys as jk
from kontra.auth import Session, login
from kontra.codec import age
from kontra.gate_client import GateError
from kontra.gate_client import models as m
from kontra.hire import JobFailed, JobKeyMissing, Kontra, Record
from kontra.profile import Profile
from kontra.testing import TestParty
from kontra.testing.devserver import DevServer

from .support import World, open_world

pytestmark = [pytest.mark.temporal, pytest.mark.asyncio(loop_scope="module")]


@pytest_asyncio.fixture(loop_scope="module", scope="module")
async def world(
    dev_server: DevServer, tmp_path_factory: pytest.TempPathFactory
) -> AsyncIterator[World]:
    async with open_world(dev_server, tmp_path_factory.mktemp("acme-share")) as w:
        yield w


async def _wraps(k: Kontra, job_id: str) -> set[str]:
    return {w.recipient for w in (await k.gate.job_key(job_id)).wraps}


async def _another_machine(
    world: World, party: TestParty, home: Path, *, device: bool = True
) -> Kontra:
    """``kontra login`` as ``party`` on another machine (its own KONTRA_HOME and key), and a
    buyer client there."""
    await asyncio.to_thread(home.mkdir, mode=0o700)
    profile = await login(
        world.mp.base_url, handle=party.handle, dev=True, home=home, device=device
    )
    session = Session(profile, home=home)
    return Kontra(session=session, client=world.mp.client(party.namespace))


async def test_a_hire_wraps_its_key_to_every_device_itself_included(world: World) -> None:
    buyer = await world.buyer()
    me = buyer.k.public_key
    listed = [d.public_key for d in (await buyer.k.gate.devices()).devices]
    assert listed == [me]  # `kontra login` made this machine a device, asking nothing
    browser_secret, browser = age.generate_identity()
    await buyer.k.gate.add_device("Firefox on Linux", browser, "browser")

    job = await buyer.k.hire(world.key(), text="shared")
    await buyer.k.sync_job_keys()  # waits for the share under way
    assert await _wraps(buyer.k, job.job_id) == {me, browser}
    [wrap] = (await buyer.k.gate.job_key(job.job_id, for_=browser)).wraps
    secret, public = Profile.job_identity(job.job_id, buyer.party.home)
    assert jk.unwrap_job_key(wrap.wrap, browser_secret, public) == secret
    assert _devices.unshared_jobs(buyer.party.home) == []
    assert (await job.result()).text == "SHARED"


async def test_another_machine_reads_a_job_with_no_key_file_copied(
    world: World, tmp_path: Path
) -> None:
    buyer = await world.buyer()
    job = await buyer.k.hire(world.key(), text="elsewhere")
    await buyer.k.sync_job_keys()
    assert (await job.result()).text == "ELSEWHERE"

    laptop = await _another_machine(world, buyer.party, tmp_path / "laptop")
    try:
        # A device added after the hire: the first machine serves it on its next catch-up.
        assert laptop.public_key not in await _wraps(buyer.k, job.job_id)
        seen: list[m.DeviceView] = []
        report = await buyer.k.sync_job_keys(notify=seen.append)
        assert report is not None
        assert laptop.public_key in report.served
        assert [d.public_key for d in seen] == [laptop.public_key]  # said once, here
        assert laptop.public_key in await _wraps(buyer.k, job.job_id)
        again = await buyer.k.sync_job_keys(notify=seen.append)
        assert again is not None
        assert (again.shared, again.served, len(seen)) == (0, [], 1)

        key_file = Profile.job_key_path(job.job_id, tmp_path / "laptop")
        assert not key_file.exists()
        there = await laptop.job(job.job_id)
        result = await there.result(timeout=30)
        assert isinstance(result, Record)
        assert result.text == "ELSEWHERE"
        assert stat.S_IMODE(key_file.stat().st_mode) == 0o600
        assert Profile.job_identity(job.job_id, tmp_path / "laptop") == Profile.job_identity(
            job.job_id, buyer.party.home
        )
    finally:
        await laptop.aclose()
        await laptop.session.aclose()


async def test_a_share_that_fails_never_fails_the_hire_and_is_retried(world: World) -> None:
    buyer = await world.buyer()
    real = buyer.k.gate.share_job_keys
    calls: list[int] = []

    async def down(items: object) -> m.SharedJobKeys:
        calls.append(1)
        raise GateError(503, "unavailable", "the gate is restarting")

    buyer.k.gate.share_job_keys = down  # type: ignore[method-assign]
    try:
        job = await buyer.k.hire(world.key(), text="later")
        await buyer.k._settle_shares()  # pyright: ignore[reportPrivateUsage]
    finally:
        buyer.k.gate.share_job_keys = real  # type: ignore[method-assign]
    assert calls == [1]
    assert _devices.unshared_jobs(buyer.party.home) == [job.job_id]
    assert await _wraps(buyer.k, job.job_id) == set()

    report = await buyer.k.sync_job_keys()
    assert report is not None
    assert report.shared >= 1
    assert await _wraps(buyer.k, job.job_id) == {buyer.k.public_key}
    assert _devices.unshared_jobs(buyer.party.home) == []
    assert (await job.result()).text == "LATER"


async def test_a_machine_that_cannot_read_a_job_says_why(world: World, tmp_path: Path) -> None:
    buyer = await world.buyer()
    job = await buyer.k.hire(world.key(), text="private")
    await buyer.k.sync_job_keys()

    seller_box = await _another_machine(world, buyer.party, tmp_path / "box", device=False)
    try:
        with pytest.raises(JobKeyMissing) as missing:
            await (await seller_box.job(job.job_id)).result(timeout=30)
        assert missing.value.reason == "not_a_device"
        assert "kontra devices add" in str(missing.value)
        assert '"device": "declined"' in (tmp_path / "box" / "devices.json").read_text()
    finally:
        await seller_box.aclose()
        await seller_box.session.aclose()

    phone = await _another_machine(world, buyer.party, tmp_path / "phone")
    try:  # a device, but no machine holding the key has caught it up yet
        with pytest.raises(JobKeyMissing) as missing:
            await (await phone.job(job.job_id)).result(timeout=30)
        assert missing.value.reason == "no_wrap"
        assert "next online" in str(missing.value)
    finally:
        await phone.aclose()
        await phone.session.aclose()


async def test_a_job_that_failed_says_so_where_no_key_reached(world: World, tmp_path: Path) -> None:
    """M4: on a machine no copy of a job's key reached, a job that closed without a delivery
    (cancelled, failed, refunded) reports that, as on the machine that hired it -- not that
    its key is missing, which would not matter."""
    buyer = await world.buyer()
    box = await _another_machine(world, buyer.party, tmp_path / "box", device=False)
    try:
        job = await buyer.k.hire(world.key(), text="take your time", mode="slow")
        await world.job(buyer, job.job_id, lambda v: v.state is m.JobState.WORKING)
        await job.cancel()
        with pytest.raises(JobFailed) as here:
            await (await buyer.k.job(job.job_id)).result(timeout=30)
        with pytest.raises(JobFailed) as there:
            await (await box.job(job.job_id)).result(timeout=30)
        assert there.value.state == here.value.state == "canceled"
    finally:
        await box.aclose()
        await box.session.aclose()


async def test_accepting_shreds_this_machines_copy_only(world: World, tmp_path: Path) -> None:
    """``accept`` shreds the key file here (decisions I7); every device of the account --
    this machine too -- keeps its copy at the gate while the job exists (decisions I25), so
    the job still reads here, and the key file comes back."""
    buyer = await world.buyer()
    laptop = await _another_machine(world, buyer.party, tmp_path / "laptop")
    try:
        job = await buyer.k.hire(world.key(), text="kept")
        await buyer.k.sync_job_keys()
        assert (await job.result()).text == "KEPT"
        await job.accept()
        assert not Profile.job_key_path(job.job_id, buyer.party.home).exists()
        there = await laptop.job(job.job_id)
        assert (await there.result(timeout=30)).text == "KEPT"
        here = await buyer.k.job(job.job_id)
        assert (await here.result(timeout=30)).text == "KEPT"
        assert Profile.job_key_path(job.job_id, buyer.party.home).exists()
    finally:
        await laptop.aclose()
        await laptop.session.aclose()
