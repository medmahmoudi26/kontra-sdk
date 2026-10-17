"""Share to my account on the fake gate (decisions I25): every scenario of
:mod:`kontra.testing.jobkey_contract` -- the platform runs the same ones against its gate --
and what only this gate shows: the rows a removal deletes, a job's wraps going with it."""

from __future__ import annotations

import itertools
from collections.abc import AsyncIterator, Sequence
from datetime import datetime
from pathlib import Path
from typing import cast

import httpx
import pytest

from kontra import _jobkeys as jk
from kontra.codec import age
from kontra.gate_client import GateClient, GateError
from kontra.gate_client import models as m
from kontra.testing.fake_gate import FakeGate, SentMail
from kontra.testing.jobkey_contract import (
    SCENARIOS,
    HiredJob,
    Scenario,
    run_jobkey_contract,
)

from .support import ScriptedControl, json_payload, offering_doc, ok_quote, wire

_ADDRESSES = itertools.count(1)


class _Tokens:
    """A sign-in's tokens, refreshed on the gate's clock when the gate refuses one."""

    def __init__(self, anon: GateClient, tokens: m.TokenResponse) -> None:
        self._anon = anon
        self._access, self._refresh = tokens.access_token, tokens.refresh_token
        self._stale = False

    async def __call__(self) -> str:
        if self._stale and self._refresh is not None:
            req = m.TokenRequest(grant_type="refresh_token", refresh_token=self._refresh)
            tok = await self._anon.token(req)
            self._access, self._refresh, self._stale = tok.access_token, tok.refresh_token, False
        return self._access

    def invalidate(self, token: str) -> None:
        self._stale = True


class Harness:
    """:class:`~kontra.testing.jobkey_contract.JobKeyHarness` for a :class:`FakeGate` on a
    scripted control plane: every sign-in from its own address, one seller for every hire."""

    def __init__(self, gate: FakeGate, control: ScriptedControl) -> None:
        self.gate = gate
        self.control = control
        control.responder = lambda p, h: ok_quote(500)
        self._clients: dict[str, GateClient] = {}
        self._seller: tuple[str, m.CatalogEntry] | None = None

    def _transport(self) -> httpx.ASGITransport:
        n = next(_ADDRESSES)
        return httpx.ASGITransport(
            app=self.gate.app, client=(f"10.{n // 256 % 256}.{n % 256}.9", 1)
        )

    def address(self, party: str) -> str:
        return f"{party}@example.com"

    async def person(self, party: str, *, email: bool = True) -> GateClient:
        transport = self._transport()
        anon = GateClient(self.gate.base_url, transport=transport)
        start = await anon.device_start()
        if email:
            address = self.gate.allow_email(self.address(party))
            await anon.magic_link(address, start.user_code)
            link = self.gate.outbox[-1].link
            outcome = await self.gate.use_magic_link(link, start.user_code, handle=party)
            assert outcome == "approved"
        else:
            assert await self.gate.approve_device(start.user_code, party, via="device:github")
        req = m.TokenRequest(grant_type=m.DEVICE_GRANT, device_code=start.device_code)
        tokens = _Tokens(anon, await anon.token(req))
        client = GateClient(self.gate.base_url, token=tokens, transport=transport)
        self._clients.setdefault(party, client)
        return client

    def _bearer(self, token: str) -> GateClient:
        async def provider() -> str:
            return token

        return GateClient(self.gate.base_url, token=provider, transport=self._transport())

    async def key(self, party: str) -> GateClient:
        person = await self.person(party)
        made = await person.create_api_key("probe")
        req = m.TokenRequest(grant_type="api_key", api_key=made.key)
        return self._bearer((await person.token(req)).access_token)

    async def worker(self, party: str) -> GateClient:
        person = await self.person(party)
        return self._bearer((await person.temporal_token()).token)

    async def hire(self, party: str, buyer_key: str) -> HiredJob:
        if self._seller is None:
            name = f"seller-{next(_ADDRESSES)}"
            seller = await self.person(name)
            self._seller = (name, await seller.publish(offering_doc(name)))
        seller_party, entry = self._seller
        buyer = self._clients.get(party) or await self.person(party)
        q = await buyer.quote(entry.name, wire(json_payload({"text": "hi"})))
        assert q.quote_id is not None
        hired = await buyer.hire(q.quote_id, buyer_key=buyer_key, max_price_minor=500)
        return HiredJob(hired.job_id, seller_party, hired.hold_token)

    async def advance(self, seconds: float) -> None:
        self.gate.advance(seconds)

    async def now(self) -> datetime:
        return self.gate.now()

    async def mails(self) -> Sequence[SentMail]:
        return list(self.gate.outbox)

    async def confirm_by_email(self, client: GateClient) -> None:
        start = await client.confirm_start()
        assert start.user_code is not None
        assert await self.gate.use_magic_link(self.gate.outbox[-1].link, start.user_code) == (
            "confirmed"
        )


@pytest.fixture
async def harness() -> AsyncIterator[Harness]:
    """A gate as production runs it (no dev login), with mail limits a contract run fits."""
    control = ScriptedControl()
    gate = FakeGate(control, dev=False, sweep_interval=0, magic_link_max_per_email=1000)
    async with gate:
        yield Harness(gate, control)


@pytest.mark.parametrize("scenario", SCENARIOS, ids=lambda s: s.__name__)
async def test_the_contract(scenario: Scenario, harness: Harness) -> None:
    await scenario(harness)


async def test_the_whole_contract_on_one_gate(harness: Harness) -> None:
    """Scenarios share a gate: each keeps to parties of its own."""
    await run_jobkey_contract(harness)


# --------------------------------------------------------------------------- this gate only


async def test_removing_a_device_deletes_its_rows_and_only_its_partys(harness: Harness) -> None:
    """The rows go, not just the listing: and only the remover's jobs' rows, when another
    party registered the same key (the key is theirs to hold; the rows are per job)."""
    gate = harness.gate
    alice, bob = await harness.person("alice"), await harness.person("bob")
    shared_secret, shared_key = age.generate_identity()
    await alice.add_device("one", shared_key, "cli")
    await bob.add_device("one", shared_key, "cli")
    stored: list[str] = []
    for party, client in (("alice", alice), ("bob", bob)):
        secret, public = age.generate_identity()
        job = await harness.hire(party, public)
        key = await client.job_key(job.job_id)
        share = m.JobKeyShare(
            job_id=job.job_id,
            proof=jk.open_challenge(key.challenge, secret),
            wraps=[
                m.WrapIn(recipient=shared_key, wrap=jk.wrap_job_key(job.job_id, secret, shared_key))
            ],
        )
        assert (await client.share_job_keys([share])).stored == 1
        stored.append(job.job_id)
    await alice.remove_device(shared_key)
    assert [k[0] for k in gate.job_key_wraps] == [stored[1]]  # bob's stays
    assert gate.devices[("alice", shared_key)].revoked_at is not None
    assert gate.devices[("bob", shared_key)].revoked_at is None
    del shared_secret


async def test_a_job_the_ledger_refused_takes_its_wraps_with_it(harness: Harness) -> None:
    """The platform's rows cascade with their job's; the fake's job only disappears when the
    ledger refuses its hold, and its wraps go with it."""
    gate = harness.gate
    alice = await harness.person("carol")
    secret, public = age.generate_identity()
    _, laptop = age.generate_identity()
    await alice.add_device("laptop", laptop, "cli")
    job = await harness.hire("carol", public)
    key = await alice.job_key(job.job_id)
    share = m.JobKeyShare(
        job_id=job.job_id,
        proof=jk.open_challenge(key.challenge, secret),
        wraps=[m.WrapIn(recipient=laptop, wrap=jk.wrap_job_key(job.job_id, secret, laptop))],
    )
    await alice.share_job_keys([share])
    gate._abandon(gate.jobs[job.job_id])  # pyright: ignore[reportPrivateUsage]
    assert gate.job_key_wraps == {}


async def test_a_job_key_listing_pages_by_job_id(harness: Harness) -> None:
    """The cursor is the last job id of a page (opaque to clients); a cursor that is not one
    is 422 ``invalid_cursor``."""
    alice = await harness.person("dave")
    _, laptop = age.generate_identity()
    await alice.add_device("laptop", laptop, "cli")
    jobs = sorted(
        [(await harness.hire("dave", age.generate_identity()[1])).job_id for _ in range(3)]
    )
    first = await alice.job_keys(laptop, limit=2)
    assert ([k.job_id for k in first.items], first.next_cursor) == (jobs[:2], jobs[1])
    rest = await alice.job_keys(laptop, cursor=first.next_cursor, limit=2)
    assert ([k.job_id for k in rest.items], rest.next_cursor) == (jobs[2:], None)
    with pytest.raises(GateError) as e:
        await alice.job_keys(laptop, cursor="nope")
    assert (e.value.status, e.value.code) == (422, "invalid_cursor")


# --------------------------------------------------------------------------- the client's half


async def _machine(harness: Harness, party: str, home: Path) -> tuple[GateClient, str, str]:
    """A signed-in machine of ``party`` with its key file in ``home``, as a device."""
    from kontra import _devices
    from kontra.profile import Profile

    client = await harness.person(party)
    secret, public = Profile.ensure_identity(home)
    outcome = await _devices.register_device(
        client, public, home=home, gate_url=harness.gate.base_url, party=party
    )
    assert outcome == "added"
    return client, secret, public


async def _hired_here(harness: Harness, party: str, home: Path) -> str:
    """A job hired from this machine: its key filed under ``home`` as `kontra hire` does."""
    from kontra.profile import Profile

    secret, public = age.generate_identity()
    job = await harness.hire(party, public)
    Profile.save_job_identity(job.job_id, secret, home)
    return job.job_id


async def test_a_catch_up_serves_new_devices_once_and_says_so_once(
    harness: Harness, tmp_path: Path
) -> None:
    from kontra import _devices

    gate_url = harness.gate.base_url
    client, secret, me = await _machine(harness, "erin", tmp_path)
    job_id = await _hired_here(harness, "erin", tmp_path)
    browser_secret, browser = age.generate_identity()
    await client.add_device("Firefox", browser, "browser")
    said: list[str] = []

    async def sync() -> _devices.SyncReport:
        return await _devices.sync_job_keys(
            client,
            home=tmp_path,
            identity=secret,
            gate_url=gate_url,
            party="erin",
            notify=lambda d: said.append(d.public_key),
        )

    first = await sync()
    assert (first.shared, sorted(first.served), said) == (2, sorted([me, browser]), [browser])
    key = await client.job_key(job_id)
    wraps = {w.recipient: w for w in key.wraps}
    assert set(wraps) == {me, browser}
    assert jk.unwrap_job_key(wraps[browser].wrap, browser_secret, key.buyer_key)
    again = await sync()
    assert (again.shared, again.served, said) == (0, [], [browser])

    # a device removed and another added: only the new one is served, and said
    await client.remove_device(browser)
    _, phone = age.generate_identity()
    await client.add_device("phone", phone, "browser")
    third = await sync()
    assert (third.shared, third.served, said) == (1, [phone], [browser, phone])
    state = _devices.DeviceState.load(tmp_path, gate_url, "erin")
    assert state.synced == {me, phone}


async def test_a_device_serves_from_its_own_wraps_too(harness: Harness, tmp_path: Path) -> None:
    """A job hired elsewhere reaches this device as a wrap; this device then serves a newer
    device from that wrap, though it never had the key file."""
    from kontra import _devices

    gate_url = harness.gate.base_url
    client, secret, me = await _machine(harness, "fred", tmp_path / "laptop")
    elsewhere_secret, elsewhere_key = age.generate_identity()
    job = await harness.hire("fred", elsewhere_key)
    key = await client.job_key(job.job_id)
    share = m.JobKeyShare(
        job_id=job.job_id,
        proof=jk.open_challenge(key.challenge, elsewhere_secret),
        wraps=[m.WrapIn(recipient=me, wrap=jk.wrap_job_key(job.job_id, elsewhere_secret, me))],
    )
    assert (await client.share_job_keys([share])).stored == 1
    _, tablet = age.generate_identity()
    await client.add_device("tablet", tablet, "browser")
    report = await _devices.sync_job_keys(
        client, home=tmp_path / "laptop", identity=secret, gate_url=gate_url, party="fred"
    )
    assert report.shared == 1
    assert {w.recipient for w in (await client.job_key(job.job_id)).wraps} == {me, tablet}

    fetched = await _devices.fetch_job_key(
        client, job.job_id, home=tmp_path / "laptop", identity=secret
    )
    assert fetched.secret == elsewhere_secret
    from kontra.profile import Profile

    assert Profile.job_identity(job.job_id, tmp_path / "laptop")[0] == elsewhere_secret


async def test_a_declined_machine_and_a_program_only_retry_their_own_shares(
    harness: Harness, tmp_path: Path
) -> None:
    from kontra import _devices

    gate_url = harness.gate.base_url
    client = await harness.person("gina")
    _, laptop = age.generate_identity()
    await client.add_device("laptop", laptop, "cli")
    secret, _ = age.generate_identity()
    _devices.decline_device(tmp_path, gate_url, "gina")
    job_id = await _hired_here(harness, "gina", tmp_path)
    _devices.mark_unshared(job_id, tmp_path)
    said: list[str] = []
    report = await _devices.sync_job_keys(
        client,
        home=tmp_path,
        identity=secret,
        gate_url=gate_url,
        party="gina",
        notify=lambda d: said.append(d.public_key),
    )
    # the share owed, nothing else -- to a device it shows first, as every machine does (L9)
    assert (report.shared, report.served, said) == (1, [], [laptop])
    assert _devices.unshared_jobs(tmp_path) == []

    program_home = tmp_path / "program"
    report = await _devices.sync_job_keys(
        client, home=program_home, identity=None, gate_url=gate_url, party="gina", catch_up=False
    )
    assert report == _devices.SyncReport()
    assert not program_home.exists()  # a program persists nothing of its own


async def test_a_gate_without_devices_is_skipped_quietly(tmp_path: Path) -> None:
    from kontra import _devices

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, json={"code": "not_found", "message": "Not Found"})

    async def token() -> str:
        return "t"

    old = GateClient("https://old.test", token=token, transport=httpx.MockTransport(handler))
    report = await _devices.sync_job_keys(
        old, home=tmp_path, identity=None, gate_url="https://old.test", party="x"
    )
    assert report.unsupported
    secret, public = age.generate_identity()
    assert (
        await _devices.register_device(
            old, public, home=tmp_path, gate_url="https://old.test", party="x"
        )
        == "unsupported"
    )
    fetched = await _devices.fetch_job_key(old, "0" * 32, home=tmp_path, identity=secret)
    assert fetched == _devices.Fetched()
    assert not (tmp_path / _devices.STATE_FILE).exists()


def test_device_state_survives_a_damaged_file(tmp_path: Path) -> None:
    from kontra import _devices

    (tmp_path / _devices.STATE_FILE).write_text("{not json")
    assert _devices.DeviceState.load(tmp_path, "https://g", "p") == _devices.DeviceState()
    (tmp_path / _devices.STATE_FILE).write_text('{"https://g p": {"seen": ["nope", 3]}}')
    assert _devices.DeviceState.load(tmp_path, "https://g", "p").seen == set()
    _, key = age.generate_identity()
    _devices.DeviceState(seen={key}, device="added").save(tmp_path, "https://g", "p")
    _devices.DeviceState(device="declined").save(tmp_path, "https://other", "p")
    loaded = _devices.DeviceState.load(tmp_path, "https://g", "p")
    assert (loaded.seen, loaded.device) == ({key}, "added")
    assert _devices.DeviceState.load(tmp_path, "https://other", "p").device == "declined"
    assert oct((tmp_path / _devices.STATE_FILE).stat().st_mode & 0o777) == "0o600"


def test_the_device_label_is_printable_ascii(monkeypatch: pytest.MonkeyPatch) -> None:
    import socket

    from kontra import _devices

    for raw, label in (
        ("MacBook-Pro.local", "MacBook-Pro.local"),
        ("café-box", "caf-box"),
        ("éé", "this machine"),
        ("x" * 80, "x" * 64),
    ):
        monkeypatch.setattr(socket, "gethostname", lambda raw=raw: raw)
        assert _devices.device_label() == label
        assert m.AddDevice(label=_devices.device_label(), public_key="k", kind="cli")


# --------------------------------------------------------------------------- review fixes


class _Calls:
    """A client that records each ``job_keys`` cursor and can fail a call on purpose."""

    def __init__(self, inner: GateClient) -> None:
        self.inner = inner
        self.cursors: list[str | None] = []
        self.fail_listing_at: int | None = None
        self.tamper_proofs = False

    def __getattr__(self, name: str) -> object:
        return getattr(self.inner, name)

    @property
    def client(self) -> GateClient:
        return cast(GateClient, self)

    async def job_keys(
        self, lacking: str, *, for_: str | None = None, cursor: str | None = None, limit: int = 100
    ) -> m.JobKeyList:
        self.cursors.append(cursor)
        if self.fail_listing_at is not None and len(self.cursors) == self.fail_listing_at:
            raise httpx.ConnectError("the network went away")
        return await self.inner.job_keys(lacking, for_=for_, cursor=cursor, limit=limit)

    async def share_job_keys(self, items: Sequence[m.JobKeyShare]) -> m.SharedJobKeys:
        if self.tamper_proofs:
            items = [i.model_copy(update={"proof": "A" * 43}) for i in items]
        return await self.inner.share_job_keys(items)


async def test_a_catch_up_resumes_where_it_stopped(
    harness: Harness, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """M2: a catch-up that runs out of time (or of network) mid-device keeps how far it got,
    per device, in devices.json; the next one starts there, so a backlog larger than one
    command's budget is still served in the end, never restarted from the first job."""
    from kontra import _devices

    monkeypatch.setattr(_devices, "PAGE_LIMIT", 2)
    gate_url = harness.gate.base_url
    client, secret, me = await _machine(harness, "hank", tmp_path)
    for _ in range(5):
        await _hired_here(harness, "hank", tmp_path)
    first = await _devices.sync_job_keys(
        client, home=tmp_path, identity=secret, gate_url=gate_url, party="hank"
    )
    assert first.served == [me]
    _, tablet = age.generate_identity()
    await client.add_device("tablet", tablet, "browser")

    calls = _Calls(client)
    calls.fail_listing_at = 2  # the second page never comes
    stopped = await _devices.sync_job_keys(
        calls.client, home=tmp_path, identity=secret, gate_url=gate_url, party="hank"
    )
    assert not stopped.complete
    assert stopped.served == []
    state = _devices.DeviceState.load(tmp_path, gate_url, "hank")
    assert tablet not in state.synced
    resume = state.cursors[tablet]
    assert resume == calls.cursors[1]  # the first page's next_cursor
    assert resume is not None

    calls = _Calls(client)
    done = await _devices.sync_job_keys(
        calls.client, home=tmp_path, identity=secret, gate_url=gate_url, party="hank"
    )
    assert calls.cursors[0] == resume  # picked up where it stopped
    assert done.complete
    assert done.served == [tablet]
    state = _devices.DeviceState.load(tmp_path, gate_url, "hank")
    assert tablet in state.synced
    assert tablet not in state.cursors
    lacking = await client.job_keys(tablet)
    assert lacking.items == []  # every job reached it


async def test_a_refused_proof_leaves_the_device_to_the_next_catch_up(
    harness: Harness, tmp_path: Path
) -> None:
    """L1: a proof the gate refuses (a signing key rotated, a bucket that rolled over while a
    page was in flight) never marks the device served: the next catch-up sends those jobs."""
    from kontra import _devices

    gate_url = harness.gate.base_url
    client, secret, _ = await _machine(harness, "ivy", tmp_path)
    job_id = await _hired_here(harness, "ivy", tmp_path)
    await _devices.sync_job_keys(
        client, home=tmp_path, identity=secret, gate_url=gate_url, party="ivy"
    )
    _, tablet = age.generate_identity()
    await client.add_device("tablet", tablet, "browser")
    calls = _Calls(client)
    calls.tamper_proofs = True
    refused = await _devices.sync_job_keys(
        calls.client, home=tmp_path, identity=secret, gate_url=gate_url, party="ivy"
    )
    assert not refused.complete
    assert refused.served == []
    assert tablet not in _devices.DeviceState.load(tmp_path, gate_url, "ivy").synced
    again = await _devices.sync_job_keys(
        client, home=tmp_path, identity=secret, gate_url=gate_url, party="ivy"
    )
    assert again.served == [tablet]
    assert again.shared == 1
    assert tablet in {w.recipient for w in (await client.job_key(job_id)).wraps}


async def test_nothing_is_copied_to_a_device_not_shown_yet(
    harness: Harness, tmp_path: Path
) -> None:
    """L9: a notice that could not be shown yet (``kontra mcp`` shows it in its next answer:
    its hook says False) holds back every copy to that device from this machine -- the
    catch-up, and the share of a job hired here -- until it is shown."""
    from kontra import _devices

    gate_url = harness.gate.base_url
    client, secret, me = await _machine(harness, "jack", tmp_path)
    job_id = await _hired_here(harness, "jack", tmp_path)
    _, tablet = age.generate_identity()
    await client.add_device("tablet", tablet, "browser")
    queued: list[str] = []

    def later(d: m.DeviceView) -> bool:
        queued.append(d.public_key)
        return False

    held = await _devices.sync_job_keys(
        client, home=tmp_path, identity=secret, gate_url=gate_url, party="jack", notify=later
    )
    assert held.served == [me]
    assert queued == [tablet]
    assert {w.recipient for w in (await client.job_key(job_id)).wraps} == {me}
    state = _devices.DeviceState.load(tmp_path, gate_url, "jack")
    assert tablet not in state.seen
    assert tablet not in state.synced
    listed = (await client.devices()).devices
    assert [
        d.public_key
        for d in _devices.shown(listed, home=tmp_path, gate_url=gate_url, party="jack", mine=me)
    ] == [me]

    _devices.mark_shown([tablet], home=tmp_path, gate_url=gate_url, party="jack")
    served = await _devices.sync_job_keys(
        client, home=tmp_path, identity=secret, gate_url=gate_url, party="jack", notify=later
    )
    assert served.served == [tablet]
    assert queued == [tablet]
    assert {w.recipient for w in (await client.job_key(job_id)).wraps} == {me, tablet}


async def test_a_catch_up_marks_this_device_used(harness: Harness, tmp_path: Path) -> None:
    """H1: each catch-up lists the devices ``for=`` this one, so a device in use is never
    taken for an idle one."""
    from kontra import _devices

    gate_url = harness.gate.base_url
    client, secret, me = await _machine(harness, "kate", tmp_path)
    await _devices.sync_job_keys(
        client, home=tmp_path, identity=secret, gate_url=gate_url, party="kate"
    )
    harness.gate.advance(3600)
    await _devices.sync_job_keys(
        client, home=tmp_path, identity=secret, gate_url=gate_url, party="kate"
    )
    [device] = (await client.devices()).devices
    assert device.public_key == me
    assert device.last_used_at is not None
    assert device.last_used_at >= harness.gate.now().replace(second=0, microsecond=0)
