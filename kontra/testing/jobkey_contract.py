"""The share-to-my-account contract (decisions I25): the scenarios every gate passes, so a rule
of devices and job-key wraps cannot hold on the fake gate and not on the platform (seam ②).

:class:`~kontra.testing.fake_gate.FakeGate` is proven against it in this repository; the
platform runs the same coroutines against its own gate::

    from kontra.testing.jobkey_contract import SCENARIOS

    @pytest.mark.parametrize("scenario", SCENARIOS, ids=lambda s: s.__name__)
    async def test_job_keys(scenario, harness):
        await scenario(harness)

A scenario is black-box: it speaks HTTP through :class:`~kontra.gate_client.GateClient`, makes
and opens wraps and challenges with :mod:`kontra._jobkeys` (as the clients do), and reaches the
gate otherwise only through a :class:`JobKeyHarness` -- new sign-ins, an API key, a worker
token, a hired job, the gate's clock and its mail. Each uses parties of its own, so scenarios
may share a gate. The gate they need: "Confirm it's you" by email (not a dev gate),
``KONTRA_CONFIRM_TTL`` an hour, a clock the harness can move, and mail limits that let a party
sign in a dozen times.

What only one gate can show stays in its own tests: the rows themselves (removal deletes them,
a job's deletion cascades), concurrent shares under one key, and jobs
from before per-job keys. A failure raises :class:`JobKeyContractViolation` naming the rule.
"""

from __future__ import annotations

import base64
import uuid
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

import jwt

from kontra import _jobkeys as jk
from kontra.codec import age
from kontra.gate_client import GateClient, GateError
from kontra.gate_client import models as m

__all__ = [
    "SCENARIOS",
    "HiredJob",
    "JobKeyContractViolation",
    "JobKeyHarness",
    "Mail",
    "Scenario",
    "run_jobkey_contract",
]

#: The one-hour confirmation the contract's gate keeps (``KONTRA_CONFIRM_TTL``).
CONFIRM_TTL_SECONDS = 3600


class JobKeyContractViolation(AssertionError):
    """A gate broke a rule of devices or job-key wraps; the message names it."""


class Mail(Protocol):
    """A mail the gate sent (:class:`kontra.testing.fake_gate.SentMail` is one)."""

    @property
    def to(self) -> str: ...

    @property
    def subject(self) -> str: ...

    @property
    def text(self) -> str: ...


@dataclass(frozen=True, slots=True)
class HiredJob:
    """A job the harness hired: its id, its seller party, the hold token the gate signed."""

    job_id: str
    seller: str
    hold_token: str


class JobKeyHarness(Protocol):
    """How a scenario reaches the gate besides HTTP. Every client it returns has no
    ``confirm`` hook, and keeps its session's tokens fresh across :meth:`advance`."""

    def address(self, party: str) -> str:
        """The address :meth:`person` signs ``party`` in with."""
        ...

    async def person(self, party: str, *, email: bool = True) -> GateClient:
        """A new sign-in of ``party`` by a person, made now: through a link mailed to
        :meth:`address` (the party is made on its first sign-in) -- or, with ``email=False``,
        a way in that leaves the party no address (GitHub)."""
        ...

    async def key(self, party: str) -> GateClient:
        """A client of a new API key of ``party`` (made by a person's sign-in of it)."""
        ...

    async def worker(self, party: str) -> GateClient:
        """A client whose bearer is ``party``'s Temporal worker token."""
        ...

    async def hire(self, party: str, buyer_key: str) -> HiredJob:
        """A job hired by ``party`` (signed in already by :meth:`person`) with ``buyer_key``
        as its key, from a seller party of the harness's own (one :meth:`person` can sign
        in)."""
        ...

    async def advance(self, seconds: float) -> None:
        """Move the gate's clock forward."""
        ...

    async def mails(self) -> Sequence[Mail]:
        """Every mail the gate has sent, oldest first, once what it queued went out."""
        ...

    async def confirm_by_email(self, client: GateClient) -> None:
        """Confirm it's you for ``client``'s session the way a person does by mail."""
        ...


Scenario = Callable[[JobKeyHarness], Awaitable[None]]


# --------------------------------------------------------------------------- helpers


def _party() -> str:
    return f"jk-{uuid.uuid4().hex[:10]}"


def _fail(rule: str, got: object) -> JobKeyContractViolation:
    return JobKeyContractViolation(f"{rule}: got {got!r}")


def _check(ok: bool, rule: str, got: object = None) -> None:
    if not ok:
        raise _fail(rule, got)


async def _refused(call: Awaitable[object], status: int, code: str | None, rule: str) -> GateError:
    """``call`` must fail with ``status`` (and ``code``, unless None); the error."""
    try:
        got = await call
    except GateError as e:
        ok = e.status == status and (code is None or e.code == code)
        _check(ok, rule, f"{e.status} {e.code}: {e.message}")
        return e
    raise _fail(rule, got)


async def _session_prefix(client: GateClient) -> str:
    current = [s for s in (await client.sessions()).sessions if s.current]
    _check(len(current) == 1, "GET /v1/sessions marks the current sign-in", current)
    return current[0].id[:12]


async def _device(client: GateClient, label: str = "laptop") -> tuple[str, str]:
    secret, public = age.generate_identity()
    await client.add_device(label, public, "cli")
    return secret, public


async def _job(h: JobKeyHarness, party: str) -> tuple[HiredJob, str, str]:
    """A job of ``party``'s, with the key it was hired with: ``(job, secret, public)``."""
    secret, public = age.generate_identity()
    return await h.hire(party, public), secret, public


async def _share(
    client: GateClient,
    job_id: str,
    secret: str,
    devices: Sequence[str],
    *,
    proof: str | None = None,
) -> m.SharedJobKeys:
    """Share ``job_id``'s key with ``devices``, with this session's proof unless given."""
    if proof is None:
        proof = jk.open_challenge((await client.job_key(job_id)).challenge, secret)
    wraps = [m.WrapIn(recipient=d, wrap=jk.wrap_job_key(job_id, secret, d)) for d in devices]
    return await client.share_job_keys([m.JobKeyShare(job_id=job_id, proof=proof, wraps=wraps)])


def _refusal(done: m.SharedJobKeys, job_id: str, code: str, rule: str) -> None:
    _check(
        [(r.job_id, r.code) for r in done.refused] == [(job_id, code)]
        and (done.stored, done.skipped) == (0, 0),
        rule,
        done,
    )


async def _mails_to(h: JobKeyHarness, party: str, subject: str) -> list[Mail]:
    to = h.address(party)
    return [x for x in await h.mails() if x.to == to and x.subject == subject]


# --------------------------------------------------------------------------- devices


async def adding_a_device_needs_a_recent_sign_in(h: JobKeyHarness) -> None:
    """``POST /v1/me/devices`` is ``recent``: an API key 403 ``fresh_login_required``, a worker
    401, a sign-in older than five minutes 403 ``confirmation_required`` (then 201 once
    confirmed). The same live key again is the same view; the cap is the gate's when no
    browser can make room (every device here is a machine); a removed key is never added
    again (409 ``device_revoked``)."""
    party = _party()
    alice = await h.person(party)
    _, laptop = age.generate_identity()
    await _refused(
        (await h.key(party)).add_device("ci", laptop, "cli"),
        403,
        "fresh_login_required",
        "an API key adds no device",
    )
    await _refused(
        (await h.worker(party)).add_device("w", laptop, "cli"), 401, None, "a worker adds none"
    )
    await h.advance(m.RECENT_CONFIRMATION_SECONDS + 1)
    err = await _refused(
        alice.add_device("laptop", laptop, "cli"),
        403,
        m.CONFIRMATION_REQUIRED,
        "a sign-in older than five minutes adds no device",
    )
    m.ConfirmationRequired.model_validate(err.detail)
    await h.confirm_by_email(alice)
    view = await alice.add_device("laptop", laptop, "cli")
    prefix = await _session_prefix(alice)
    _check(
        (view.public_key, view.label, view.kind, view.added_by)
        == (laptop, "laptop", "cli", prefix),
        "the device is listed as added, by this sign-in",
        view,
    )
    again = await alice.add_device("another name", laptop, "browser")
    _check(again == view, "adding a live device again answers the same view", again)
    listed = (await alice.devices()).devices
    _check([d.public_key for d in listed] == [laptop], "GET /v1/me/devices lists it once", listed)
    await _refused(
        alice.add_device("bad", "age1" + "q" * 58, "cli"),
        422,
        "invalid_public_key",
        "a device key is an age X25519 recipient",
    )

    for i in range(m.DEVICES_MAX - 1):
        await _device(alice, f"browser {i}")
    _, extra = age.generate_identity()
    await _refused(
        alice.add_device("one too many", extra, "browser"),
        409,
        "too_many_devices",
        f"{m.DEVICES_MAX} live devices at most",
    )
    await alice.remove_device(laptop)
    await _refused(
        alice.add_device("laptop", laptop, "cli"),
        409,
        "device_revoked",
        "a removed device is never added again",
    )
    await alice.add_device("one more", extra, "browser")  # removing made room


async def removing_a_device_is_a_change_and_ends_its_copies(h: JobKeyHarness) -> None:
    """``DELETE /v1/me/devices/{key}`` is ``fresh``. A removed device is listed nowhere, its
    wraps are gone, a share to it is skipped, ``lacking`` it is 404 ``device_not_found``, and
    so is removing it again."""
    party = _party()
    alice = await h.person(party)
    _, phone = await _device(alice, "phone")
    _, laptop = await _device(alice)
    job, secret, _ = await _job(h, party)
    done = await _share(alice, job.job_id, secret, [phone, laptop])
    _check(done.stored == 2, "both wraps are stored", done)
    await _refused(
        (await h.key(party)).remove_device(phone),
        403,
        "fresh_login_required",
        "an API key removes no device",
    )
    await h.advance(CONFIRM_TTL_SECONDS + 1)
    await _refused(
        alice.remove_device(phone),
        403,
        m.CONFIRMATION_REQUIRED,
        "removing a device is a change: an unconfirmed sign-in is refused",
    )
    await h.confirm_by_email(alice)
    await alice.remove_device(phone)
    listed = [d.public_key for d in (await alice.devices()).devices]
    _check(listed == [laptop], "a removed device is not listed", listed)
    wraps = [w.recipient for w in (await alice.job_key(job.job_id)).wraps]
    _check(wraps == [laptop], "a removed device's wraps are gone", wraps)
    _check(
        (await alice.job_key(job.job_id, for_=phone)).wraps == [],
        "a removed device gets no wrap, even asked for by name",
    )
    again = await _share(alice, job.job_id, secret, [phone])
    _check((again.stored, again.skipped) == (0, 1), "a wrap to a removed device is skipped", again)
    await _refused(
        alice.job_keys(phone), 404, "device_not_found", "lacking a removed device is 404"
    )
    await _refused(alice.remove_device(phone), 404, "device_not_found", "a removed device is gone")


async def devices_are_mailed_to_the_party(h: JobKeyHarness) -> None:
    """Adding a device mails the party's address which device (its label quoted, its own
    quotes escaped) and which sign-in added it; adding it again mails nothing; removing it
    mails which sign-in removed it. No link, nothing secret. No address, no mail."""
    party = _party()
    alice = await h.person(party)
    prefix = await _session_prefix(alice)
    secret, public = age.generate_identity()
    await alice.add_device('Work "laptop"', public, "browser")
    await alice.add_device('Work "laptop"', public, "browser")
    added = await _mails_to(h, party, jk.SUBJECT_DEVICE_ADDED)
    _check(len(added) == 1, "the party is mailed once that a device was added", added)
    text = added[0].text
    _check('"Work \\"laptop\\""' in text, "the notice quotes the label, escaped", text)
    _check(prefix in text, "the notice names the sign-in that added it", text)
    for secret_part in ("http", secret, "AGE-SECRET-KEY"):
        _check(secret_part not in text, "the notice carries no link and no key", secret_part)
    await alice.remove_device(public)
    removed = await _mails_to(h, party, jk.SUBJECT_DEVICE_REMOVED)
    _check(len(removed) == 1, "the party is mailed that a device was removed", removed)
    _check(prefix in removed[0].text, "the notice names the sign-in that removed it", removed)
    _check("http" not in removed[0].text, "the notice carries no link", removed[0].text)

    sent = len(await h.mails())
    github = await h.person(_party(), email=False)
    await _device(github)
    _check(len(await h.mails()) == sent, "a party with no address is mailed nothing")


async def a_full_account_retires_the_browser_used_least_recently(h: JobKeyHarness) -> None:
    """Past ``DEVICES_MAX`` live devices, a new one takes the place of the *browser* used least
    recently (its last ``for=``, else when it was added): retired, not removed -- listed
    nowhere, given nothing, but its copies kept. The mail of the new device names it. The
    same key added again brings it back with its copies, mailing nothing (it was a device
    already), and makes room the same way."""
    party = _party()
    alice = await h.person(party)
    _, old = age.generate_identity()
    await alice.add_device("old browser", old, "browser")
    await h.advance(70)  # a sign-in stays recent throughout
    _, idle = age.generate_identity()
    await alice.add_device("idle browser", idle, "browser")
    await h.advance(70)  # a sign-in stays recent throughout
    await alice.devices(for_=old)  # the older one is the one in use
    job, secret, _ = await _job(h, party)
    _check((await _share(alice, job.job_id, secret, [old, idle])).stored == 2, "both stored")
    for i in range(m.DEVICES_MAX - 2):
        await _device(alice, f"machine {i}")
    added = len(await _mails_to(h, party, jk.SUBJECT_DEVICE_ADDED))
    await h.advance(70)  # a sign-in stays recent throughout
    _, newcomer = age.generate_identity()
    await alice.add_device("newcomer", newcomer, "cli")
    listed = {d.public_key for d in (await alice.devices()).devices}
    _check(
        len(listed) == m.DEVICES_MAX and old in listed and newcomer in listed,
        "a new device past the cap is added",
        len(listed),
    )
    _check(idle not in listed, "the browser used least recently made room", listed)
    mails = await _mails_to(h, party, jk.SUBJECT_DEVICE_ADDED)
    _check(len(mails) == added + 1, "the new device is mailed", mails)
    _check('"idle browser"' in mails[-1].text, "its mail names the browser retired", mails[-1])
    _check(
        [w.recipient for w in (await alice.job_key(job.job_id)).wraps] == [old],
        "a retired device's copies are not served",
    )
    again = await _share(alice, job.job_id, secret, [idle])
    _check((again.stored, again.skipped) == (0, 1), "a wrap to a retired device is skipped", again)
    await _refused(alice.job_keys(idle), 404, "device_not_found", "a retired device lacks nothing")

    await h.advance(70)  # a sign-in stays recent throughout
    back = await alice.add_device("idle browser", idle, "browser")
    _check(back.public_key == idle, "the same key comes back", back)
    listed = {d.public_key for d in (await alice.devices()).devices}
    _check(
        idle in listed and old not in listed and len(listed) == m.DEVICES_MAX,
        "coming back makes room the same way (the browser used least recently now)",
        listed,
    )
    _check(
        len(await _mails_to(h, party, jk.SUBJECT_DEVICE_ADDED)) == added + 1,
        "a device that comes back is not mailed again",
    )
    _check(
        [w.recipient for w in (await alice.job_key(job.job_id)).wraps] == [idle],
        "it comes back with the copies it had",
    )


async def an_idle_browser_retires_and_comes_back(h: JobKeyHarness) -> None:
    """A browser device nobody used for ``DEVICE_IDLE_SECONDS`` is retired (a private window,
    a profile deleted); one in use (``GET /v1/me/devices?for=<it>``) is not, nor is a
    ``kontra login`` machine however idle. A retired device comes back, with its copies,
    when its browser adds the same key again -- a sign-in's ``recent``, no new mail."""
    party = _party()
    alice = await h.person(party)
    _, gone = age.generate_identity()
    await alice.add_device("private window", gone, "browser")
    _, kept = age.generate_identity()
    await alice.add_device("my browser", kept, "browser")
    _, machine = await _device(alice)
    job, secret, _ = await _job(h, party)
    _check((await _share(alice, job.job_id, secret, [gone, kept, machine])).stored == 3, "3")
    step = m.DEVICE_IDLE_SECONDS // 3 + 60
    for _ in range(3):  # a month, in steps a sign-in's tokens outlive
        await h.advance(step)
        await alice.devices(for_=kept)
    listed = [d.public_key for d in (await alice.devices()).devices]
    _check(
        gone not in listed and kept in listed and machine in listed,
        "an idle browser is retired; a browser in use and a machine are not",
        listed,
    )
    wraps = {w.recipient for w in (await alice.job_key(job.job_id)).wraps}
    _check(wraps == {kept, machine}, "a retired device's copies are not served", wraps)
    await _refused(alice.job_keys(gone), 404, "device_not_found", "nor is it listed as lacking")
    await _refused(
        alice.add_device("private window", gone, "browser"),
        403,
        m.CONFIRMATION_REQUIRED,
        "coming back is an addition: recent",
    )
    await h.confirm_by_email(alice)
    sent = len(await _mails_to(h, party, jk.SUBJECT_DEVICE_ADDED))
    await alice.add_device("private window", gone, "browser")
    listed = [d.public_key for d in (await alice.devices()).devices]
    _check(gone in listed, "the same key added again is a device again", listed)
    wraps = {w.recipient for w in (await alice.job_key(job.job_id)).wraps}
    _check(wraps == {gone, kept, machine}, "it comes back with its copies", wraps)
    _check(
        len(await _mails_to(h, party, jk.SUBJECT_DEVICE_ADDED)) == sent,
        "a device coming back is not mailed again",
    )


async def adding_devices_is_rate_limited(h: JobKeyHarness) -> None:
    """At most ``DEVICE_ADDS_PER_HOUR`` new devices an hour (retired ones count: a browser
    added past the cap retires another, so the cap alone limits nothing), else 429
    ``rate_limited`` with ``Retry-After``: each one mails the party."""
    party = _party()
    alice = await h.person(party)
    for i in range(m.DEVICE_ADDS_PER_HOUR):
        await alice.add_device(f"window {i}", age.generate_identity()[1], "browser")
    _check(
        len((await alice.devices()).devices) == m.DEVICES_MAX,
        "browsers past the cap retire one another",
    )
    err = await _refused(
        alice.add_device("one more", age.generate_identity()[1], "browser"),
        429,
        "rate_limited",
        f"{m.DEVICE_ADDS_PER_HOUR} devices an hour at most",
    )
    retry = (err.detail or {}).get("retry_after_seconds")
    _check(isinstance(retry, int) and 0 < retry <= 3600, "the refusal says when", err.detail)
    await h.advance(3601)
    await h.confirm_by_email(alice)
    await alice.add_device("an hour later", age.generate_identity()[1], "browser")


# --------------------------------------------------------------------------- job keys


async def the_job_key_is_the_one_its_hold_seals(h: JobKeyHarness) -> None:
    """``GET /v1/jobs/{id}/key``'s ``buyer_key`` is the hold token's ``buyer_key`` claim (the
    token verified with the gate's JWKS); its challenge opens with that key and no other."""
    party = _party()
    alice = await h.person(party)
    job, secret, public = await _job(h, party)
    key = await alice.job_key(job.job_id)
    jwks = await alice.jwks()
    kid = jwt.get_unverified_header(job.hold_token).get("kid")
    jwk = next((k for k in jwks.get("keys", []) if k.get("kid") == kid), None)
    _check(jwk is not None, "the hold token's signing key is in the JWKS", kid)
    claims: dict[str, Any] = jwt.decode(
        job.hold_token,
        jwt.PyJWK(jwk).key,  # pyright: ignore[reportArgumentType]
        algorithms=["ES256"],
        audience="kontra:hold",
        options={"verify_exp": False, "verify_iat": False, "verify_iss": False},
    )
    _check(
        (key.job_id, key.buyer_key, claims.get("buyer_key")) == (job.job_id, public, public),
        "the job key is the hold's buyer_key",
        (key.buyer_key, claims.get("buyer_key")),
    )
    _check(jk.is_proof(jk.open_challenge(key.challenge, secret)), "the challenge holds a proof")
    try:
        jk.open_challenge(key.challenge, age.generate_identity()[0])
    except jk.InvalidWrap:
        pass
    else:
        raise _fail("the challenge opens with the job's key only", "it opened")

    # JobView.buyer_key: the same key, on the job itself -- only when asked, and otherwise
    # absent from the JSON (never null): 0.2.0-0.2.3 parse every answer strictly.
    raw = await alice._object("job", path={"job_id": job.job_id})  # pyright: ignore[reportPrivateUsage]
    _check("buyer_key" not in raw, "a job answers no buyer_key unless asked", sorted(raw))
    m.JobView.model_validate(raw)  # what an older client does: no unknown field
    view = await alice.job(job.job_id, include_buyer_key=True)
    _check(view.buyer_key == public, "?include=buyer_key adds the job's own key", view.buyer_key)
    listed = await alice.jobs(include_buyer_key=True)
    _check(
        [(v.job_id, v.buyer_key) for v in listed.items if v.job_id == job.job_id]
        == [(job.job_id, public)],
        "the job list adds it too when asked",
        listed,
    )
    plain = await alice.jobs()
    _check(all(v.buyer_key is None for v in plain.items), "the job list adds it only when asked")


async def only_the_buyer_reads_or_shares_a_jobs_key(h: JobKeyHarness) -> None:
    """A job's key is the buyer's: its seller 403 ``forbidden``, anyone else 404
    ``job_not_found`` -- and a share of a job the caller did not hire is refused
    ``job_not_found``, for its seller too."""
    party = _party()
    alice = await h.person(party)
    job, _, _ = await _job(h, party)
    seller = await h.person(job.seller)
    await _refused(seller.job_key(job.job_id), 403, "forbidden", "the seller reads no job key")
    stranger = await h.person(_party())
    await _refused(stranger.job_key(job.job_id), 404, "job_not_found", "a stranger learns nothing")
    junk, _ = age.generate_identity()
    for who, client in (("a stranger", stranger), ("the seller", seller)):
        _, theirs = await _device(client)
        wrap = jk.wrap_job_key(job.job_id, junk, theirs)
        share = m.JobKeyShare(
            job_id=job.job_id, proof="p" * 43, wraps=[m.WrapIn(recipient=theirs, wrap=wrap)]
        )
        done = await client.share_job_keys([share])
        _refusal(done, job.job_id, "job_not_found", f"{who} shares no key of another's job")
    _check((await alice.job_key(job.job_id)).wraps == [], "nothing was stored for those shares")


async def a_share_needs_this_sessions_proof_of_the_job_key(h: JobKeyHarness) -> None:
    """Only a holder of the job's key writes a wrap: the share's proof must be the job's
    challenge opened, for this session, made in this ten-minute bucket or the one before.
    Anything else is refused ``bad_proof`` and stores nothing."""
    party = _party()
    alice = await h.person(party)
    _, laptop = await _device(alice)
    job, secret, _ = await _job(h, party)
    other, other_secret, _ = await _job(h, party)

    done = await _share(alice, job.job_id, secret, [laptop], proof="A" * 43)
    _refusal(done, job.job_id, "bad_proof", "a proof must be the challenge's")
    bob = await h.person(party)  # the same party, another sign-in
    theirs = jk.open_challenge((await bob.job_key(job.job_id)).challenge, secret)
    done = await _share(alice, job.job_id, secret, [laptop], proof=theirs)
    _refusal(done, job.job_id, "bad_proof", "another sign-in's proof is not this one's")
    elsewhere = jk.open_challenge((await alice.job_key(other.job_id)).challenge, other_secret)
    done = await _share(alice, job.job_id, secret, [laptop], proof=elsewhere)
    _refusal(done, job.job_id, "bad_proof", "another job's proof is not this job's")

    stale = jk.open_challenge((await alice.job_key(job.job_id)).challenge, secret)
    await h.advance(2 * jk.BUCKET_SECONDS)
    done = await _share(alice, job.job_id, secret, [laptop], proof=stale)
    _refusal(done, job.job_id, "bad_proof", "a proof is good for twenty minutes at most")
    _check((await alice.job_key(job.job_id)).wraps == [], "no refused share stored anything")

    recent = jk.open_challenge((await alice.job_key(job.job_id)).challenge, secret)
    await h.advance(jk.BUCKET_SECONDS)
    done = await _share(alice, job.job_id, secret, [laptop], proof=recent)
    _check((done.stored, done.refused) == (1, []), "the previous bucket's proof is good", done)
    bob_done = await _share(bob, other.job_id, other_secret, [laptop])
    _check(bob_done.stored == 1, "each sign-in shares with its own proof", bob_done)


async def the_first_wrap_wins(h: JobKeyHarness) -> None:
    """A wrap is never replaced: a second one for the same job and device is skipped and
    the first is what every reader gets; a wrap to a key that is no live device of the
    caller's is skipped too. ``for`` narrows the wraps to one device and marks it used."""
    party = _party()
    alice = await h.person(party)
    phone_secret, phone = await _device(alice, "phone")
    _, laptop = await _device(alice)
    job, secret, public = await _job(h, party)
    first = await _share(alice, job.job_id, secret, [phone])
    _check((first.stored, first.skipped) == (1, 0), "the first wrap is stored", first)
    [stored] = (await alice.job_key(job.job_id)).wraps
    _, nobody = age.generate_identity()
    second = await _share(alice, job.job_id, secret, [phone, laptop, nobody])
    _check(
        (second.stored, second.skipped) == (1, 2),
        "a second wrap for a device, and a wrap to no device, are skipped",
        second,
    )
    mine = (await alice.job_key(job.job_id, for_=phone)).wraps
    _check(
        [(w.recipient, w.wrap) for w in mine] == [(phone, stored.wrap)],
        "the first wrap stands, and for= lists that device's only",
        mine,
    )
    _check(
        jk.unwrap_job_key(mine[0].wrap, phone_secret, public) == secret,
        "the device opens its wrap to the job's key",
    )
    used = {d.public_key: d.last_used_at for d in (await alice.devices()).devices}
    _check(used[phone] is not None, "for= marks the device used", used)
    _check(used[laptop] is None, "only the device named", used)
    listed = (await alice.devices(for_=laptop)).devices
    _check(
        {d.public_key for d in listed if d.last_used_at is not None} == {phone, laptop},
        "GET /v1/me/devices?for= marks that device used too (a catch-up does)",
        listed,
    )

    stranger = await h.person(_party())
    _, theirs = await _device(stranger)
    third = await _share(alice, job.job_id, secret, [theirs])
    _check(
        (third.stored, third.skipped, third.refused) == (0, 1, []),
        "a wrap to another party's device is skipped, however live it is there",
        third,
    )
    _check(
        theirs not in {w.recipient for w in (await alice.job_key(job.job_id)).wraps},
        "nothing is stored for another party's device",
    )


async def lacking_lists_the_jobs_a_device_has_no_wrap_for(h: JobKeyHarness) -> None:
    """``GET /v1/me/job-keys?lacking=D``: exactly the caller's shareable buyer jobs without a
    wrap to D, by job id, paged by cursor; each with its ``buyer_key``, wraps (``for``: that
    device's only) and a challenge. ``lacking`` must be a live device of the caller's."""
    party = _party()
    alice = await h.person(party)
    _, phone = await _device(alice, "phone")
    _, laptop = await _device(alice)
    jobs = [await _job(h, party) for _ in range(3)]
    middle = sorted(jobs, key=lambda j: j[0].job_id)[1]
    await _share(alice, middle[0].job_id, middle[1], [phone])

    found: list[m.JobKey] = []
    cursor: str | None = None
    for _ in range(10):
        page = await alice.job_keys(phone, cursor=cursor, limit=1)
        _check(len(page.items) <= 1, "a page holds at most limit items", page)
        found += page.items
        cursor = page.next_cursor
        if cursor is None:
            break
    expected = sorted(j[0].job_id for j in jobs if j is not middle)
    _check([k.job_id for k in found] == expected, "lacking lists the jobs without a wrap", found)
    keys = {j[0].job_id: j[2] for j in jobs}
    _check(
        all(k.buyer_key == keys[k.job_id] and k.wraps == [] for k in found),
        "each item names its job's key, and no device has a wrap of it",
        found,
    )
    whole = await alice.job_keys(laptop, for_=phone)
    by_id = {k.job_id: k for k in whole.items}
    _check(sorted(by_id) == sorted(keys), "the other device lacks every job", sorted(by_id))
    _check(
        [w.recipient for w in by_id[middle[0].job_id].wraps] == [phone],
        "for= narrows each item's wraps to that device",
        by_id[middle[0].job_id].wraps,
    )
    _, nobody = age.generate_identity()
    await _refused(
        alice.job_keys(nobody), 404, "device_not_found", "lacking must be one of your devices"
    )
    stranger = await h.person(_party())
    _, theirs = await _device(stranger)
    await _refused(
        alice.job_keys(theirs), 404, "device_not_found", "another party's device is not yours"
    )
    _check((await stranger.job_keys(theirs)).items == [], "a stranger lacks none of alice's")


async def a_job_hired_to_a_recipient_is_never_shared(h: JobKeyHarness) -> None:
    """A job whose ``buyer_key`` is or was one of the buyer's recipients (every job from
    before per-job keys: the party's login key) is never shared: no wraps, not listed, a
    share refused ``not_shareable`` -- that key also reads seller input and older jobs."""
    party = _party()
    alice = await h.person(party)
    _, laptop = await _device(alice)
    login_secret, login_key = age.generate_identity()
    await alice.add_recipient("an older machine", login_key)
    revoked_secret, revoked_key = age.generate_identity()
    await alice.add_recipient("a lost machine", revoked_key)
    await alice.revoke_recipient(revoked_key)
    for secret, public, what in (
        (login_secret, login_key, "a recipient's"),
        (revoked_secret, revoked_key, "a revoked recipient's"),
    ):
        job = await h.hire(party, public)
        done = await _share(alice, job.job_id, secret, [laptop])
        _refusal(done, job.job_id, "not_shareable", f"a job hired to {what} key is not shared")
        key = await alice.job_key(job.job_id)
        _check(
            (key.buyer_key, key.wraps) == (public, []),
            "its key is named, never wrapped",
            key,
        )
        listed = [k.job_id for k in (await alice.job_keys(laptop)).items]
        _check(job.job_id not in listed, "it is never listed as lacking", listed)


async def wraps_are_ciphertext_and_nothing_else(h: JobKeyHarness) -> None:
    """A wrap is an age file with exactly one X25519 stanza: a key file in the clear, two
    stanzas, or another stanza type is refused ``invalid_wrap`` and stores nothing; what is
    stored never holds a secret key in the clear."""
    party = _party()
    alice = await h.person(party)
    _, laptop = await _device(alice)
    _, phone = await _device(alice, "phone")
    job, secret, _ = await _job(h, party)
    text = jk.key_file_text(job.job_id, secret).encode()
    scrypt = (
        b"age-encryption.org/v1\n-> scrypt "
        + b"A" * 22
        + b" 18\n"
        + b"A" * 43
        + b"\n--- "
        + b"A" * 43
        + b"\n"
        + b"\0" * 64
    )
    for bad, what in (
        (text, "a key file in the clear"),
        (age.encrypt(text, [laptop, phone]), "two stanzas"),
        (scrypt, "a passphrase stanza"),
    ):
        proof = jk.open_challenge((await alice.job_key(job.job_id)).challenge, secret)
        share = m.JobKeyShare(
            job_id=job.job_id,
            proof=proof,
            wraps=[m.WrapIn(recipient=laptop, wrap=base64.b64encode(bad).decode())],
        )
        done = await alice.share_job_keys([share])
        _refusal(done, job.job_id, "invalid_wrap", f"{what} is no wrap")
    await _share(alice, job.job_id, secret, [laptop, phone])
    for w in (await alice.job_key(job.job_id)).wraps:
        raw = base64.b64decode(w.wrap)
        _check(raw.startswith(b"age-encryption.org/v1\n-> X25519 "), "a wrap is age", raw[:40])
        _check(b"AGE-SECRET-KEY" not in raw.upper(), "no secret key is stored in the clear")


async def a_program_shares_and_a_worker_gets_nothing(h: JobKeyHarness) -> None:
    """An API key's session lists the devices and shares the keys of the jobs it hires (CI
    wraps to the devices too) with its own proof; a worker token is 401 on every route."""
    party = _party()
    alice = await h.person(party)
    _, laptop = await _device(alice)
    program = await h.key(party)
    listed = (await program.devices()).devices
    _check([d.public_key for d in listed] == [laptop], "a program lists the devices", listed)
    _check(
        [(d.label, d.added_by, d.last_used_at) for d in listed]
        == [(m.HIDDEN_DEVICE_LABEL, None, None)],
        "a program reads the keys, never the names of a person's devices or sign-ins",
        listed,
    )
    job, secret, _ = await _job(h, party)
    done = await _share(program, job.job_id, secret, [laptop])
    _check(done.stored == 1, "a program shares with its own session's proof", done)
    worker = await h.worker(party)
    _, nobody = age.generate_identity()
    calls: dict[str, Callable[[], Awaitable[object]]] = {
        "devices": worker.devices,
        "add_device": lambda: worker.add_device("w", nobody, "cli"),
        "remove_device": lambda: worker.remove_device(laptop),
        "job_key": lambda: worker.job_key(job.job_id),
        "job_keys": lambda: worker.job_keys(laptop),
        "share_job_keys": lambda: worker.share_job_keys([]),
    }
    for name, call in calls.items():
        await _refused(call(), 401, None, f"{name} refuses a worker token")


#: Every scenario, in the order of decisions I25's rules.
SCENARIOS: tuple[Scenario, ...] = (
    adding_a_device_needs_a_recent_sign_in,
    removing_a_device_is_a_change_and_ends_its_copies,
    devices_are_mailed_to_the_party,
    a_full_account_retires_the_browser_used_least_recently,
    an_idle_browser_retires_and_comes_back,
    adding_devices_is_rate_limited,
    the_job_key_is_the_one_its_hold_seals,
    only_the_buyer_reads_or_shares_a_jobs_key,
    a_share_needs_this_sessions_proof_of_the_job_key,
    the_first_wrap_wins,
    lacking_lists_the_jobs_a_device_has_no_wrap_for,
    a_job_hired_to_a_recipient_is_never_shared,
    wraps_are_ciphertext_and_nothing_else,
    a_program_shares_and_a_worker_gets_nothing,
)


async def run_jobkey_contract(harness: JobKeyHarness) -> None:
    """Every scenario, in order, on one gate."""
    for scenario in SCENARIOS:
        await scenario(harness)
