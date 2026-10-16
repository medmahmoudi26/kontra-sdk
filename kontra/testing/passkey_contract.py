"""The passkey contract (decisions I24): the scenarios every gate passes, so a passkey rule
cannot hold on the fake gate and not on the platform (seam ②).

:class:`~kontra.testing.fake_gate.FakeGate` is proven against it in this repository; the
platform runs the same coroutines against its own gate::

    from kontra.testing.passkey_contract import SCENARIOS

    @pytest.mark.parametrize("scenario", SCENARIOS, ids=lambda s: s.__name__)
    async def test_passkeys(scenario, harness):
        await scenario(harness)

A scenario is black-box: it speaks HTTP through :class:`~kontra.gate_client.GateClient`, makes
its answers with :class:`~kontra.testing.SoftAuthenticator`, and reaches the gate otherwise
only through a :class:`PasskeyHarness` -- new sign-ins, an API key, a worker token, the gate's
clock, its mail and its operator. Each uses parties of its own, so scenarios may share a gate.
The gate they need: passkeys on, an RP ID and a console origin that are the harness's, "Confirm
it's you" by email (not a dev gate), ``KONTRA_CONFIRM_TTL`` an hour, a clock the harness can
move. Whether a change is allowed is probed with ``DELETE /v1/keys/<an id nobody has>``: a
``fresh`` row that changes nothing, 404 when the session may change things and 403
``confirmation_required`` when it may not.

What only one gate can show stays in its own tests: the switch that turns passkeys off, the
stored use counts, races, the database's constraints. A failure raises
:class:`PasskeyContractViolation` naming the rule.
"""

from __future__ import annotations

import base64
import uuid
from collections.abc import Awaitable, Callable, Sequence
from datetime import datetime, timedelta
from typing import Any, Protocol

from kontra.gate_client import GateClient, GateError
from kontra.gate_client import models as m
from kontra.testing.passkeys import AuthenticatorRefused, SoftAuthenticator

__all__ = [
    "SCENARIOS",
    "Mail",
    "PasskeyContractViolation",
    "PasskeyHarness",
    "Scenario",
    "run_passkey_contract",
]

SUBJECT_ADDED = "A passkey was added to your Kontra account"
SUBJECT_REVOKED = "A passkey of yours was revoked"
SUBJECT_REMOVED = "A passkey was removed from your Kontra account"
#: Another environment's RP ID: what a passkey made for staging signs for.
OTHER_RP_ID = "staging-app.kontra.run"
PHISHING_ORIGIN = "https://kontra-login.example"
_NO_KEY = "key_" + "0" * 16
_NO_CEREMONY = "wac_" + "0" * 32
_NO_PASSKEY = "pky_" + "0" * 32


class PasskeyContractViolation(AssertionError):
    """A gate broke a passkey rule; the message names it."""


class Mail(Protocol):
    """A mail the gate sent (:class:`kontra.testing.fake_gate.SentMail` is one)."""

    @property
    def to(self) -> str: ...

    @property
    def subject(self) -> str: ...

    @property
    def text(self) -> str: ...


class PasskeyHarness(Protocol):
    """How a scenario reaches the gate besides HTTP. Every client it returns has no
    ``confirm`` hook, and keeps its session's tokens fresh across :meth:`advance`."""

    @property
    def origin(self) -> str:
        """The console's origin: where a person's answers come from."""
        ...

    @property
    def rp_id(self) -> str:
        """The gate's RP ID."""
        ...

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

    async def advance(self, seconds: float) -> None:
        """Move the gate's clock forward."""
        ...

    async def now(self) -> datetime:
        """The gate's clock."""
        ...

    async def mails(self) -> Sequence[Mail]:
        """Every mail the gate has sent, oldest first, once what it queued went out."""
        ...

    async def confirm_by_email(self, client: GateClient) -> None:
        """Confirm it's you for ``client``'s session the way a person does by mail."""
        ...

    async def require_confirmation(self, party: str) -> None:
        """The operator asks every sign-in of ``party`` to confirm again
        (``gate.admin require-confirmation``)."""
        ...


Scenario = Callable[[PasskeyHarness], Awaitable[None]]


# --------------------------------------------------------------------------- helpers


def _party() -> str:
    return f"pk-{uuid.uuid4().hex[:10]}"


def _fail(rule: str, got: object) -> PasskeyContractViolation:
    return PasskeyContractViolation(f"{rule}: got {got!r}")


def _check(ok: bool, rule: str, got: object = None) -> None:
    if not ok:
        raise _fail(rule, got)


def _b64(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _unb64(text: str) -> bytes:
    return base64.urlsafe_b64decode(text + "=" * (-len(text) % 4))


async def _refused(call: Awaitable[object], status: int, code: str, rule: str) -> GateError:
    """``call`` must fail with ``status`` ``code``; the error, for its detail."""
    try:
        got = await call
    except GateError as e:
        _check((e.status, e.code) == (status, code), rule, f"{e.status} {e.code}: {e.message}")
        return e
    raise _fail(rule, got)


async def _may_change(client: GateClient) -> bool:
    """Whether ``client``'s session may make a change now (see the module's docstring)."""
    try:
        await client.revoke_api_key(_NO_KEY)
    except GateError as e:
        if (e.status, e.code) == (404, "not_found"):
            return True
        if (e.status, e.code) == (403, m.CONFIRMATION_REQUIRED):
            return False
        raise
    raise _fail("revoking an API key nobody has is 404", "2xx")


async def _register(
    client: GateClient, auth: SoftAuthenticator, label: str = "laptop"
) -> m.PasskeyView:
    ceremony = await client.passkey_options()
    return await client.add_passkey(ceremony.ceremony_id, label, auth.create(ceremony.options))


async def _confirm(client: GateClient, auth: SoftAuthenticator, **kw: Any) -> m.PasskeyConfirmed:
    ceremony = await client.confirm_passkey_options()
    return await client.confirm_passkey(ceremony.ceremony_id, auth.get(ceremony.options, **kw))


def _authenticator(h: PasskeyHarness, **kw: Any) -> SoftAuthenticator:
    return SoftAuthenticator(origin=h.origin, **kw)


async def _view(client: GateClient, passkey_id: str) -> m.PasskeyView:
    found = [p for p in (await client.passkeys()).passkeys if p.id == passkey_id]
    _check(len(found) == 1, f"GET /v1/passkeys lists {passkey_id} once", found)
    return found[0]


async def _session_prefix(client: GateClient) -> str:
    current = [s for s in (await client.sessions()).sessions if s.current]
    _check(len(current) == 1, "GET /v1/sessions marks the current sign-in", current)
    return current[0].id[:12]


async def _mails_to(h: PasskeyHarness, party: str, subject: str) -> list[Mail]:
    to = h.address(party)
    return [x for x in await h.mails() if x.to == to and x.subject == subject]


# --------------------------------------------------------------------------- adding


async def adding_needs_a_confirmation_of_the_last_five_minutes(h: PasskeyHarness) -> None:
    """``passkey_options`` is ``recent``: a sign-in counts for 300 s, the hour does not, an
    email confirmation starts the 300 s again, and an operator's mark refuses even inside
    them."""
    party = _party()
    alice = await h.person(party)
    await h.advance(m.RECENT_CONFIRMATION_SECONDS - 1)
    await alice.passkey_options()
    await h.advance(1)
    _check(await _may_change(alice), "a sign-in five minutes old still makes changes")
    e = await _refused(
        alice.passkey_options(),
        403,
        m.CONFIRMATION_REQUIRED,
        "adding a passkey needs a confirmation of the last 300 s",
    )
    need = m.ConfirmationRequired.model_validate(e.detail)
    _check(need.confirm_with == "email", "the refusal says how to confirm", need)
    await h.confirm_by_email(alice)
    await alice.passkey_options()

    again = await h.person(party)
    await h.require_confirmation(party)
    await _refused(
        again.passkey_options(),
        403,
        m.CONFIRMATION_REQUIRED,
        "an operator's mark refuses adding a passkey, even from a sign-in a moment old",
    )


async def adding_a_passkey_lists_it_and_mails_the_party(h: PasskeyHarness) -> None:
    """``add_passkey`` answers the view; the list shows it; the party's address is told
    which sign-in added it, and nothing secret. A party with no address is mailed nothing."""
    party = _party()
    alice = await h.person(party)
    auth = _authenticator(h)
    before = await h.now()
    view = await _register(alice, auth, "Test laptop")
    prefix = await _session_prefix(alice)
    _check(view.id.startswith("pky_") and m.is_passkey_id(view.id), "a passkey id", view.id)
    _check(
        (view.label, view.added_by, view.last_used_at, view.revoked_at, view.revoked_by)
        == ("Test laptop", prefix, None, None, None),
        "a new passkey's view",
        view,
    )
    _check((view.backed_up, view.transports) == (False, ["internal"]), "BS and transports", view)
    _check(view.created_at >= before - timedelta(seconds=1), "created now", view.created_at)
    listed = await alice.passkeys()
    _check(listed.rp_id == h.rp_id, "the list names the RP ID", listed.rp_id)
    _check([p.id for p in listed.passkeys] == [view.id], "the list shows it", listed)

    notices = await _mails_to(h, party, SUBJECT_ADDED)
    _check(len(notices) == 1, "the party is mailed that a passkey was added", notices)
    notice = notices[0]
    cid = auth.credentials[0].credential_id
    _check('"Test laptop"' in notice.text, "the notice quotes the label", notice.text)
    _check(prefix in notice.text, "the notice names the sign-in that added it", notice.text)
    for secret in (view.id, _b64(cid), cid.hex(), "http"):
        _check(secret not in notice.text, "the notice carries no id, key or link", secret)

    sent = len(await h.mails())
    github = await h.person(_party(), email=False)
    await _register(github, _authenticator(h))
    _check(len(await h.mails()) == sent, "a party with no address is mailed nothing")


# --------------------------------------------------------------------------- confirming


async def a_passkey_confirms_this_sign_in_and_no_other(h: PasskeyHarness) -> None:
    """The options list the party's live passkeys and ask for user verification; a good
    answer confirms the session that asked, for ``confirm_ttl``, mails nobody, and leaves
    the party's other sign-ins as they were."""
    party = _party()
    alice = await h.person(party)
    other = await h.person(party)
    auth = _authenticator(h)
    view = await _register(alice, auth)
    await h.advance(3601)
    e = await _refused(
        alice.revoke_api_key(_NO_KEY), 403, m.CONFIRMATION_REQUIRED, "an hour later, a change asks"
    )
    ttl = m.ConfirmationRequired.model_validate(e.detail).confirm_ttl_seconds
    sent = len(await h.mails())

    ceremony = await alice.confirm_passkey_options()
    options = ceremony.options
    allowed = options.get("allowCredentials")
    _check(
        isinstance(allowed, list)
        and [d.get("id") for d in allowed if isinstance(d, dict)]
        == [_b64(auth.credentials[0].credential_id)],
        "the options allow the party's live passkeys",
        allowed,
    )
    _check(options.get("userVerification") == "required", "user verification required", options)
    _check(options.get("rpId") == h.rp_id == ceremony.rp_id, "the options' RP ID", options)
    _check(0 < ceremony.expires_in <= 360, "a ceremony lives 6 minutes", ceremony.expires_in)
    before = await h.now()
    done = await alice.confirm_passkey(ceremony.ceremony_id, auth.get(options))
    after = await h.now()
    _check(done.passkey_id == view.id, "the answer names the passkey", done)
    window = timedelta(seconds=ttl)
    _check(
        before + window - timedelta(seconds=1)
        <= done.confirmed_until
        <= after + window + timedelta(seconds=1),
        "confirmed for KONTRA_CONFIRM_TTL from now",
        done.confirmed_until,
    )
    _check(await _may_change(alice), "the confirmed sign-in makes changes")
    _check(not await _may_change(other), "another sign-in of the party is not confirmed")
    _check(len(await h.mails()) == sent, "a passkey confirmation mails nobody")
    used = (await _view(alice, view.id)).last_used_at
    _check(used is not None and used >= before - timedelta(seconds=1), "last_used_at", used)

    await h.advance(60)
    await _confirm(alice, auth)  # the use count moved on: a second confirmation is fine
    again = (await _view(alice, view.id)).last_used_at
    _check(again is not None and used is not None and again > used, "last_used_at moves", again)


async def a_passkey_confirmation_clears_an_operators_mark(h: PasskeyHarness) -> None:
    """A passkey confirmation is a confirmation like any other: later than the mark, it
    lets the session change things again."""
    party = _party()
    alice = await h.person(party)
    auth = _authenticator(h)
    await _register(alice, auth)
    await h.require_confirmation(party)
    _check(not await _may_change(alice), "the operator's mark holds changes")
    await h.advance(1)
    await _confirm(alice, auth)
    _check(await _may_change(alice), "a passkey confirmation after the mark clears it")


async def every_ceremony_is_used_once(h: PasskeyHarness) -> None:
    """A ceremony is the session's, of one purpose, used up by the first answer whatever
    becomes of it, and gone after 6 minutes: every other case is 410 ``ceremony_gone``."""
    party = _party()
    alice = await h.person(party)
    other = await h.person(party)
    auth = _authenticator(h)
    await _register(alice, auth)
    gone = "ceremony_gone"

    first = await alice.confirm_passkey_options()
    answer = auth.get(first.options)
    await alice.confirm_passkey(first.ceremony_id, answer)
    await _refused(alice.confirm_passkey(first.ceremony_id, answer), 410, gone, "a replay")

    second = await alice.confirm_passkey_options()
    await _refused(
        alice.confirm_passkey(second.ceremony_id, answer),
        400,
        "passkey_rejected",
        "an old answer to a new ceremony",
    )
    await _refused(
        alice.confirm_passkey(second.ceremony_id, auth.get(second.options)),
        410,
        gone,
        "a refused answer used its ceremony up",
    )

    register = await alice.passkey_options()
    await _refused(
        alice.add_passkey(
            register.ceremony_id, "no uv", _authenticator(h, uv=False).create(register.options)
        ),
        400,
        "passkey_rejected",
        "a registration without user verification",
    )
    await _refused(
        alice.add_passkey(
            register.ceremony_id, "fixed", _authenticator(h).create(register.options)
        ),
        410,
        gone,
        "a refused registration used its ceremony up",
    )

    mine = await alice.confirm_passkey_options()
    await _refused(
        other.confirm_passkey(mine.ceremony_id, auth.get(mine.options)),
        410,
        gone,
        "another sign-in's ceremony",
    )
    register = await alice.passkey_options()
    await _refused(
        other.add_passkey(register.ceremony_id, "x", _authenticator(h).create(register.options)),
        410,
        gone,
        "another sign-in's registration",
    )
    register = await alice.passkey_options()
    confirm = await alice.confirm_passkey_options()
    await _refused(
        alice.confirm_passkey(register.ceremony_id, auth.get(confirm.options)),
        410,
        gone,
        "a registration ceremony at confirm",
    )
    await _refused(
        alice.add_passkey(confirm.ceremony_id, "x", _authenticator(h).create(register.options)),
        410,
        gone,
        "a confirm ceremony at registration",
    )
    await _refused(
        alice.confirm_passkey(_NO_CEREMONY, auth.get(confirm.options)), 410, gone, "no ceremony"
    )

    late = await alice.confirm_passkey_options()
    await h.advance(360)
    await _refused(
        alice.confirm_passkey(late.ceremony_id, auth.get(late.options)),
        410,
        gone,
        "a ceremony 6 minutes old",
    )


# --------------------------------------------------------------------------- counters and flags


async def a_copy_of_a_passkey_gets_it_revoked(h: PasskeyHarness) -> None:
    """A signed answer whose use count did not move forward is a copy of the authenticator:
    403 ``passkey_cloned``, the passkey revoked (``counter``), every sign-in of the party
    asked to confirm again, the party mailed, nothing confirmed."""
    party = _party()
    alice = await h.person(party)
    other = await h.person(party)
    auth = _authenticator(h)
    view = await _register(alice, auth, "Key")
    await _confirm(alice, auth)
    copy = auth.clone()
    await _confirm(alice, auth)
    await _refused(_confirm(alice, copy), 403, "passkey_cloned", "a copy's old use count")
    revoked = await _view(alice, view.id)
    _check(
        revoked.revoked_by == "counter" and revoked.revoked_at is not None,
        "revoked by the gate",
        revoked,
    )
    _check(not await _may_change(alice), "the sign-in that met the copy must confirm again")
    _check(not await _may_change(other), "every sign-in of the party must confirm again")
    notices = await _mails_to(h, party, SUBJECT_REVOKED)
    _check(len(notices) == 1 and '"Key"' in notices[0].text, "the party is mailed", notices)
    await _refused(
        alice.confirm_passkey_options(), 409, "no_passkeys", "its only passkey is revoked"
    )
    await h.confirm_by_email(alice)
    _check(await _may_change(alice), "a confirmation by email clears the mark")


async def a_use_count_that_falls_to_zero_is_a_copy(h: PasskeyHarness) -> None:
    alice = await h.person(_party())
    auth = _authenticator(h)
    await _register(alice, auth)
    await _confirm(alice, auth)
    auth.counter = False
    await _refused(_confirm(alice, auth), 403, "passkey_cloned", "a count from 1 back to 0")


async def a_synced_passkey_counts_nothing(h: PasskeyHarness) -> None:
    """A passkey whose count is always 0 (synced by a password manager) carries no clone
    signal and confirms every time."""
    alice = await h.person(_party())
    auth = _authenticator(h, counter=False, backup_eligible=True, backed_up=True)
    view = await _register(alice, auth)
    _check(view.backed_up, "BS: a synced passkey", view)
    for _ in range(3):
        await _confirm(alice, auth)


async def a_synced_passkeys_old_count_is_refused_not_revoked(h: PasskeyHarness) -> None:
    """A synced passkey (backup eligible) lives in copies by design -- every device of a
    password manager -- so a use count that did not move forward is no clone signal: that
    answer is 400 ``passkey_rejected`` (the panel offers email), and nothing is revoked,
    nobody asked again, nobody mailed. Once its count moves forward it confirms again."""
    party = _party()
    alice = await h.person(party)
    other = await h.person(party)
    auth = _authenticator(h, backup_eligible=True, backed_up=True)
    view = await _register(alice, auth, "Synced")
    copy = auth.clone()  # the same passkey on another device of the password manager
    await h.advance(3601)
    await _confirm(other, auth)
    await _refused(
        _confirm(alice, copy), 400, "passkey_rejected", "a synced passkey's count that did not move"
    )
    _check((await _view(alice, view.id)).revoked_at is None, "a synced passkey is not revoked")
    _check(await _may_change(other), "no sign-in is asked to confirm again")
    _check(not await _may_change(alice), "the refused answer confirmed nothing")
    _check(await _mails_to(h, party, SUBJECT_REVOKED) == [], "nobody is mailed")
    await _confirm(alice, copy)
    _check(await _may_change(alice), "a count that moved forward confirms")


async def a_backup_eligibility_flip_is_refused_not_revoked(h: PasskeyHarness) -> None:
    alice = await h.person(_party())
    auth = _authenticator(h)
    view = await _register(alice, auth)
    auth.backup_eligible = True
    await _refused(_confirm(alice, auth), 400, "passkey_rejected", "BE differs from registration")
    _check((await _view(alice, view.id)).revoked_at is None, "a BE flip revokes nothing")
    auth.backup_eligible = False
    await _confirm(alice, auth)


# --------------------------------------------------------------------------- what an answer must be


async def a_registration_must_verify_the_user_on_this_console(h: PasskeyHarness) -> None:
    """User verification, the console's origin (in no other site's frame) and this gate's RP
    ID, or 400 and nothing added."""
    alice = await h.person(_party())
    bad = {
        "no user verification": _authenticator(h, uv=False),
        "a phishing page": SoftAuthenticator(origin=PHISHING_ORIGIN),
        "the console framed by a phishing page": _authenticator(h, top_origin=PHISHING_ORIGIN),
        "another environment's RP ID": _authenticator(h, rp_id=OTHER_RP_ID),
    }
    for rule, auth in bad.items():
        await _refused(_register(alice, auth), 400, "passkey_rejected", rule)
    _check((await alice.passkeys()).passkeys == [], "nothing was added")


async def an_assertion_must_verify_the_user_on_this_console(h: PasskeyHarness) -> None:
    """User verification and presence, the console's origin (in no other site's frame), this
    gate's RP ID, a good signature, the party's own user handle and one of its own passkeys,
    or 400 and nothing confirmed."""
    party = _party()
    alice = await h.person(party)
    auth = _authenticator(h)
    await _register(alice, auth)
    bob = await h.person(_party())
    bobs = _authenticator(h)
    await _register(bob, bobs)
    await h.advance(3601)
    rejected = "passkey_rejected"
    auth.uv = False
    await _refused(_confirm(alice, auth), 400, rejected, "no user verification")
    auth.uv, auth.up = True, False
    await _refused(_confirm(alice, auth), 400, rejected, "no user presence")
    auth.up, auth.origin = True, PHISHING_ORIGIN
    await _refused(_confirm(alice, auth), 400, rejected, "a phishing page")
    auth.origin, auth.top_origin = h.origin, PHISHING_ORIGIN
    await _refused(_confirm(alice, auth), 400, rejected, "the console framed by a phishing page")
    auth.top_origin, auth.rp_id = None, OTHER_RP_ID
    await _refused(_confirm(alice, auth), 400, rejected, "another environment's RP ID")
    auth.rp_id = None

    ceremony = await alice.confirm_passkey_options()
    forged = auth.get(ceremony.options)
    signature = bytearray(_unb64(forged["response"]["signature"]))
    signature[-1] ^= 1
    forged["response"]["signature"] = _b64(bytes(signature))
    await _refused(
        alice.confirm_passkey(ceremony.ceremony_id, forged), 400, "passkey_rejected", "signature"
    )

    ceremony = await alice.confirm_passkey_options()
    stranger = auth.get(ceremony.options)
    stranger["response"]["userHandle"] = _b64(b"\x01" * 32)
    await _refused(
        alice.confirm_passkey(ceremony.ceremony_id, stranger),
        400,
        "passkey_rejected",
        "another account's user handle",
    )

    ceremony = await alice.confirm_passkey_options()
    theirs = bobs.get(ceremony.options, credential_id=bobs.credentials[0].credential_id)
    await _refused(
        alice.confirm_passkey(ceremony.ceremony_id, theirs),
        400,
        "passkey_rejected",
        "another party's passkey",
    )
    _check(not await _may_change(alice), "no refused answer confirmed anything")
    await _confirm(alice, auth)
    _check(await _may_change(alice), "the good answer confirms")


# --------------------------------------------------------------------------- limits


async def ceremonies_are_rate_limited(h: PasskeyHarness) -> None:
    """20 ceremonies of a purpose per sign-in an hour; the 21st is 429 with when to retry."""
    alice = await h.person(_party())
    for _ in range(20):
        await alice.passkey_options()
    e = await _refused(alice.passkey_options(), 429, "rate_limited", "the 21st ceremony")
    retry = (e.detail or {}).get("retry_after_seconds")
    _check(isinstance(retry, int) and 0 < retry <= 3600, "429 says when to retry", e.detail)
    await h.advance(3600)
    await h.confirm_by_email(alice)
    await alice.passkey_options()


async def refused_answers_stop_new_ceremonies(h: PasskeyHarness) -> None:
    """Ten refused answers of a sign-in in an hour: no new ceremony of either purpose."""
    alice = await h.person(_party())
    auth = _authenticator(h)
    await _register(alice, auth)
    auth.uv = False
    for _ in range(10):
        await _refused(_confirm(alice, auth), 400, "passkey_rejected", "an answer without UV")
    await _refused(alice.confirm_passkey_options(), 429, "rate_limited", "10 refused answers")
    await _refused(alice.passkey_options(), 429, "rate_limited", "10 refused answers, adding")


async def ten_live_passkeys_at_most(h: PasskeyHarness) -> None:
    alice = await h.person(_party())
    views = [await _register(alice, _authenticator(h), f"key {i}") for i in range(10)]
    e = await _refused(alice.passkey_options(), 409, "too_many_passkeys", "the 11th live passkey")
    _check(e.detail == {"max_passkeys": 10}, "too_many_passkeys says the limit", e.detail)
    await alice.revoke_passkey(views[0].id)
    await _register(alice, _authenticator(h), "key 10")


async def a_credential_is_registered_once(h: PasskeyHarness) -> None:
    """A credential id is the gate's once, revoked or not, whoever presents it again."""
    alice = await h.person(_party())
    auth = _authenticator(h)
    view = await _register(alice, auth)
    cid = auth.credentials[0].credential_id

    async def again(client: GateClient) -> m.PasskeyView:
        ceremony = await client.passkey_options()
        answer = _authenticator(h).create(ceremony.options, credential_id=cid)
        return await client.add_passkey(ceremony.ceremony_id, "again", answer)

    await _refused(again(alice), 409, "passkey_exists", "the party's own id again")
    await _refused(again(await h.person(_party())), 409, "passkey_exists", "another party's id")
    await alice.revoke_passkey(view.id)
    await _refused(again(alice), 409, "passkey_exists", "a revoked passkey's id")


# --------------------------------------------------------------------------- revoking


async def revoking_a_passkey_marks_the_other_sign_ins(h: PasskeyHarness) -> None:
    """Revoking is a change, idempotent, the party's own only; the party's other sign-ins
    must confirm again, the revoking one need not; the passkey confirms nothing more."""
    party = _party()
    alice = await h.person(party)
    other = await h.person(party)
    auth, spare = _authenticator(h), _authenticator(h)
    view = await _register(alice, auth, "old")
    kept = await _register(alice, spare, "kept")
    bob = await h.person(_party())
    bobs = await _register(bob, _authenticator(h))

    await alice.revoke_passkey(view.id)
    await alice.revoke_passkey(view.id)
    for missing, rule in (
        (bobs.id, "another party's passkey"),
        (_NO_PASSKEY, "an id nobody has"),
        ("pky_not-an-id", "a malformed id"),
    ):
        await _refused(alice.revoke_passkey(missing), 404, "not_found", rule)
    _check(await _may_change(alice), "the revoking sign-in is not asked again")
    _check(not await _may_change(other), "the party's other sign-ins are")
    listed = (await alice.passkeys()).passkeys
    _check(
        [(p.id, p.revoked_by) for p in listed] == [(kept.id, None), (view.id, "revoked")],
        "live passkeys first, then the revoked",
        listed,
    )
    ceremony = await alice.confirm_passkey_options()
    try:
        auth.get(ceremony.options)
    except AuthenticatorRefused as e:
        _check(e.reason == "no_credential", "the options no longer allow it", e.reason)
    else:
        raise _fail("the options no longer allow a revoked passkey", ceremony.options)
    answer = auth.get(ceremony.options, credential_id=auth.credentials[0].credential_id)
    await _refused(
        alice.confirm_passkey(ceremony.ceremony_id, answer),
        400,
        "passkey_rejected",
        "a revoked passkey's answer",
    )


async def revoking_a_passkey_mails_the_party(h: PasskeyHarness) -> None:
    """Every revocation is mailed to the party's address: which passkey (its label quoted,
    its own quotes escaped), and which sign-in removed it, with what to do if it was not
    them -- no link, nothing secret. Revoking a revoked one mails nothing; a party with no
    address is mailed nothing."""
    party = _party()
    alice = await h.person(party)
    auth = _authenticator(h)
    view = await _register(alice, auth, 'Work "laptop"')
    prefix = await _session_prefix(alice)
    await alice.revoke_passkey(view.id)
    await alice.revoke_passkey(view.id)
    notices = await _mails_to(h, party, SUBJECT_REMOVED)
    _check(len(notices) == 1, "the party is mailed once that a passkey was removed", notices)
    text = notices[0].text
    _check('"Work \\"laptop\\""' in text, "the notice quotes the label, escaped", text)
    _check(prefix in text, "the notice names the sign-in that removed it", text)
    _check("sign out" in text, "the notice says what to do if it was not them", text)
    cid = auth.credentials[0].credential_id
    for secret in (view.id, _b64(cid), cid.hex(), "http"):
        _check(secret not in text, "the notice carries no id, key or link", secret)

    sent = len(await h.mails())
    github = await h.person(_party(), email=False)
    theirs = await _register(github, _authenticator(h))
    await github.revoke_passkey(theirs.id)
    _check(len(await h.mails()) == sent, "a party with no address is mailed nothing")


# --------------------------------------------------------------------------- who may ask


async def an_api_key_or_a_worker_gets_nothing(h: PasskeyHarness) -> None:
    """Every passkey route is a person's: an API key's session 403 ``fresh_login_required``,
    a worker token 401."""
    party = _party()
    clients = (
        ("an API key", await h.key(party), 403, "fresh_login_required"),
        ("a worker token", await h.worker(party), 401, None),
    )
    for who, client, status, code in clients:
        calls: dict[str, Callable[[], Awaitable[object]]] = {
            "passkeys": client.passkeys,
            "passkey_options": client.passkey_options,
            "add_passkey": lambda c=client: c.add_passkey(_NO_CEREMONY, "x", {"id": "x"}),
            "revoke_passkey": lambda c=client: c.revoke_passkey(_NO_PASSKEY),
            "confirm_passkey_options": client.confirm_passkey_options,
            "confirm_passkey": lambda c=client: c.confirm_passkey(_NO_CEREMONY, {"id": "x"}),
        }
        for name, call in calls.items():
            try:
                got = await call()
            except GateError as e:
                ok = e.status == status and (code is None or e.code == code)
                _check(ok, f"{name} refuses {who}", f"{e.status} {e.code}")
            else:
                raise _fail(f"{name} refuses {who}", got)


#: Every scenario, in the order of decisions I24's rules.
SCENARIOS: tuple[Scenario, ...] = (
    adding_needs_a_confirmation_of_the_last_five_minutes,
    adding_a_passkey_lists_it_and_mails_the_party,
    a_passkey_confirms_this_sign_in_and_no_other,
    a_passkey_confirmation_clears_an_operators_mark,
    every_ceremony_is_used_once,
    a_copy_of_a_passkey_gets_it_revoked,
    a_use_count_that_falls_to_zero_is_a_copy,
    a_synced_passkey_counts_nothing,
    a_synced_passkeys_old_count_is_refused_not_revoked,
    a_backup_eligibility_flip_is_refused_not_revoked,
    a_registration_must_verify_the_user_on_this_console,
    an_assertion_must_verify_the_user_on_this_console,
    ceremonies_are_rate_limited,
    refused_answers_stop_new_ceremonies,
    ten_live_passkeys_at_most,
    a_credential_is_registered_once,
    revoking_a_passkey_marks_the_other_sign_ins,
    revoking_a_passkey_mails_the_party,
    an_api_key_or_a_worker_gets_nothing,
)


async def run_passkey_contract(harness: PasskeyHarness) -> None:
    """Every scenario, in order, on one gate."""
    for scenario in SCENARIOS:
        await scenario(harness)
