"""This machine as one of the account's devices (decisions I25).

A ``kontra login`` machine's device key is its ``~/.kontra/key`` (also one of the party's
recipients: two rows, two revocations). This module does what the CLI, :mod:`kontra.hire` and
``kontra mcp`` need of it, over :class:`~kontra.gate_client.GateClient` and the files under
``$KONTRA_HOME``:

* register the key as a device (:func:`register_device`; ``kontra login`` does it right after
  the sign-in, so nobody is asked anything);
* share the key of a job hired here with every live device (:func:`share_job_key`), each
  wrap with the proof that this machine holds the key;
* read a job hired elsewhere (:func:`fetch_job_key`): unwrap this device's copy, check it
  against the job's ``buyer_key``, write ``jobs/<id>.key`` 0600 -- no export, no import;
* catch up (:func:`sync_job_keys`): serve every device this machine has not served yet from
  the job keys it holds, and retry the shares that failed (``jobs/<id>.unshared`` markers).

Local state is ``$KONTRA_HOME/devices.json``, per gate and party: the devices this machine has
shown the person (*seen*) and served (*synced*), and whether this machine is a device
(``added``, ``declined`` by ``kontra login --no-device``, ``revoked``). Nothing here ever fails
the command it runs in: a gate that predates sharing (404 on these routes) is skipped quietly.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import re
import socket
import tempfile
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, field
from datetime import UTC
from pathlib import Path
from typing import Literal

import httpx

from kontra import _jobkeys as jk
from kontra.codec import age
from kontra.gate_client import GateClient, GateError
from kontra.gate_client import models as m
from kontra.profile import JOBS_DIR, Profile, ProfileError, kontra_home

__all__ = [
    "PAGE_LIMIT",
    "READ_BUDGET",
    "STATE_FILE",
    "SYNC_BUDGET",
    "DeviceOutcome",
    "DeviceState",
    "Fetched",
    "Notify",
    "ShareRefused",
    "SyncReport",
    "clear_unshared",
    "decline_device",
    "device_label",
    "fetch_job_key",
    "local_job_keys",
    "mark_shown",
    "mark_unshared",
    "notice_new_devices",
    "notice_text",
    "register_device",
    "register_if_undecided",
    "resolve_home",
    "share_job_key",
    "shown",
    "sync_job_keys",
    "unshared_jobs",
    "unsupported",
]

log = logging.getLogger("kontra.devices")

STATE_FILE = "devices.json"
#: A job whose key a hire here could not share yet: ``jobs/<job_id>.unshared`` (empty).
UNSHARED_SUFFIX = ".unshared"
#: How long a command spends catching up before it gives up for this time: a hire's or a
#: login's (:data:`SYNC_BUDGET`), and a read's (``kontra jobs``, ``kontra job``), which runs
#: after the command printed what was asked. A catch-up that stops keeps how far it got.
SYNC_BUDGET = 5.0
READ_BUDGET = 1.5
#: Jobs one listing page asks for (``GET /v1/me/job-keys?limit=``).
PAGE_LIMIT = 100
_JOB_ID = re.compile(r"[0-9a-f]{32}", re.ASCII)
_LABEL = re.compile(r"[\x20-\x7e]{1,64}", re.ASCII)
#: A listing cursor kept between catch-ups: opaque, but no more than a short token.
_CURSOR = re.compile(r"[\x21-\x7e]{1,256}", re.ASCII)

#: What registering this machine came to: ``added`` (or already a device), ``unsupported``
#: (a gate without devices), ``revoked`` (this key was removed as a device: never again),
#: ``full`` (the account holds the most devices it may), ``refused`` (no recent sign-in or
#: confirmation, or an API key), ``no_key`` (this machine has no key file).
DeviceOutcome = Literal["added", "unsupported", "revoked", "full", "refused", "no_key"]


class ShareRefused(Exception):
    """The gate refused a job of a share (``job_not_found``, ``not_shareable``,
    ``bad_proof``, ``invalid_wrap``)."""

    def __init__(self, job_id: str, code: str) -> None:
        self.job_id, self.code = job_id, code
        super().__init__(f"job {job_id}: {code}")


def unsupported(err: GateError) -> bool:
    """The gate predates sharing: a 404 that no route of it answered."""
    return err.status in (404, 405) and err.code in (
        "not_found",
        "method_not_allowed",
        "http_error",
    )


# --------------------------------------------------------------------------- local state


@dataclass
class DeviceState:
    """What this machine remembers of one account's devices (``devices.json``): the devices
    shown (*seen*) and served to the end (*synced*), where a catch-up of a device stopped
    (*cursors*: the listing's cursor to resume from), and what this machine is."""

    seen: set[str] = field(default_factory=set[str])
    synced: set[str] = field(default_factory=set[str])
    device: Literal["added", "declined", "revoked"] | None = None
    cursors: dict[str, str] = field(default_factory=dict[str, str])

    @staticmethod
    def _key(gate_url: str, party: str) -> str:
        return f"{gate_url.rstrip('/')} {party}"

    @classmethod
    def load(cls, home: Path, gate_url: str, party: str) -> DeviceState:
        try:
            raw: object = json.loads((home / STATE_FILE).read_text("utf-8"))
        except (OSError, ValueError):
            return cls()
        entry = raw.get(cls._key(gate_url, party)) if isinstance(raw, dict) else None
        if not isinstance(entry, dict):
            return cls()

        def keys(name: str) -> set[str]:
            found: object = entry.get(name)  # pyright: ignore[reportUnknownMemberType]
            if not isinstance(found, list):
                return set()
            return {k for k in found if isinstance(k, str) and age.is_recipient(k)}  # pyright: ignore[reportUnknownVariableType]

        device: object = entry.get("device")  # pyright: ignore[reportUnknownMemberType]
        found: object = entry.get("cursors")  # pyright: ignore[reportUnknownMemberType]
        cursors: dict[str, str] = {}
        if isinstance(found, dict):
            for k, v in found.items():  # pyright: ignore[reportUnknownVariableType]
                if (
                    isinstance(k, str)
                    and age.is_recipient(k)
                    and isinstance(v, str)
                    and _CURSOR.fullmatch(v)
                ):
                    cursors[k] = v
        return cls(
            seen=keys("seen"),
            synced=keys("synced"),
            device=device if device in ("added", "declined", "revoked") else None,  # pyright: ignore[reportArgumentType]
            cursors=cursors,
        )

    def save(self, home: Path, gate_url: str, party: str) -> None:
        """Atomically, 0600; best effort (a read-only home only loses what was learnt)."""
        path = home / STATE_FILE
        try:
            raw: object = json.loads(path.read_text("utf-8"))
        except (OSError, ValueError):
            raw = {}
        data: dict[str, object] = {}
        if isinstance(raw, dict):
            data = {str(k): v for k, v in raw.items()}  # pyright: ignore[reportUnknownVariableType, reportUnknownArgumentType]
        data[self._key(gate_url, party)] = {
            "seen": sorted(self.seen),
            "synced": sorted(self.synced),
            "device": self.device,
            "cursors": dict(sorted(self.cursors.items())),
        }
        try:
            home.mkdir(mode=0o700, parents=True, exist_ok=True)
            fd, tmp = tempfile.mkstemp(dir=home, prefix=f".{STATE_FILE}-")
            try:
                os.chmod(tmp, 0o600)
                with os.fdopen(fd, "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=1, sort_keys=True)
                os.replace(tmp, path)
            finally:
                with contextlib.suppress(FileNotFoundError):
                    os.unlink(tmp)
        except OSError as err:
            log.info("could not write %s: %s", path, err)


def resolve_home(home: Path | str | None) -> Path:
    """``home`` as a directory path, else ``$KONTRA_HOME`` (``~/.kontra``)."""
    return Path(home).expanduser() if home is not None else kontra_home()


def _jobs_dir(home: Path) -> Path:
    return home / JOBS_DIR


def mark_unshared(job_id: str, home: Path) -> None:
    """Remember that ``job_id``'s key may not have reached every device yet."""
    if not _JOB_ID.fullmatch(job_id):
        return
    with contextlib.suppress(OSError):
        directory = _jobs_dir(home)
        directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        (directory / f"{job_id}{UNSHARED_SUFFIX}").touch(mode=0o600)


def clear_unshared(job_id: str, home: Path) -> None:
    if _JOB_ID.fullmatch(job_id):
        with contextlib.suppress(OSError):
            (_jobs_dir(home) / f"{job_id}{UNSHARED_SUFFIX}").unlink(missing_ok=True)


def unshared_jobs(home: Path) -> list[str]:
    directory = _jobs_dir(home)
    if not directory.is_dir():
        return []
    found = (p.name.removesuffix(UNSHARED_SUFFIX) for p in directory.glob(f"*{UNSHARED_SUFFIX}"))
    return sorted(j for j in found if _JOB_ID.fullmatch(j))


def local_job_keys(home: Path) -> dict[str, str]:
    """Every job key on this machine, by its public key (``jobs/*.key``). Never the machine's
    own key: that one also reads seller input and every job from before per-job keys."""
    out: dict[str, str] = {}
    for secret in Profile.job_identities(home):
        with contextlib.suppress(age.AgeError):
            out[age.public_key_of(secret)] = secret
    return out


# --------------------------------------------------------------------------- this machine


def device_label() -> str:
    """This machine's name for its device row: the hostname, as printable ASCII of at most
    64 characters (a device label's rule), else ``this machine``. Shown, never parsed."""
    raw = ""
    with contextlib.suppress(OSError):
        raw = socket.gethostname()
    text = "".join(c for c in raw if " " <= c <= "~").strip()[:64].rstrip()
    return text if _LABEL.fullmatch(text) else "this machine"


async def register_device(
    gate: GateClient,
    public_key: str,
    *,
    home: Path,
    gate_url: str,
    party: str,
    label: str | None = None,
    quiet: bool = True,
) -> DeviceOutcome:
    """Add ``public_key`` (this machine's key) as one of the party's devices, and remember
    the outcome. ``quiet``: a 403 is an outcome, never a confirmation asked of anyone."""
    state = DeviceState.load(home, gate_url, party)
    try:
        await gate.add_device(label or device_label(), public_key, "cli", quiet=quiet)
    except GateError as err:
        if unsupported(err):
            return "unsupported"
        if err.code == "device_revoked":
            state.device = "revoked"
            state.save(home, gate_url, party)
            return "revoked"
        if err.code == "too_many_devices":
            return "full"
        if err.status in (401, 403):
            return "refused"
        raise
    state.device = "added"
    state.seen.add(public_key)
    state.save(home, gate_url, party)
    return "added"


def decline_device(home: Path, gate_url: str, party: str) -> None:
    """``kontra login --no-device``: this machine is not to read the jobs the party hires,
    and later confirmations do not register it either."""
    state = DeviceState.load(home, gate_url, party)
    state.device = "declined"
    state.save(home, gate_url, party)


async def register_if_undecided(
    gate: GateClient, *, home: Path, gate_url: str, party: str
) -> DeviceOutcome | None:
    """Right after a confirmation: register this machine if it never was and was never
    declined (a login from before devices existed). Quiet, and never raises."""
    if DeviceState.load(home, gate_url, party).device is not None:
        return None
    try:
        public = age.public_key_of(Profile.device_identity(home))
    except (ProfileError, age.AgeError):
        return None
    try:
        return await register_device(gate, public, home=home, gate_url=gate_url, party=party)
    except Exception as err:  # never fail the change the person just confirmed
        log.info("could not add this machine as a device: %s", err)
        return None


# --------------------------------------------------------------------------- sharing


async def share_job_key(
    gate: GateClient,
    job_id: str,
    secret: str,
    *,
    devices: Sequence[m.DeviceView] | None = None,
) -> int:
    """Wrap ``job_id``'s key ``secret`` to every live device that has no wrap of it yet, with
    the proof; how many wraps the gate stored. :class:`ShareRefused` when the gate refuses
    the job; :class:`kontra._jobkeys.JobKeyMismatch` when the gate names another key for it
    (nothing is sent then)."""
    if devices is None:
        devices = (await gate.devices()).devices
    targets = [d.public_key for d in devices]
    if not targets:
        return 0
    key = await gate.job_key(job_id)
    if key.buyer_key != age.public_key_of(secret):
        raise jk.JobKeyMismatch(f"the gate names another key for job {job_id}")
    have = {w.recipient for w in key.wraps}
    wraps = [
        m.WrapIn(recipient=r, wrap=jk.wrap_job_key(job_id, secret, r))
        for r in dict.fromkeys(targets)
        if r not in have
    ]
    if not wraps:
        return 0
    proof = jk.open_challenge(key.challenge, secret)
    done = await gate.share_job_keys([m.JobKeyShare(job_id=job_id, proof=proof, wraps=wraps)])
    if done.refused:
        raise ShareRefused(job_id, done.refused[0].code)
    return done.stored


@dataclass(frozen=True, slots=True)
class Fetched:
    """What :func:`fetch_job_key` found: the job's key (``secret``), or why there is none
    here (``reason``: ``no_wrap``, ``not_a_device``, ``older_key``, ``no_key_file``), or
    neither (the gate cannot say: an older gate, a seller's job, the gate out of reach)."""

    secret: str | None = None
    reason: str | None = None
    buyer_key: str | None = None


async def fetch_job_key(
    gate: GateClient,
    job_id: str,
    *,
    home: Path,
    identity: str | None,
    held: Iterable[str] = (),
) -> Fetched:
    """This machine's copy of ``job_id``'s key, from the gate: the wrap to this device,
    opened and checked against the job's own ``buyer_key``, then written to
    ``jobs/<id>.key`` (0600, never over a key already there). ``held``: the job keys
    already in memory, checked first. ``identity``: this machine's key, None without a key
    file (an API key's CI box)."""
    mine = age.public_key_of(identity) if identity is not None else None
    try:
        key = await gate.job_key(job_id, for_=mine)
    except GateError as err:
        log.debug("job %s: no job key from the gate (%s)", job_id, err.code)
        return Fetched()
    except httpx.TransportError:
        return Fetched()
    if key.buyer_key == mine:
        return Fetched(secret=identity, buyer_key=key.buyer_key)
    for secret in held:
        with contextlib.suppress(age.AgeError):
            if age.public_key_of(secret) == key.buyer_key:
                return Fetched(secret=secret, buyer_key=key.buyer_key)
    if identity is None or mine is None:
        return Fetched(reason="no_key_file", buyer_key=key.buyer_key)
    for wrap in key.wraps:
        if wrap.recipient != mine:
            continue
        try:
            secret = jk.unwrap_job_key(wrap.wrap, identity, key.buyer_key)
        except (jk.InvalidWrap, jk.JobKeyMismatch) as err:
            log.warning("job %s: a copy of its key for this device is unusable (%s)", job_id, err)
            continue
        with contextlib.suppress(OSError, ValueError, ProfileError):
            Profile.save_job_identity(job_id, secret, home)
        return Fetched(secret=secret, buyer_key=key.buyer_key)
    reason = "no_wrap"
    with contextlib.suppress(GateError, httpx.TransportError):
        if key.buyer_key in {k.public_key for k in (await gate.me()).keys}:
            reason = "older_key"
        elif mine not in {d.public_key for d in (await gate.devices()).devices}:
            reason = "not_a_device"
    return Fetched(reason=reason, buyer_key=key.buyer_key)


# --------------------------------------------------------------------------- catching up


def notice_text(device: m.DeviceView) -> str:
    """What this machine says, once, of a device it sees for the first time."""
    kind = jk.KIND_WORDS.get(device.kind, device.kind)
    by = f" by sign-in {device.added_by}" if device.added_by else ""
    return (
        f'"{device.label}" ({kind}) can now read your jobs (added '
        f"{device.added_at.astimezone(UTC):%Y-%m-%d %H:%M} UTC{by}). Not you? `kontra devices remove "
        f"{device.public_key}`, and sign that sign-in out in the console."
    )


#: Told of a device this machine sees for the first time, before anything is copied to it.
#: It returns False when it could not show the device to the person yet (``kontra mcp``
#: shows it in its next answer): nothing is copied to it from here until it is shown
#: (:func:`mark_shown`). Anything else counts as shown.
Notify = Callable[[m.DeviceView], bool | None]


def notice_new_devices(
    listed: Sequence[m.DeviceView],
    *,
    home: Path,
    gate_url: str,
    party: str,
    mine: str | None,
    notify: Notify | None = None,
) -> list[m.DeviceView]:
    """Say, once, of each listed device this machine has not shown yet (not itself) that it
    can now read the jobs -- before anything is copied to it -- and remember it shown. The
    devices said (``notify``, default: a warning in the log); one ``notify`` could not show
    yet (False) is neither remembered nor served, and is said again next time."""
    state = DeviceState.load(home, gate_url, party)
    new = [d for d in listed if d.public_key not in state.seen]
    said = [d for d in new if d.public_key != mine]
    held = {d.public_key for d in said if (notify or _notify_log)(d) is False}
    shown_now = {d.public_key for d in new} - held
    if shown_now:
        state.seen |= shown_now
        state.save(home, gate_url, party)
    return said


def shown(
    listed: Sequence[m.DeviceView], *, home: Path, gate_url: str, party: str, mine: str | None
) -> list[m.DeviceView]:
    """The listed devices this machine may copy keys to: itself, and those it has shown the
    person (D5 of decisions I25: a device the gate lists behind the person's back is shown
    by every device that would serve it, before it is served)."""
    seen = DeviceState.load(home, gate_url, party).seen
    return [d for d in listed if d.public_key == mine or d.public_key in seen]


def mark_shown(keys: Iterable[str], *, home: Path, gate_url: str, party: str) -> None:
    """The person was shown these devices (a notice held back, shown since)."""
    state = DeviceState.load(home, gate_url, party)
    fresh = {k for k in keys if age.is_recipient(k)} - state.seen
    if fresh:
        state.seen |= fresh
        state.save(home, gate_url, party)


@dataclass
class SyncReport:
    """What one catch-up did: wraps ``shared``, devices ``served`` completely, devices seen
    for the first time (``notices``), whether it ran out of time (``complete``), and whether
    the gate predates sharing (``unsupported``)."""

    shared: int = 0
    served: list[str] = field(default_factory=list[str])
    notices: list[m.DeviceView] = field(default_factory=list[m.DeviceView])
    complete: bool = True
    unsupported: bool = False


def _notify_log(device: m.DeviceView) -> None:
    log.warning("%s", notice_text(device))


async def sync_job_keys(
    gate: GateClient,
    *,
    home: Path | None,
    identity: str | None,
    gate_url: str,
    party: str,
    budget: float = SYNC_BUDGET,
    notify: Notify | None = None,
    catch_up: bool = True,
) -> SyncReport:
    """D5 of decisions I25, from this machine, within ``budget`` seconds:

    1. the live devices (``for=`` this one: the gate marks it used); one ``notify``
       (default: a warning in the log) for each this machine has not shown yet;
    2. the shares that failed here (``jobs/<id>.unshared``), to every live device shown;
    3. every device shown and not served yet (itself included: its own jobs' keys are then
       kept for it too): the jobs it lacks, from the keys held here -- a job key file, or
       this device's own wrap -- each with its proof. A job no key here opens is left to the
       device that has it. Where a device's listing stopped (the budget, the network, a
       proof the gate refused) is kept, and the next catch-up resumes there; a device
       served to the end is remembered.

    ``catch_up=False`` (an API key's session: not a device, it shows nothing and persists
    nothing of its own) does step 2 only, to every live device; so does a machine that
    declined to be a device (``kontra login --no-device``), to the devices it showed. Raises
    what the gate raises for the device list; every later failure is logged and ends the
    catch-up early. A gate without devices is an empty, ``unsupported`` report."""
    home = home or kontra_home()
    report = SyncReport()
    notify = notify or _notify_log
    unshared = unshared_jobs(home)
    program = not catch_up
    if catch_up and DeviceState.load(home, gate_url, party).device == "declined":
        catch_up = False  # `kontra login --no-device`: no part in the account's devices
    if not catch_up and not unshared:
        return report
    mine = age.public_key_of(identity) if identity is not None else None
    try:
        listed = (await gate.devices(for_=mine if catch_up else None)).devices
    except GateError as err:
        if unsupported(err):
            report.unsupported = True
            return report
        raise
    targets = list(listed)
    state = DeviceState()
    if not program:
        report.notices = notice_new_devices(
            listed, home=home, gate_url=gate_url, party=party, mine=mine, notify=notify
        )
        targets = shown(listed, home=home, gate_url=gate_url, party=party, mine=mine)
        state = DeviceState.load(home, gate_url, party)
        live = {d.public_key for d in listed}
        if not state.synced <= live or not state.cursors.keys() <= live:
            state.synced &= live
            state.cursors = {k: v for k, v in state.cursors.items() if k in live}
            state.save(home, gate_url, party)
    try:
        async with asyncio.timeout(budget):
            for job_id in unshared:
                report.shared += await _retry_share(gate, job_id, home, targets)
            if not catch_up:
                return report
            keys = local_job_keys(home)
            order = sorted(targets, key=lambda d: d.public_key != mine)  # this device first
            for d in order:
                if d.public_key in state.synced:
                    continue

                def progress(cursor: str | None, device: str = d.public_key) -> None:
                    if cursor is None:
                        state.cursors.pop(device, None)
                    else:
                        state.cursors[device] = cursor
                    state.save(home, gate_url, party)

                stored, complete = await _serve(
                    gate,
                    d.public_key,
                    keys,
                    identity,
                    mine,
                    start=state.cursors.get(d.public_key),
                    progress=progress,
                )
                report.shared += stored
                if not complete:
                    report.complete = False
                    break  # a refused proof: the next catch-up sends these again
                state.synced.add(d.public_key)
                state.cursors.pop(d.public_key, None)
                report.served.append(d.public_key)
                state.save(home, gate_url, party)
    except TimeoutError:
        report.complete = False
    except (GateError, httpx.TransportError) as err:
        log.info("catching up your devices stopped: %s", err)
        report.complete = False
    return report


async def _retry_share(
    gate: GateClient, job_id: str, home: Path, listed: Sequence[m.DeviceView]
) -> int:
    """One failed share again; its marker goes once it is done, or can never be."""
    try:
        secret, _ = Profile.job_identity(job_id, home)
    except (ProfileError, ValueError):
        clear_unshared(job_id, home)  # its key is gone (accepted): nothing to share
        return 0
    try:
        stored = await share_job_key(gate, job_id, secret, devices=listed)
    except ShareRefused as err:
        log.info("job %s: its key is not shared (%s)", job_id, err.code)
        if err.code in ("job_not_found", "not_shareable"):
            clear_unshared(job_id, home)
        return 0
    except (jk.JobKeyMismatch, jk.InvalidWrap) as err:
        log.warning("job %s: its key is not shared (%s)", job_id, err)
        return 0
    clear_unshared(job_id, home)
    return stored


async def _serve(
    gate: GateClient,
    device: str,
    keys: dict[str, str],
    identity: str | None,
    mine: str | None,
    *,
    start: str | None = None,
    progress: Callable[[str | None], None] = lambda _: None,
) -> tuple[int, bool]:
    """Wrap to ``device`` every job it lacks whose key this machine holds or can unwrap,
    from the listing's cursor ``start``: (wraps stored, served to the end). ``progress`` is
    told the cursor to resume from after each page whose jobs all went (None: from the
    start). A page with a proof the gate refused stops here, its cursor not passed: those
    jobs are sent again by the next catch-up."""
    stored = 0
    cursor = start
    while True:
        page = await gate.job_keys(device, for_=mine, cursor=cursor, limit=PAGE_LIMIT)
        items: list[m.JobKeyShare] = []
        for item in page.items:
            secret = keys.get(item.buyer_key)
            if secret is None and identity is not None:
                for w in item.wraps:
                    if w.recipient != mine:
                        continue
                    with contextlib.suppress(jk.InvalidWrap, jk.JobKeyMismatch):
                        secret = jk.unwrap_job_key(w.wrap, identity, item.buyer_key)
                        break
            if secret is None:
                continue  # another device has it
            try:
                proof = jk.open_challenge(item.challenge, secret)
            except jk.InvalidWrap:
                continue
            wrap = jk.wrap_job_key(item.job_id, secret, device)
            items.append(
                m.JobKeyShare(
                    job_id=item.job_id, proof=proof, wraps=[m.WrapIn(recipient=device, wrap=wrap)]
                )
            )
        if items:
            done = await gate.share_job_keys(items)
            stored += done.stored
            for refusal in done.refused:
                log.info("job %s: its key was not shared (%s)", refusal.job_id, refusal.code)
            if any(r.code == "bad_proof" for r in done.refused):
                return stored, False
        cursor = page.next_cursor
        if cursor is None or not page.items:
            progress(None)
            return stored, True
        progress(cursor)
