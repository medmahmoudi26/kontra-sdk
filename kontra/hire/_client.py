"""Hiring from a script (PRD §7.1): :class:`Kontra`, :class:`QuoteResult`, :class:`Job`.

``Kontra()`` reads the profile (``$KONTRA_HOME``), installs the buyer runtime (unless the
process already has one) and connects lazily: to the gate on the first call, to Temporal on
the first hire. Then, for one job:

1. ``quote``: the offering is resolved in the catalog; the input is validated against the
   published contract *before* it is encrypted (§11.3), encrypted to the seller and us
   (the pydantic converter, then :class:`~kontra.codec.KontraCodec` in the buyer role under
   the ``quote`` operation's serialization context) and relayed by the gate (I4).
2. ``hire``: the gate locks the price and signs a hold token; the buyer then starts the
   *standalone* Nexus operation ``execute`` in its own namespace with the token in the
   ``kontra-hold`` header and ``schedule_to_close_timeout = max_duration`` (I4). No buyer
   worker is needed, and the job survives the script. A start the seller's handler refuses
   (a bad token, an input it will not take) is cancelled at the gate so the hold refunds at
   once, and raises :class:`~kontra.hire.JobFailed`.
3. ``result``: the operation's outcome, decrypted with our key and wrapped as a
   :class:`~kontra.hire.Record` from the contract's output schema (artifacts become
   :class:`~kontra.types.Artifact` s bound to this client), or as ``output_type``. When the
   operation ends without a result, the gate's record of the job decides (decisions I6): a
   delivery the gate counted (and will release) is read from the gate; only a job the gate
   closed without one raises :class:`~kontra.hire.JobFailed`.

When Temporal cannot be reached the gate stands in: it starts the operation for us
(``POST /v1/jobs/{id}/start``) and serves the stored, still encrypted, delivery
(``GET /v1/jobs/{id}/result``).
"""

from __future__ import annotations

import asyncio
import base64
import binascii
import dataclasses
import json
import logging
import math
import re
from collections.abc import AsyncIterator, Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import ROUND_FLOOR, Decimal
from pathlib import Path
from types import TracebackType
from typing import Any, TypeVar, cast, overload

import httpx
import temporalio.api.enums.v1 as temporal_enums
import temporalio.api.workflowservice.v1 as temporal_service
from google.protobuf.message import DecodeError
from pydantic import BaseModel, PrivateAttr, TypeAdapter
from temporalio.api.common.v1 import Payload
from temporalio.client import Client, NexusOperationFailureError, NexusOperationHandle
from temporalio.contrib.pydantic import PydanticPayloadConverter
from temporalio.converter import DataConverter, NexusSerializationContext
from temporalio.exceptions import NexusOperationAlreadyStartedError
from temporalio.service import RPCError, RPCStatusCode

from kontra import _runtime
from kontra.artifacts import download, iter_plaintext, upload
from kontra.auth import Session
from kontra.codec import CodecError, GateBlobStore, KontraCodec, data_converter
from kontra.codec import age as age_crypto
from kontra.contract import ContractError, ContractInfo, InputValidator
from kontra.contract.names import NameError_
from kontra.gate_client import GateClient, GateError
from kontra.gate_client import models as m
from kontra.hire._errors import (
    HireRefused,
    InsufficientFunds,
    InvalidDelivery,
    InvalidInput,
    JobFailed,
    KontraError,
    PriceAboveMax,
    QuoteRejected,
    SellerBusy,
)
from kontra.hire._record import Record, wrap
from kontra.hire._service import (
    EXECUTE_OPERATION,
    HOLD_HEADER,
    Outcome,
    input_value,
    outcome_of,
    price_text,
)
from kontra.ledger import CURRENCIES, Money
from kontra.profile import Profile
from kontra.serve.jobs import SellerKeyCache
from kontra.types import Artifact

__all__ = [
    "Job",
    "Kontra",
    "Progress",
    "QuoteResult",
    "TemporalUnavailable",
    "buyer_data_converter",
]

log = logging.getLogger("kontra.hire")

_T = TypeVar("_T")
QUOTE_OPERATION = "quote"
#: Gate refusals that are transient or about our credentials, never the buyer's request.
_NOT_REFUSALS = frozenset({401, 408, 429})
_CLOSED = frozenset({m.JobState.COMPLETED, m.JobState.FAILED, m.JobState.CANCELED})
_POLL_START, _POLL_MAX = 0.2, 2.0
#: How long accept/reject wait for the gate to observe a delivery we already received.
_GATE_CATCH_UP = 60.0
#: How long past a job's deadline to wait for the gate's verdict on it (one sweep is enough),
#: and the longest wait in all: when ``execute`` has ended the gate's deadline is at most its
#: grace away (acceptance 4: a minute), unless the gate is not observing at all.
_VERDICT_MARGIN = 60.0
_VERDICT_MAX_WAIT = 300.0
_OPEN = frozenset({m.JobState.SUBMITTED, m.JobState.WORKING})
#: One long poll for the start of ``execute`` never waits longer than this.
_START_POLL_CHUNK = 10.0
_STAGE_STARTED = temporal_enums.NexusOperationWaitStage.NEXUS_OPERATION_WAIT_STAGE_STARTED


class TemporalUnavailable(Exception):
    """Temporal could not be reached; the gate stands in where it can."""


def buyer_data_converter(codec: KontraCodec) -> DataConverter:
    """The pydantic payload converter (models round-trip) with ``codec``, failures encoded
    too (decisions I7). The same converter for the client, the hire worker and the gate
    fallback, so every door encrypts and decrypts alike."""
    return dataclasses.replace(
        data_converter(codec), payload_converter_class=PydanticPayloadConverter
    )


class _BuyerArtifact(Artifact):
    """An artifact from a delivery, bound to the client that received it: downloads use that
    client's gate and key, whatever runtime the process has installed (a process may serve
    as a seller and hire as a buyer at once)."""

    _gate: GateClient | None = PrivateAttr(default=None)
    _identity: str | None = PrivateAttr(default=None)

    def __reduce__(self) -> tuple[Any, ...]:
        """Pickled as a plain :class:`Artifact`: the binding (a live gate client and our
        secret key) never goes into a pickle, a cache or another process."""
        return (Artifact.model_validate, (self.model_dump(),))

    def __copy__(self) -> _BuyerArtifact:
        return self  # frozen: a copy would be indistinguishable

    def __deepcopy__(self, memo: dict[int, Any] | None = None) -> _BuyerArtifact:
        # pydantic would deep-copy the private gate client (and fail on its locks).
        return self

    def _bound(self) -> tuple[GateClient, str]:
        if self._gate is None or self._identity is None:  # a copy made without the binding
            rt = _runtime.current()
            return rt.gate, rt.identity
        return self._gate, self._identity

    async def download(self, path: str | Path) -> Path:
        gate, identity = self._bound()
        return await download(gate, self, identity, Path(path))

    async def chunks(self) -> AsyncIterator[bytes]:
        gate, identity = self._bound()
        async for chunk in iter_plaintext(gate, self, identity):
            yield chunk


class _CheckedSellerKeys(SellerKeyCache):
    """The codec's ``endpoint -> seller key`` lookup. Filled from catalog entries and hire
    answers this client has checked; on a miss (another worker process, a restart) the
    catalog's ``GET /v1/endpoints/{endpoint}`` answer is checked the same way -- its
    document's own hash, and the entry's key being the one that document seals -- so a Nexus
    input is never encrypted to a key no sealed contract names."""

    def __init__(self, gate: GateClient, check: Callable[[m.CatalogEntry], ContractInfo]) -> None:
        super().__init__(gate)
        self._gate_client = gate
        self._check = check

    async def seller_key(self, endpoint: str) -> str:
        found = self._keys.get(endpoint)
        if found is not None:
            return found
        entry = await self._gate_client.by_endpoint(endpoint)
        if entry.endpoint != endpoint:
            raise LookupError("the catalog answered for another endpoint")
        self._check(entry)  # HireRefused if the entry and its sealed contract disagree
        self.remember(endpoint, entry.public_key)
        return entry.public_key


def _floor_minor(value: Decimal | int | float | str, currency: str) -> int:
    """A maximum in minor units, rounded *down*: a cap of 19.999 never allows 20.00."""
    exp = CURRENCIES.get(currency)
    if exp is None:
        raise HireRefused(f"the quote is in {currency!r}, a currency this SDK does not know")
    d = Decimal(price_text(value)) * (Decimal(10) ** exp)
    return int(d.to_integral_value(rounding=ROUND_FLOOR))


def _refusal(err: GateError, offering: str, currency: str | None = None) -> KontraError | None:
    """The hire error a gate refusal means, or None when it is not the buyer's to act on
    (the gate or seller is down, our credentials expired: those stay ``GateError``)."""
    detail = err.detail or {}
    if err.code == "price_above_max":
        amount, cap = detail.get("amount_minor"), detail.get("max_price_minor")

        def major(minor: object) -> str:
            if not isinstance(minor, int) or currency is None or currency not in CURRENCIES:
                return "0"
            return str(Money(minor, currency).to_decimal())

        return PriceAboveMax(
            f"{offering}: {err.message}",
            offering=offering,
            price=major(amount),
            max_price=major(cap),
            currency=currency or "",
        )
    if err.code == "busy":
        eta = detail.get("eta_seconds")
        return SellerBusy(
            f"{offering}: {err.message}",
            offering=offering,
            eta_seconds=eta if isinstance(eta, int) else None,
        )
    if err.code == "insufficient_funds":
        return InsufficientFunds(err.message, offering=offering)
    if 400 <= err.status < 500 and err.status not in _NOT_REFUSALS:
        return HireRefused(f"{offering}: {err.message}", code=err.code, status=err.status)
    return None


def _unavailable(err: RPCError) -> bool:
    return err.status in (RPCStatusCode.UNAVAILABLE, RPCStatusCode.DEADLINE_EXCEEDED)


# --------------------------------------------------------------------------- progress


@dataclass(frozen=True, slots=True)
class Progress:
    """One progress event of a job (§8.4).

    ``heartbeat``: the seller's own ``activity.heartbeat(details)``, decrypted with our key;
    ``percent`` and ``message`` are read from details that are a mapping with those keys.
    ``history``: liveness the platform reads from workflow history metadata (state, the
    activities running, how many completed). ``str(p)`` follows the rendering convention:
    percent and message when the seller gave them, else "alive, in <activity>, at <time>".
    """

    seq: int
    at: datetime
    source: m.ProgressSource
    activity: str | None = None
    percent: float | None = None
    message: str | None = None
    details: Any = None
    state: m.JobState | None = None
    activities_running: tuple[str, ...] = ()
    activities_completed: int | None = None
    last_event: str | None = None
    continue_as_new_count: int | None = None

    def __str__(self) -> str:
        """The rendering convention of §8.4. The message and activity names are the
        seller's text: control characters are dropped here (``message`` keeps them), so
        printing progress cannot drive the buyer's terminal."""
        if self.percent is not None or self.message is not None:
            parts: list[str] = []
            if self.percent is not None:
                parts.append(f"{self.percent:g}%")
            if self.message is not None:
                parts.append(_printable(self.message))
            return " ".join(parts)
        where = _printable(self.activity or ", ".join(self.activities_running))
        when = self.at.isoformat(timespec="seconds")
        if where:
            return f"alive, in {where}, at {when}"
        state = self.state.value if self.state is not None else "alive"
        return f"{state}, at {when}"


#: C0 and C1 control characters (ESC starts a terminal escape sequence), DEL included.
_CONTROL = re.compile(r"[\x00-\x1f\x7f-\x9f]")


def _printable(text: str) -> str:
    return _CONTROL.sub("", text)


def _heartbeat_fields(details: Any) -> tuple[float | None, str | None]:
    """``percent`` and ``message`` of decrypted heartbeat details. The details are the
    seller's JSON: anything that is not a finite number or a string is ignored, never
    raised (an integer too large for a float would otherwise end ``progress()``)."""
    if not isinstance(details, Mapping):
        return None, None
    found = cast("Mapping[str, Any]", details)
    percent, message = found.get("percent"), found.get("message")
    pct: float | None = None
    if isinstance(percent, int | float) and not isinstance(percent, bool):
        try:
            pct = float(percent)
        except OverflowError:
            pct = None
        if pct is not None and not math.isfinite(pct):
            pct = None
    return pct, message if isinstance(message, str) else None


# --------------------------------------------------------------------------- quote


@dataclass
class QuoteResult:
    """The seller's answer for one input. ``status`` is ``ok`` (a fixed price, valid until
    ``expires_at``), ``rejected`` (``reason``) or ``busy`` (``eta``)."""

    status: m.QuoteStatus
    offering: str
    quote_id: str | None
    price: Decimal | None
    currency: str | None
    amount_minor: int | None
    max_duration: timedelta | None
    reason: str | None
    eta: timedelta | None
    expires_at: datetime | None
    contract_hash: str
    endpoint: str
    service: str
    seller_public_key: str
    _kontra: Kontra = field(repr=False)
    _input: dict[str, Any] = field(repr=False)

    @property
    def ok(self) -> bool:
        return self.status is m.QuoteStatus.OK

    async def hire(self, max_price: Decimal | int | float | str | None = None) -> Job:
        """Lock the price and start the job. Refuses to lock above ``max_price`` (in the
        quote's currency): :class:`PriceAboveMax`. A rejected or busy quote raises
        :class:`QuoteRejected` / :class:`SellerBusy`."""
        hired = await self._kontra._lock(self, max_price)
        return await self._kontra._start(hired, self._input)


# --------------------------------------------------------------------------- job


class Job:
    """One hired job. ``await job.result()`` blocks until delivery."""

    def __init__(
        self,
        kontra: Kontra,
        *,
        job_id: str,
        offering: str,
        amount_minor: int,
        currency: str,
        operation_id: str,
        handle: NexusOperationHandle[Any] | None = None,
    ) -> None:
        self._kontra = kontra
        self.job_id = job_id
        self.offering = offering
        self.amount_minor = amount_minor
        self.currency = currency
        self.operation_id = operation_id
        self._handle = handle
        self._delivered = False

    @property
    def price(self) -> Decimal:
        return Money(self.amount_minor, self.currency).to_decimal()

    def __repr__(self) -> str:
        return (
            f"Job({self.job_id!r}, offering={self.offering!r}, price={self.price} {self.currency})"
        )

    async def status(self) -> m.JobView:
        """The gate's view: A2A state, escrow, deadline, delivery hash, reason."""
        return await self._kontra.gate.job(self.job_id)

    @overload
    async def result(self, timeout: float | None = None) -> Record: ...  # noqa: ASYNC109
    @overload
    async def result(
        self,
        timeout: float | None = None,  # noqa: ASYNC109
        *,
        output_type: type[_T],
    ) -> _T: ...
    async def result(
        self,
        timeout: float | None = None,  # noqa: ASYNC109 -- the PRD's API
        *,
        output_type: type[Any] | None = None,
    ) -> Any:
        """The seller's output, decrypted and checked against the contract.

        A :class:`~kontra.hire.Record` (artifacts are :class:`~kontra.types.Artifact` s:
        ``await result.records.download(path)``), or ``output_type`` (a pydantic model, e.g.
        one generated by ``kontra pull``) validated from it. Raises
        :class:`~kontra.hire.JobFailed` when the gate closes the job without a delivery
        (its hold is refunded) and builtin ``TimeoutError`` when ``timeout`` seconds pass
        first (the job keeps running).

        When ``execute`` ends without a result, the gate's record decides, not the
        operation (see :meth:`Kontra._verdict`): a delivery the gate counted is returned,
        because the gate releases it to the seller when the review window closes."""
        if timeout is None:
            value = await self._outcome()
        else:
            value = await asyncio.wait_for(self._outcome(), timeout)
        self._delivered = True
        return await self._kontra._deliver(self, value, output_type)

    async def _outcome(self) -> Any:
        try:
            return await self._from_temporal()
        except TemporalUnavailable as err:
            log.info("job %s: %s; waiting for the delivery through the gate", self.job_id, err)
            return await self._from_gate()

    async def _operation(self) -> NexusOperationHandle[Any]:
        if self._handle is None:
            client = await self._kontra.temporal()
            self._handle = client.get_nexus_operation_handle(self.operation_id)
        return self._handle

    async def _from_temporal(self) -> Any:
        handle = await self._operation()
        try:
            return await handle.result()
        except NexusOperationFailureError as err:
            await self._kontra._verdict(self.job_id, outcome_of(err), cause=err)
            # The gate holds a delivery the operation did not return: it is ours to read.
            return await self._kontra._open_delivery(await self._kontra.gate.result(self.job_id))
        except RPCError as err:
            if _unavailable(err):
                raise TemporalUnavailable(str(err)) from err
            if err.status == RPCStatusCode.NOT_FOUND:
                # Not in our namespace (started elsewhere, or past retention): the gate knows.
                raise TemporalUnavailable(f"operation {self.operation_id}: {err}") from err
            raise

    async def _from_gate(self) -> Any:
        delay = _POLL_START
        while True:
            view = await self._kontra.gate.job(self.job_id)
            if view.state is m.JobState.COMPLETED:
                return await self._kontra._open_delivery(
                    await self._kontra.gate.result(self.job_id)
                )
            if view.state in _CLOSED:
                raise JobFailed(
                    view.reason or f"the job is {view.state.value}",
                    state=view.state.value,
                    job_id=self.job_id,
                )
            await asyncio.sleep(delay)
            delay = min(delay * 2, _POLL_MAX)

    async def progress(self) -> AsyncIterator[Progress]:
        """Progress until the job closes: the seller's heartbeats (decrypted with our key)
        and liveness from history, in order. Server-sent events through the gate."""
        events = self._kontra.gate.progress(self.job_id)
        try:
            async for event in events:
                yield self._kontra._progress(event)
        finally:
            # A caller that stops early closes this generator: close the stream with it now,
            # not whenever the garbage collector gets to the open HTTP response.
            close = getattr(events, "aclose", None)
            if close is not None:
                await close()

    async def accept(self) -> m.JobView:
        """Release the escrow to the seller now (otherwise automatic when the review
        window closes)."""
        await self._gate_saw_delivery()
        return await self._kontra.gate.accept(self.job_id)

    async def reject(self, reason: str) -> m.JobView:
        """Open a dispute about the delivery (within the review window)."""
        await self._gate_saw_delivery()
        return await self._kontra.gate.reject(self.job_id, reason)

    async def _gate_saw_delivery(self) -> None:
        """The gate learns of a delivery by observing the seller's workflow (I6), a moment
        after ``result()`` may already have returned it. When this job has delivered to us,
        wait (bounded) until the gate's record says so, so ``accept`` right after
        ``result`` is not refused as "not delivered"."""
        if not self._delivered:
            return
        loop = asyncio.get_running_loop()
        end = loop.time() + _GATE_CATCH_UP
        delay = _POLL_START
        while loop.time() < end:
            if (await self.status()).state not in _OPEN:
                return
            await asyncio.sleep(delay)
            delay = min(delay * 2, _POLL_MAX)

    async def cancel(self) -> m.JobView:
        """Cancel before delivery: the gate refunds the hold now and stops the seller's
        workflow; the Nexus operation is cancelled too, so ``result()`` ends."""
        view = await self._kontra.gate.cancel(self.job_id)
        try:
            handle = await self._operation()
            await handle.cancel(reason="canceled by buyer")
        except (TemporalUnavailable, RPCError) as err:
            log.info(
                "job %s: the operation was not cancelled (%s); the gate stops it", self.job_id, err
            )
        return view


# --------------------------------------------------------------------------- the client


class Kontra:
    """The buyer's client. ``Kontra()`` uses the profile in ``$KONTRA_HOME``; tests pass a
    logged-in ``session``. ``client`` shares an existing Temporal connection (its data
    converter is replaced by the buyer's). ``start_timeout``: how long ``hire`` waits for
    the seller's handler to accept the start before returning the job anyway.

    The process's Kontra runtime (``Artifact`` I/O) is installed only if none is: a seller
    worker in the same process keeps its own. Artifacts this client returns are bound to it.
    """

    def __init__(
        self,
        *,
        profile: Profile | None = None,
        session: Session | None = None,
        home: str | Path | None = None,
        client: Client | None = None,
        start_timeout: float = 5.0,
    ) -> None:
        if profile is not None and session is not None:
            raise TypeError("pass a profile or a session, not both")
        self._owns_session = session is None
        if session is None:
            profile = profile if profile is not None else Profile.load(home)
            session = Session(profile, home=home)
        self._session = session
        self.profile = session.profile
        identity = session.identity()
        self.public_key = age_crypto.public_key_of(identity)
        if self.profile.public_key is not None and self.profile.public_key != self.public_key:
            raise KontraError(
                f"the key file is not the one {self.profile.party} registered "
                f"({self.profile.public_key}); copy the right key, or run `kontra login`"
            )
        self._identity = identity
        self.start_timeout = start_timeout
        self._keys = _CheckedSellerKeys(session.gate, self.contract)
        self._codec = KontraCodec(
            identity,
            self.public_key,
            role="buyer",
            seller_keys=self._keys,
            blobs=GateBlobStore(session.gate),
        )
        self.data_converter = buyer_data_converter(self._codec)
        self._client = self.adopt(client) if client is not None else None
        self._client_lock = asyncio.Lock()
        self._entries: dict[str, m.CatalogEntry] = {}
        self._infos: dict[str, ContractInfo] = {}
        self._validators: dict[str, InputValidator] = {}
        self._runtime: _runtime.Runtime | None = None
        try:
            _runtime.current()
        except _runtime.NoRuntime:
            self._runtime = _runtime.Runtime(
                party=self.profile.party,
                gate=session.gate,
                identity=identity,
                recipient=self.public_key,
            )
            _runtime.install(self._runtime)
        self._closed = False

    # ------------------------------------------------------------------ plumbing

    @property
    def gate(self) -> GateClient:
        return self._session.gate

    @property
    def session(self) -> Session:
        return self._session

    @property
    def party(self) -> str:
        return self.profile.party

    @property
    def seller_keys(self) -> SellerKeyCache:
        """endpoint -> seller key, filled from hire answers (the codec reads it)."""
        return self._keys

    def adopt(self, client: Client) -> Client:
        """``client``'s connection with the buyer's data converter."""
        config = client.config()
        config["data_converter"] = self.data_converter
        return Client(**config)

    async def temporal(self) -> Client:
        """The Temporal client for our namespace (connected on first use, as the ``worker``
        role, its JWT refreshed in the background). :class:`TemporalUnavailable` if it
        cannot be reached."""
        if self._client is not None:
            return self._client
        async with self._client_lock:
            if self._client is None:
                try:
                    self._client = await self._session.connect_temporal(
                        "worker", data_converter=self.data_converter
                    )
                except RuntimeError as err:  # temporalio: "Failed client connect: ..."
                    raise TemporalUnavailable(str(err)) from err
                except RPCError as err:
                    if _unavailable(err):
                        raise TemporalUnavailable(str(err)) from err
                    raise
            return self._client

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._runtime is not None:
            _runtime.uninstall(self._runtime)
        if self._owns_session:
            await self._session.aclose()

    async def __aenter__(self) -> Kontra:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.aclose()

    # ------------------------------------------------------------------ catalog

    async def offering(self, ref: str) -> m.CatalogEntry:
        """The catalog entry ``ref`` resolves to (``acme/enrich``, ``@1``, exact), with its
        contract checked: the document's own hash, and the entry agreeing with it."""
        entry = self._entries.get(ref)
        if entry is None:
            try:
                entry = await self.gate.get_offering(ref)
            except NameError_ as err:
                raise HireRefused(str(err), code="invalid_offering") from err
            except GateError as err:
                mapped = _refusal(err, ref)
                if mapped is None:
                    raise
                raise mapped from err
            self.contract(entry)
            self._entries[entry.offering] = entry
        return entry

    def contract(self, entry: m.CatalogEntry) -> ContractInfo:
        """The validated contract of ``entry``. A document that fails the catalog's rules or
        disagrees with its entry (offering, hash, key, service) is refused: the buyer never
        encrypts to a key or validates against a schema the document does not seal."""
        info = self._infos.get(entry.contract_hash)
        if info is None:
            try:
                validator = InputValidator(entry.document)
            except ContractError as err:
                raise HireRefused(
                    f"the catalog's contract for {entry.offering} is invalid: {err}",
                    code="invalid_contract",
                ) from err
            info = validator.info
            if info.contract_hash != entry.contract_hash:
                raise HireRefused(
                    f"the catalog entry for {entry.offering} names another contract",
                    code="invalid_contract",
                )
            self._infos[info.contract_hash] = info
            self._validators[info.contract_hash] = validator
        if (
            info.offering.key != entry.offering
            or info.facts.public_key != entry.public_key
            or info.offering.service_name != entry.service
        ):
            raise HireRefused(
                f"the catalog entry for {entry.offering} does not match its contract",
                code="invalid_contract",
            )
        return info

    def _validator(self, entry: m.CatalogEntry) -> InputValidator:
        self.contract(entry)
        return self._validators[entry.contract_hash]

    async def _wire(
        self, endpoint: str, service: str, value: dict[str, Any], operation: str
    ) -> m.WirePayload:
        [payload] = self.data_converter.payload_converter.to_payloads([value])
        codec = self._codec.with_context(
            NexusSerializationContext(endpoint=endpoint, service=service, operation=operation)
        )
        [encoded] = await codec.encode([payload])
        return m.WirePayload(b64=base64.b64encode(encoded.SerializeToString()).decode("ascii"))

    # ------------------------------------------------------------------ quote & hire

    async def quote(self, offering: str, input: Any = None, /, **kwargs: Any) -> QuoteResult:
        """Ask the seller's price for this input (a pydantic model, a dict, or keywords).
        The input is checked against the published contract before it is encrypted; a
        mismatch raises :class:`~kontra.hire.InvalidInput` naming the path, and nothing is
        sent. A rejected or busy answer is returned, not raised (``hire`` raises)."""
        value = input_value(input, kwargs)
        entry = await self.offering(offering)
        try:
            self._validator(entry).validate(value)
        except ContractError as err:
            raise InvalidInput(
                f"the input does not match the contract of {entry.offering}: {err}",
                offering=entry.offering,
                path=err.path,
            ) from err
        self._keys.remember(entry.endpoint, entry.public_key)
        wire = await self._wire(entry.endpoint, entry.service, value, QUOTE_OPERATION)
        try:
            view = await self.gate.quote(entry.offering, wire)
        except GateError as err:
            mapped = _refusal(err, entry.offering)
            if mapped is None:
                raise
            raise mapped from err
        if (
            view.offering,
            view.contract_hash,
            view.seller_public_key,
            view.endpoint,
            view.service,
        ) != (
            entry.offering,
            entry.contract_hash,
            entry.public_key,
            entry.endpoint,
            entry.service,
        ):
            self._entries.pop(entry.offering, None)
            raise HireRefused(
                f"{entry.offering} changed in the catalog while quoting; quote again",
                code="contract_changed",
            )
        price = (
            Money(view.amount_minor, view.currency).to_decimal()
            if view.amount_minor is not None and view.currency is not None
            else None
        )
        return QuoteResult(
            status=view.status,
            offering=view.offering,
            quote_id=view.quote_id,
            price=price,
            currency=view.currency,
            amount_minor=view.amount_minor,
            max_duration=(
                timedelta(seconds=view.max_duration_seconds)
                if view.max_duration_seconds is not None
                else None
            ),
            reason=view.reason,
            eta=timedelta(seconds=view.eta_seconds) if view.eta_seconds is not None else None,
            expires_at=view.expires_at,
            contract_hash=view.contract_hash,
            endpoint=view.endpoint,
            service=view.service,
            seller_public_key=view.seller_public_key,
            _kontra=self,
            _input=value,
        )

    async def hire(
        self,
        offering: str,
        input: Any = None,
        /,
        *,
        max_price: Decimal | int | float | str | None = None,
        **kwargs: Any,
    ) -> Job:
        """Quote, then hire: ``await k.hire("acme/enrich", urls=urls, max_price=20)``."""
        q = await self.quote(offering, input, **kwargs)
        return await q.hire(max_price)

    async def _lock(
        self, q: QuoteResult, max_price: Decimal | int | float | str | None
    ) -> m.HireView:
        """Check the quote and the cap, then lock the hold (no Temporal involved)."""
        if q.status is m.QuoteStatus.REJECTED:
            raise QuoteRejected(
                f"{q.offering} rejected this input: {q.reason}",
                offering=q.offering,
                reason=q.reason or "",
            )
        if q.status is m.QuoteStatus.BUSY or q.quote_id is None:
            eta = int(q.eta.total_seconds()) if q.eta is not None else None
            raise SellerBusy(
                f"{q.offering} is busy: {q.reason or 'try again later'}",
                offering=q.offering,
                eta_seconds=eta,
            )
        if q.amount_minor is None or q.currency is None or q.max_duration is None:
            raise HireRefused(
                f"the gate's quote for {q.offering} names no price or duration",
                code="invalid_quote",
            )
        cap: int | None = None
        if max_price is not None:
            cap = _floor_minor(max_price, q.currency)
            if q.amount_minor > cap:
                raise PriceAboveMax(
                    f"{q.offering} quoted {Money(q.amount_minor, q.currency).display()}, above "
                    f"your maximum of {price_text(max_price)} {q.currency}; nothing was locked",
                    offering=q.offering,
                    price=str(q.price),
                    max_price=price_text(max_price),
                    currency=q.currency,
                )
        try:
            hired = await self.gate.hire(q.quote_id, max_price_minor=cap)
        except GateError as err:
            mapped = _refusal(err, q.offering, q.currency)
            if mapped is None:
                raise
            raise mapped from err
        # The seller key here is what the job input is encrypted to: it must be the one the
        # quoted contract seals, never a key the hire answer merely names.
        if (
            hired.offering,
            hired.contract_hash,
            hired.amount_minor,
            hired.currency,
            hired.max_duration_seconds,
            hired.endpoint,
            hired.service,
            hired.seller_public_key,
        ) != (
            q.offering,
            q.contract_hash,
            q.amount_minor,
            q.currency,
            int(q.max_duration.total_seconds()),
            q.endpoint,
            q.service,
            q.seller_public_key,
        ):
            await self._abandon(hired.job_id, "the hold does not match the quote")
            raise HireRefused(
                f"the gate locked a hold that differs from the quote for {q.offering}; "
                "it was cancelled",
                code="hold_mismatch",
            )
        self._keys.remember(hired.endpoint, hired.seller_public_key)
        return hired

    async def _start(self, hired: m.HireView, value: dict[str, Any]) -> Job:
        """Start ``execute`` as a standalone Nexus operation in our namespace (the gate does
        it for us when Temporal is unreachable). A start that is refused, or cannot be made,
        is settled at the gate (:meth:`_verdict`): cancelled so the hold refunds now, unless
        the gate already holds a delivery for the job -- then the job is returned and
        ``result()`` reads that delivery."""
        job = Job(
            self,
            job_id=hired.job_id,
            offering=hired.offering,
            amount_minor=hired.amount_minor,
            currency=hired.currency,
            operation_id=hired.operation_id,
        )
        try:
            client = await self.temporal()
            try:
                job._handle = await client.create_nexus_client(
                    service=hired.service, endpoint=hired.endpoint
                ).start_operation(
                    EXECUTE_OPERATION,
                    value,
                    id=hired.operation_id,
                    headers={HOLD_HEADER: hired.hold_token},
                    schedule_to_close_timeout=timedelta(seconds=hired.max_duration_seconds),
                )
            except NexusOperationAlreadyStartedError:
                job._handle = client.get_nexus_operation_handle(hired.operation_id)
            except RPCError as err:
                if _unavailable(err):
                    raise TemporalUnavailable(str(err)) from err
                # Refused outright (no such endpoint, not allowed): nothing will ever run.
                await self._verdict(
                    hired.job_id,
                    Outcome("refused", f"Temporal refused to start the job: {err}"),
                    cause=err,
                )
                return job
        except TemporalUnavailable as err:
            log.info("job %s: %s; the gate starts it", hired.job_id, err)
            wire = await self._wire(hired.endpoint, hired.service, value, EXECUTE_OPERATION)
            try:
                await self.gate.start(hired.job_id, wire)
            except GateError as gate_err:
                await self._verdict(
                    hired.job_id,
                    Outcome("refused", f"the job could not be started: {gate_err.message}"),
                    cause=gate_err,
                )
            return job
        refused = await self._start_refused(client, hired)
        if refused is not None:
            await self._verdict(hired.job_id, refused)
        return job

    async def _start_refused(self, client: Client, hired: m.HireView) -> Outcome | None:
        """Wait up to ``start_timeout`` for the handler to take the start. The refusal if it
        refused; None if it started, or if the seller has not answered yet (an offline seller
        is the gate's to refund at the deadline)."""
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self.start_timeout
        req = temporal_service.PollNexusOperationExecutionRequest(
            namespace=client.namespace,
            operation_id=hired.operation_id,
            wait_stage=_STAGE_STARTED,
        )
        while (left := deadline - loop.time()) > 0:
            try:
                resp = await client.workflow_service.poll_nexus_operation_execution(
                    req, retry=False, timeout=timedelta(seconds=min(left, _START_POLL_CHUNK))
                )
            except RPCError as err:
                if err.status == RPCStatusCode.DEADLINE_EXCEEDED:
                    continue
                log.info("job %s: cannot tell whether it started (%s)", hired.job_id, err)
                return None
            if resp.WhichOneof("outcome") == "failure":
                converter = client.data_converter.with_context(
                    NexusSerializationContext(
                        endpoint=hired.endpoint, service=hired.service, operation=EXECUTE_OPERATION
                    )
                )
                cause = await converter.decode_failure(resp.failure)
                wrapper = NexusOperationFailureError(cause=cause)
                outcome = outcome_of(wrapper)
                return outcome if outcome.kind == "refused" else None
            if resp.WhichOneof("outcome") == "result" or resp.operation_token:
                return None
            if resp.wait_stage >= _STAGE_STARTED:
                return None
            await asyncio.sleep(0.1)  # the server answered early with nothing new
        return None

    async def _verdict(
        self, job_id: str, outcome: Outcome, *, cause: BaseException | None = None
    ) -> m.JobView:
        """``execute`` ended (or could not start) without a result: the gate's view of the
        job once it is settled, if the gate counted a delivery; otherwise raise
        :class:`JobFailed` with ``outcome``'s reason and the state the gate closed the job in.

        The escrow decides, not the operation (decisions I6). The gate counts a delivery by
        watching the seller's ``job-<id>``, which can complete although the buyer's
        operation did not return it: a seller that starts ``job-<id>`` itself (the buyer's
        own start is then refused as "already ran"), or one that delivers after the buyer's
        operation timed out (``max_duration`` from the buyer's start) but before the gate's
        deadline (lock + ``max_duration`` + grace). The gate releases such a delivery to the
        seller when the review window closes; telling the buyer "failed, refunded" would
        have it pay without looking. A refused start is cancelled first, so its hold refunds
        now. If the gate cannot be reached, the operation's outcome is all there is."""
        if outcome.kind == "canceled":
            raise JobFailed(outcome.reason, state=outcome.state, job_id=job_id) from cause
        try:
            if outcome.kind == "refused":
                await self._cancel_at_gate(job_id)
            view = await self._closed_view(job_id)
        except (GateError, httpx.TransportError) as err:
            log.warning("job %s: the gate's verdict is unavailable (%s)", job_id, err)
            raise JobFailed(outcome.reason, state=outcome.state, job_id=job_id) from cause
        if view.state is m.JobState.COMPLETED:
            log.warning(  # %r: the reason is the seller's text
                "job %s: %r, yet the gate recorded a delivery; returning it (reject it if it "
                "is not what was hired)",
                job_id,
                outcome.reason,
            )
            return view
        state = outcome.state if view.state in _OPEN else view.state.value
        raise JobFailed(outcome.reason, state=state, job_id=job_id) from cause

    async def _cancel_at_gate(self, job_id: str) -> None:
        """Cancel a job that will not run here, so its hold refunds now. A 409 means the gate
        has closed it already (delivered, failed, canceled): its verdict stands."""
        try:
            await self.gate.cancel(job_id)
        except GateError as err:
            if err.status != 409:
                raise

    async def _closed_view(
        self, job_id: str, *, beat: Callable[[], None] | None = None
    ) -> m.JobView:
        """The gate's view of ``job_id`` once the gate has closed it -- or the last view, if
        the gate has not by the job's deadline plus a margin (it closes every job by then),
        and at most :data:`_VERDICT_MAX_WAIT`. ``beat`` is called on every poll (an activity
        heartbeats with it)."""
        loop = asyncio.get_running_loop()
        view = await self.gate.job(job_id)
        left = (view.deadline - datetime.now(UTC)).total_seconds()
        end = loop.time() + min(max(left, 0.0) + _VERDICT_MARGIN, _VERDICT_MAX_WAIT)
        delay = _POLL_START
        while view.state in _OPEN and loop.time() < end:
            if beat is not None:
                beat()
            await asyncio.sleep(delay)
            delay = min(delay * 2, _POLL_MAX)
            view = await self.gate.job(job_id)
        return view

    async def _abandon(self, job_id: str, reason: str) -> None:
        """Cancel a hold this client refuses to use (best effort: if the gate cannot, the
        hold refunds at its deadline)."""
        try:
            await self.gate.cancel(job_id)
        except (GateError, httpx.TransportError) as err:
            log.warning(
                "job %s: %s; could not cancel it (%s); it refunds at its deadline",
                job_id,
                reason,
                err,
            )

    # ------------------------------------------------------------------ delivery & progress

    async def _open_delivery(self, view: m.ResultView) -> Any:
        """The gate's stored delivery, decrypted (the bucket and the gate hold ciphertext)."""
        try:
            raw = base64.b64decode(view.payload.b64, validate=True)
        except (binascii.Error, ValueError) as err:
            raise InvalidDelivery("the stored delivery is not base64", job_id=view.job_id) from err
        try:
            payload = Payload()
            payload.ParseFromString(raw)
            [plain] = await self._codec.decode([payload])
            return self.data_converter.payload_converter.from_payloads([plain])[0]
        # The seller's bytes: a bad proto, ciphertext not for us, an unknown encoding (the
        # converter's KeyError) or broken JSON (its RuntimeError).
        except (CodecError, DecodeError, ValueError, KeyError, RuntimeError) as err:
            raise InvalidDelivery(
                f"the stored delivery of job {view.job_id} cannot be read: {err}",
                job_id=view.job_id,
            ) from err

    async def _deliver(self, job: Job, value: Any, output_type: Any) -> Any:
        """``output_type`` is a class or a typing form (``dict[str, Any]`` is not a class)."""
        info = await self._check_delivery(job.job_id, job.offering, value)
        if output_type is not None:
            if isinstance(output_type, type) and issubclass(output_type, BaseModel):
                return output_type.model_validate(value)
            return TypeAdapter(output_type).validate_python(value)  # e.g. dict[str, Any]
        return wrap(value, info.output_schema, artifact=self._bind)

    async def _check_delivery(self, job_id: str, offering: str, value: Any) -> ContractInfo:
        """The contract ``value`` was checked against: a delivery is the seller's input to
        us, and one that breaks the published output schema is :class:`InvalidDelivery`."""
        entry = await self.offering(offering)
        info = self.contract(entry)
        try:
            self._validators[info.contract_hash].validate_output(value)
        except ContractError as err:
            raise InvalidDelivery(
                f"the delivery of job {job_id} does not match the contract of "
                f"{offering}: {err}; reject it with job.reject(...)",
                job_id=job_id,
                value=value,
            ) from err
        return info

    def _bind(self, artifact: Artifact) -> Artifact:
        bound = _BuyerArtifact.model_validate(artifact.model_dump())
        bound._gate = self.gate
        bound._identity = self._identity
        return bound

    def _progress(self, event: m.ProgressEvent) -> Progress:
        if event.source is m.ProgressSource.HEARTBEAT and event.ciphertext_b64 is not None:
            details: Any = None
            try:
                plain = age_crypto.decrypt(base64.b64decode(event.ciphertext_b64), self._identity)
                details = json.loads(plain)
            except (age_crypto.AgeError, binascii.Error, ValueError) as err:
                log.warning("progress event %d does not decrypt for us: %s", event.seq, err)
            percent, message = _heartbeat_fields(details)
            return Progress(
                seq=event.seq,
                at=event.at,
                source=event.source,
                activity=event.activity,
                percent=percent,
                message=message,
                details=details,
                state=event.state,
            )
        return Progress(
            seq=event.seq,
            at=event.at,
            source=event.source,
            activity=event.activity,
            state=event.state,
            activities_running=tuple(event.activities_running or ()),
            activities_completed=event.activities_completed,
            last_event=event.last_event,
            continue_as_new_count=event.continue_as_new_count,
        )

    # ------------------------------------------------------------------ jobs, money, files

    async def job(self, job_id: str) -> Job:
        """A job hired earlier (by this or another process)."""
        view = await self.gate.job(job_id)
        return Job(
            self,
            job_id=view.job_id,
            offering=view.offering,
            amount_minor=view.amount_minor,
            currency=view.currency,
            operation_id=view.operation_id,
        )

    async def jobs(
        self, *, as_: str = "buyer", state: m.JobState | str | None = None, limit: int = 50
    ) -> list[m.JobView]:
        """Our jobs, newest first (``as_="seller"`` for the jobs we sell), up to ``limit``."""
        out: list[m.JobView] = []
        cursor: str | None = None
        wanted = state.value if isinstance(state, m.JobState) else state
        while len(out) < limit:
            page = await self.gate.jobs(
                as_=as_, state=wanted, cursor=cursor, limit=min(200, limit - len(out))
            )
            out += page.items
            cursor = page.next_cursor
            if cursor is None or not page.items:
                break
        return out[:limit]

    async def balance(self, currency: str = "USD") -> m.Balance:
        return await self.gate.balance(currency)

    async def topup(
        self, amount: Decimal | int | float | str, currency: str = "USD"
    ) -> m.TopupResult:
        """Add ``amount`` (major units, e.g. ``"25.00"``). Play money under ``kontra dev``."""
        return await self.gate.topup(Money.from_decimal(amount, currency).amount_minor, currency)

    async def upload(
        self, offering: str, path: str | Path, content_type: str | None = None
    ) -> Artifact:
        """Encrypt a local file to ``offering``'s seller and to us, upload it (streaming),
        and return the :class:`~kontra.types.Artifact` to pass as an input field."""
        entry = await self.offering(offering)
        artifact = await upload(
            self.gate,
            Path(path),
            recipients=[entry.public_key, self.public_key],
            content_type=content_type,
        )
        return self._bind(artifact)
