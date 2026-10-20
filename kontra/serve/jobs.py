"""Who a seller's payloads are for: the job directory and the seller-key cache (decisions I7).

:class:`JobDirectory` maps a job to its buyer's public key (and the contract hash it was
hired under). The Nexus handler records it from the verified hold token -- the key travels
signed by the gate, so a seller cannot be tricked into encrypting a delivery to someone
else -- and the codec reads it (a job stream's items too: the progress forwarder publishes
through the codec, decisions I31). After a restart, or in another
worker process, the entry is missing; the directory then asks the gate
(``GET /v1/jobs/{id}/seller``), which only answers the job's seller.

:class:`SellerKeyCache` is the other direction, for a seller whose own workflows hire
other offerings: the public key of the offering behind a Nexus endpoint, from the catalog
(``GET /v1/endpoints/{endpoint}``). An endpoint name is opaque and never parsed.
"""

from __future__ import annotations

import asyncio
from collections import OrderedDict
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Generic, Protocol, TypeVar

from kontra.codec import age
from kontra.contract.names import is_job_id
from kontra.gate_client import models as m

__all__ = ["JobDirectory", "JobEntry", "SellerKeyCache"]

_MAX_ENTRIES = 10_000

_K = TypeVar("_K")
_V = TypeVar("_V")


class SellerJobGate(Protocol):
    async def seller_job(self, job_id: str) -> m.SellerJobView: ...


class EndpointGate(Protocol):
    async def by_endpoint(self, endpoint: str) -> m.CatalogEntry: ...


@dataclass(frozen=True, slots=True)
class JobEntry:
    job_id: str
    buyer_key: str
    contract_hash: str


class _Lru(Generic[_K, _V]):
    """A bounded map: a long-running worker sees an unbounded number of jobs."""

    def __init__(self, limit: int) -> None:
        self._limit = limit
        self._data: OrderedDict[_K, _V] = OrderedDict()

    def get(self, key: _K) -> _V | None:
        value = self._data.get(key)
        if value is not None:
            self._data.move_to_end(key)
        return value

    def put(self, key: _K, value: _V) -> None:
        self._data[key] = value
        self._data.move_to_end(key)
        while len(self._data) > self._limit:
            self._data.popitem(last=False)

    def __len__(self) -> int:
        return len(self._data)


class JobDirectory:
    """``job_id -> buyer key`` for this worker. Implements the codec's ``BuyerKeys``."""

    def __init__(self, gate: SellerJobGate, *, limit: int = _MAX_ENTRIES) -> None:
        self._gate = gate
        self._entries: _Lru[str, JobEntry] = _Lru(limit)
        #: One gate request per missing job, owned by the directory rather than by the
        #: first caller: a caller that gives up (a timed-out progress send) must not cancel
        #: it for the others (the codec encrypting the job's result).
        self._pending: dict[str, asyncio.Task[JobEntry]] = {}

    def record(self, job_id: str, buyer_key: str, contract_hash: str) -> JobEntry:
        """What the handler learned from a verified hold token."""
        if not is_job_id(job_id):
            raise ValueError(f"{job_id!r} is not a job id")
        if not age.is_recipient(buyer_key):
            raise ValueError("the buyer key is not an age X25519 recipient")
        entry = JobEntry(job_id, buyer_key, contract_hash)
        self._entries.put(job_id, entry)
        return entry

    def cached(self, job_id: str) -> JobEntry | None:
        return self._entries.get(job_id)

    async def lookup(self, job_id: str) -> JobEntry:
        """The entry for ``job_id``: from memory, else from the gate (one request per job,
        however many payloads ask at once)."""
        found = self._entries.get(job_id)
        if found is not None:
            return found
        if not is_job_id(job_id):
            raise LookupError(f"{job_id!r} is not a job id")
        task = self._pending.get(job_id)
        if task is None:
            task = asyncio.get_running_loop().create_task(
                self._fetch(job_id), name=f"kontra-seller-job {job_id}"
            )
            self._pending[job_id] = task
            task.add_done_callback(lambda t: self._done(job_id, t))
        return await asyncio.shield(task)

    async def _fetch(self, job_id: str) -> JobEntry:
        view = await self._gate.seller_job(job_id)
        if view.job_id != job_id:
            raise LookupError(f"the gate answered for job {view.job_id!r}, not {job_id!r}")
        return self.record(job_id, view.buyer_public_key, view.contract_hash)

    def _done(self, job_id: str, task: asyncio.Task[JobEntry]) -> None:
        if self._pending.get(job_id) is task:
            del self._pending[job_id]
        if not task.cancelled():
            task.exception()  # retrieved: a lookup every caller gave up on logs nothing

    async def aclose(self) -> None:
        """Cancel the gate lookups still running (the worker is stopping)."""
        tasks = list(self._pending.values())
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    async def buyer_key(self, job_id: str) -> str:
        """The codec's ``BuyerKeys`` protocol."""
        return (await self.lookup(job_id)).buyer_key

    def __len__(self) -> int:
        return len(self._entries)


class SellerKeyCache:
    """``endpoint -> the seller party's recipients``, from the catalog. Implements the
    codec's ``SellerKeys``: a seller-role worker whose workflow hires another offering
    encrypts that Nexus input to every key that seller can read with (decisions I7).

    The set is what the catalog holds now, so a fleet key added after this entry was
    cached is missed until it falls out; a worker that cannot decrypt an input is the
    seller's own signal to restart, exactly as it is for a key it never mounted."""

    def __init__(self, gate: EndpointGate, *, limit: int = 1_000) -> None:
        self._gate = gate
        self._keys: _Lru[str, tuple[str, ...]] = _Lru(limit)

    def remember(self, endpoint: str, recipients: Sequence[str]) -> None:
        """Seed the cache (a hire answer already names the seller's recipients)."""
        if isinstance(recipients, str):
            raise TypeError("recipients is a sequence of age keys, not one key")
        keys = tuple(dict.fromkeys(recipients))
        if not keys:
            raise ValueError("a seller with no recipient cannot be sent anything")
        for key in keys:
            if not age.is_recipient(key):
                raise ValueError("not an age X25519 recipient")
        self._keys.put(endpoint, keys)

    async def seller_recipients(self, endpoint: str) -> Sequence[str]:
        found = self._keys.get(endpoint)
        if found is not None:
            return found
        entry = await self._gate.by_endpoint(endpoint)
        if entry.endpoint != endpoint:
            raise LookupError("the catalog answered for another endpoint")
        self.remember(endpoint, entry.recipients)
        return tuple(entry.recipients)
