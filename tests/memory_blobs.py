"""An in-memory bucket for tests that run a seller's worker without a gate (decisions I39):
claim-checks, a job's stream blobs, and the gate's reader of them, with the gate's scoping
rules (``kontra.testing.FakeGate``). Shared by ``tests/serve`` and ``tests/temporal``."""

from __future__ import annotations

import asyncio
import hashlib
from collections.abc import Awaitable, Callable, Sequence

__all__ = ["BlobScopeError", "MemoryBlobs"]


class BlobScopeError(Exception):
    """What the gate answers 409 ``blob_scope`` for: a digest stored for another job's
    stream, or for no job's (a claim-check)."""


class MemoryBlobs:
    """An in-memory bucket for a seller's codec: claim-checks and a job's stream blobs
    (``kontra.codec.stream.StreamStore``), and the gate's reader of one job's (``reader``,
    what ``kontra._relay.relay`` takes as ``blobs``). Scoped as the gate scopes them: a
    stream blob is its job's alone -- ``put_stream`` refuses, whole, a call naming a digest
    stored for another job or for none, and a job's reader reads no other job's blob.
    ``delay``: seconds each new stream blob takes to upload, one after another (a slow
    bucket)."""

    def __init__(self, *, delay: float = 0.0) -> None:
        self.data: dict[str, bytes] = {}
        self.scopes: dict[str, str] = {}
        self.uploads = 0
        self.delay = delay

    async def put(self, sha256: str, data: bytes) -> None:
        if sha256 in self.scopes:
            raise BlobScopeError("this blob belongs to a job's stream")
        self.data[sha256] = data

    async def get(self, sha256: str, *, limit: int | None = None) -> bytes:
        data = self.data[sha256]
        if limit is not None and len(data) > limit:
            raise ValueError(f"larger than {limit} bytes")
        return data

    async def put_stream(
        self, job_id: str, blobs: Sequence[tuple[str, bytes]], done: Callable[[str], None]
    ) -> None:
        for digest, _ in blobs:
            known = self.scopes.get(digest)
            if (known is None and digest in self.data) or (known is not None and known != job_id):
                raise BlobScopeError(f"{digest} is not job {job_id}'s stream's")
        for digest, data in blobs:
            if digest not in self.data:
                if self.delay:
                    await asyncio.sleep(self.delay)
                assert hashlib.sha256(data).hexdigest() == digest
                self.data[digest] = data
                self.scopes[digest] = job_id
                self.uploads += 1
            done(digest)

    def of(self, job_id: str) -> dict[str, bytes]:
        """The stream blobs stored for ``job_id``."""
        return {d: b for d, b in self.data.items() if self.scopes.get(d) == job_id}

    def reader(self, job_id: str) -> Callable[[str, int], Awaitable[bytes | None]]:
        """The gate's reader of ``job_id``'s stream blobs: a blob if it is that job's and no
        larger than the size asked for, else None (the relay records a gap)."""

        async def fetch(sha256: str, size: int) -> bytes | None:
            data = self.data.get(sha256)
            if data is None or len(data) > size or self.scopes.get(sha256) != job_id:
                return None
            return data

        return fetch
