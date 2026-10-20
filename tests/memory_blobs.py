"""An in-memory bucket for tests that run a seller's worker without a gate (decisions I39):
claim-checks, a job's stream blobs, and the gate's reader of them. Shared by
``tests/serve`` and ``tests/temporal``."""

from __future__ import annotations

import asyncio
import hashlib
from collections.abc import Callable, Sequence

__all__ = ["MemoryBlobs"]


class MemoryBlobs:
    """An in-memory bucket for a seller's codec: claim-checks and a job's stream blobs
    (``kontra.codec.stream.StreamStore``), and the gate's reader of them (``fetch``, what
    ``kontra._relay.relay`` takes as ``blobs``). ``delay``: seconds each new stream blob takes
    to upload, one after another (a slow bucket)."""

    def __init__(self, *, delay: float = 0.0) -> None:
        self.data: dict[str, bytes] = {}
        self.scopes: dict[str, str] = {}
        self.uploads = 0
        self.delay = delay

    async def put(self, sha256: str, data: bytes) -> None:
        self.data[sha256] = data

    async def get(self, sha256: str, *, limit: int | None = None) -> bytes:
        data = self.data[sha256]
        if limit is not None and len(data) > limit:
            raise ValueError(f"larger than {limit} bytes")
        return data

    async def put_stream(
        self, job_id: str, blobs: Sequence[tuple[str, bytes]], done: Callable[[str], None]
    ) -> None:
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

    async def fetch(self, sha256: str, size: int) -> bytes | None:
        return self.data.get(sha256)
