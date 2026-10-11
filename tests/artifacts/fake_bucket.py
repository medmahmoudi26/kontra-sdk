"""An in-process gate (artifact and blob routes only) plus the bucket behind its presigned
URLs, as an httpx transport. Parts and blobs live on disk under ``root``, so a multi-GB
artifact costs disk, not memory, and the RSS the tests measure is the client's.

``httpx.MockTransport`` reads every request body into memory before calling its handler;
:class:`StreamingMockTransport` hands the handler the unread stream instead, which is how a
real socket behaves and what a 3 GiB upload needs.

Fault injection: ``fail_parts`` (part numbers whose first PUT fails: ``"500"`` answers 500,
``"drop"`` reads half the body and then raises a transport error), ``cut_download_at``
(the first GET dies after that many bytes), ``ignore_range`` (answer 200 to a Range GET),
``corrupt`` (flip one stored ciphertext byte at an offset), ``on_part`` (a callback before a
part is stored). ``record_bodies`` keeps every part PUT's body as received, failed attempts
included (small objects only): what an observer at the bucket gets to see.
"""

from __future__ import annotations

import inspect
import secrets
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from typing import Any

import httpx

from kontra.gate_client import GateClient
from kontra.gate_client import models as m
from kontra.gate_client.routes import BY_NAME

GATE = "http://gate.test"
BUCKET = "http://bucket.test"
_STEP = 1024 * 1024


class StreamingMockTransport(httpx.MockTransport):
    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        response = self.handler(request)  # type: ignore[attr-defined]
        if inspect.isawaitable(response):
            response = await response
        return response


@dataclass
class StoredArtifact:
    key: str
    size: int
    content_type: str
    job_id: str | None
    upload_id: str | None
    plan: list[int]
    etags: dict[int, str] = field(default_factory=dict)
    completed: bool = False
    ciphertext_sha256: str | None = None


def _route_path(name: str, **params: str) -> str:
    return BY_NAME[name].path.format(**params)


def _json(status: int, model: Any) -> httpx.Response:
    return httpx.Response(status, json=model.model_dump(mode="json"))


def _error(status: int, code: str, message: str) -> httpx.Response:
    return _json(status, m.ErrorBody(code=code, message=message))


class FakeBucket:
    def __init__(
        self,
        root: Path,
        *,
        part_size: int = 8 * 1024 * 1024,
        max_output_bytes: int | None = None,
    ) -> None:
        self.root = root
        self.part_size = part_size
        self.max_output_bytes = max_output_bytes
        self.artifacts: dict[str, StoredArtifact] = {}
        self.blobs: dict[str, bytes] = {}
        self.requests: list[tuple[str, str]] = []
        self.fail_parts: dict[int, str] = {}
        self.cut_download_at: int | None = None
        self.ignore_range = False
        self.blob_put_status = 200
        #: Called with the part number before a part is stored (to mutate the source file).
        self.on_part: Callable[[int], None] | None = None
        self.record_bodies = False
        #: ``(part number, body bytes received)`` per PUT attempt, if ``record_bodies``.
        self.bodies: list[tuple[int, bytes]] = []
        self.transport = StreamingMockTransport(self._handle)

    # ----------------------------------------------------------------- storage

    def _part_path(self, key: str, number: int) -> Path:
        return self.root / key.replace("/", "_") / f"{number:05d}"

    def stored_bytes(self, key: str) -> int:
        a = self.artifacts[key]
        return sum(
            self._part_path(key, n).stat().st_size
            for n in range(1, len(a.plan) + 1)
            if self._part_path(key, n).exists()
        )

    def read_object(self, key: str) -> bytes:
        """The whole stored ciphertext (small objects only)."""
        a = self.artifacts[key]
        return b"".join(self._part_path(key, n).read_bytes() for n in range(1, len(a.plan) + 1))

    def corrupt(self, key: str, offset: int) -> None:
        a = self.artifacts[key]
        for n, size in enumerate(a.plan, start=1):
            if offset < size:
                p = self._part_path(key, n)
                data = bytearray(p.read_bytes())
                data[offset] ^= 0x01
                p.write_bytes(bytes(data))
                return
            offset -= size
        raise ValueError("offset beyond object")

    def delete(self, key: str) -> None:
        a = self.artifacts.pop(key)
        for n in range(1, len(a.plan) + 1):
            self._part_path(key, n).unlink(missing_ok=True)

    # ----------------------------------------------------------------- routing

    async def _handle(self, request: httpx.Request) -> httpx.Response:
        url = request.url
        self.requests.append((request.method, str(url)))
        base = f"{url.scheme}://{url.host}"
        path = url.path
        if base == GATE:
            return await self._gate(request, path)
        if base == BUCKET:
            return await self._bucket(request, path)
        return httpx.Response(404)

    async def _gate(self, request: httpx.Request, path: str) -> httpx.Response:
        method = request.method
        if method == "POST" and path == _route_path("create_artifact"):
            return self._create(m.ArtifactCreate.model_validate_json(await request.aread()))
        if method == "POST" and path.startswith("/v1/artifacts/") and path.endswith("/complete"):
            key = path[len("/v1/artifacts/") : -len("/complete")]
            assert path == _route_path("complete_artifact", key=key)
            return self._complete(
                key, m.ArtifactComplete.model_validate_json(await request.aread())
            )
        if method == "GET" and path.startswith("/v1/artifacts/"):
            key = path[len("/v1/artifacts/") :]
            assert path == _route_path("download_artifact", key=key)
            a = self.artifacts.get(key)
            if a is None or not a.completed:
                return _error(404, "not_found", "no such artifact")
            return _json(
                200,
                m.DownloadUrl(url=f"{BUCKET}/get/{key}", size=a.size, expires_at=_soon()),
            )
        if method == "POST" and path == _route_path("create_blob"):
            req = m.BlobCreate.model_validate_json(await request.aread())
            return _json(
                200,
                m.BlobUrl(
                    sha256=req.sha256,
                    url=f"{BUCKET}/blob/{req.sha256}",
                    exists=req.sha256 in self.blobs,
                    expires_at=_soon(),
                ),
            )
        if method == "GET" and path.startswith("/v1/blobs/"):
            digest = path[len("/v1/blobs/") :]
            assert path == _route_path("get_blob", sha256=digest)
            if digest not in self.blobs:
                return _error(404, "not_found", "no such blob")
            return _json(
                200,
                m.BlobUrl(
                    sha256=digest, url=f"{BUCKET}/blob/{digest}", exists=True, expires_at=_soon()
                ),
            )
        return _error(404, "not_found", f"{method} {path}")

    def _create(self, req: m.ArtifactCreate) -> httpx.Response:
        if self.max_output_bytes is not None and req.job_id and req.size > self.max_output_bytes:
            return _error(413, "output_too_large", "above the offering's max_output_bytes")
        key = secrets.token_hex(16)
        if req.size <= self.part_size:
            plan, upload_id = [req.size], None
        else:
            full, rest = divmod(req.size, self.part_size)
            plan = [self.part_size] * full + ([rest] if rest else [])
            upload_id = secrets.token_hex(8)
        self.artifacts[key] = StoredArtifact(
            key, req.size, req.content_type, req.job_id, upload_id, plan
        )
        parts = [
            m.UploadPart(number=n, url=f"{BUCKET}/put/{key}/{n}", size=size)
            for n, size in enumerate(plan, start=1)
        ]
        return _json(
            200,
            m.ArtifactUpload(
                key=key,
                upload_id=upload_id,
                part_size=self.part_size,
                parts=parts,
                expires_at=_soon(),
            ),
        )

    def _complete(self, key: str, req: m.ArtifactComplete) -> httpx.Response:
        a = self.artifacts.get(key)
        if a is None:
            return _error(404, "not_found", "no such artifact")
        if req.upload_id != a.upload_id:
            return _error(400, "bad_upload", "upload id mismatch")
        expected = [a.etags.get(n, "") for n in range(1, len(a.plan) + 1)]
        if req.etags != expected:
            return _error(400, "bad_upload", "etags do not match the stored parts")
        if self.stored_bytes(key) != a.size:
            return _error(400, "bad_upload", "parts missing")
        h = sha256()
        for n in range(1, len(a.plan) + 1):
            with self._part_path(key, n).open("rb") as f:
                while block := f.read(_STEP):
                    h.update(block)
        if h.hexdigest() != req.ciphertext_sha256:
            return _error(400, "bad_upload", "ciphertext sha256 mismatch")
        a.completed, a.ciphertext_sha256 = True, req.ciphertext_sha256
        return httpx.Response(204)

    async def _bucket(self, request: httpx.Request, path: str) -> httpx.Response:
        if request.method == "PUT" and path.startswith("/put/"):
            key, _, number = path[len("/put/") :].rpartition("/")
            return await self._put_part(request, key, int(number))
        if request.method == "GET" and path.startswith("/get/"):
            return self._get(request, path[len("/get/") :])
        if path.startswith("/blob/"):
            digest = path[len("/blob/") :]
            if request.method == "PUT":
                body = await request.aread()
                if self.blob_put_status != 200:
                    return httpx.Response(self.blob_put_status)
                self.blobs[digest] = body
                return httpx.Response(200)
            if request.method == "GET":
                if digest not in self.blobs:
                    return httpx.Response(404)
                return httpx.Response(200, content=self.blobs[digest])
        return httpx.Response(404)

    async def _put_part(self, request: httpx.Request, key: str, number: int) -> httpx.Response:
        a = self.artifacts[key]
        planned = a.plan[number - 1]
        if request.headers.get("content-length") != str(planned):
            return httpx.Response(400, text="content-length must be the planned part size")
        if self.on_part is not None:
            self.on_part(number)
        fault = self.fail_parts.pop(number, None)
        if fault == "500":
            return httpx.Response(500)
        p = self._part_path(key, number)
        p.parent.mkdir(parents=True, exist_ok=True)
        h = sha256()
        written = 0
        seen = bytearray()
        assert isinstance(request.stream, httpx.AsyncByteStream)
        try:
            with p.open("wb") as f:
                async for block in request.stream:
                    if self.record_bodies:
                        seen += block
                    if fault == "drop" and written + len(block) > planned // 2:
                        raise httpx.WriteError("connection reset by fake bucket")
                    f.write(block)
                    h.update(block)
                    written += len(block)
        finally:
            if self.record_bodies:
                self.bodies.append((number, bytes(seen)))
        if written != planned:
            p.unlink()
            return httpx.Response(400, text="body length differs from content-length")
        etag = f'"{h.hexdigest()[:32]}"'
        a.etags[number] = etag
        return httpx.Response(200, headers={"ETag": etag})

    def _get(self, request: httpx.Request, key: str) -> httpx.Response:
        a = self.artifacts.get(key)
        if a is None or not a.completed:
            return httpx.Response(404)
        start = 0
        rng = request.headers.get("range")
        if rng and not self.ignore_range:
            assert rng.startswith("bytes=")
            assert rng.endswith("-")
            start = int(rng[len("bytes=") : -1])
        cut = self.cut_download_at
        self.cut_download_at = None
        status = 206 if start else 200
        headers = {"Content-Length": str(a.size - start)}
        return httpx.Response(status, headers=headers, content=self._stream(key, start, cut))

    async def _stream(self, key: str, start: int, cut: int | None) -> AsyncIterator[bytes]:
        a = self.artifacts[key]
        sent = 0
        offset = 0
        for n, size in enumerate(a.plan, start=1):
            if offset + size <= start:
                offset += size
                continue
            with self._part_path(key, n).open("rb") as f:
                f.seek(max(0, start - offset))
                while block := f.read(_STEP):
                    if cut is not None and sent + len(block) > cut:
                        yield block[: cut - sent]
                        raise httpx.RemoteProtocolError("peer closed connection (fake)")
                    sent += len(block)
                    yield block
            offset += size


def _soon() -> datetime:
    return datetime.now(UTC) + timedelta(hours=1)


def make_gate(transport: httpx.AsyncBaseTransport) -> tuple[GateClient, Callable[[], Any]]:
    """``(gate, aclose)``: the real gate client on the fake's transport."""
    client = GateClient(GATE, transport=transport)
    return client, client.aclose
