"""Artifacts: files in the claim-check bucket, age-encrypted, streamed (PRD §10.8, decisions I7).

Memory stays flat whatever the size (acceptance 14): the file is read, encrypted and sent
1 MiB at a time, and a download is decrypted, hashed and written the same way.

Upload, in order:

1. The exact ciphertext size is computed from the plaintext size and the recipient count
   (:func:`kontra.codec.age.ciphertext_size`) and sent to the gate *first*, so the gate can
   refuse an output above ``max_output_bytes`` before a single byte is stored (acceptance 15).
2. The gate answers with presigned part URLs. Each part's body is generated on the fly from
   the file: age chunks are independent given the payload key, so any byte range of the
   ciphertext can be produced again, and a failed part is retried without buffering it.
   A regenerated chunk reuses its nonce, so it must be the same bytes: each one's tag is
   checked against the first sealing, and a file that changed stops the upload.
3. ``sha256`` of the plaintext (what :class:`~kontra.types.Artifact` records and a download
   verifies) and of the ciphertext (what the gate records for disputes) are computed in the
   same pass.

Download streams the ciphertext from a presigned GET (resuming with ``Range`` after a dropped
connection), decrypts incrementally, and checks size and sha256 before the result counts:
:func:`download` writes to a temporary file and renames it only when everything verified;
:func:`iter_plaintext` raises before yielding the last piece if anything is off.
"""

from __future__ import annotations

import asyncio
import contextlib
import mimetypes
import os
import secrets
from collections.abc import AsyncIterator, Sequence
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path, PurePosixPath
from typing import BinaryIO, Protocol

import httpx

from kontra.codec import age
from kontra.contract.names import is_job_id
from kontra.gate_client import GateError
from kontra.gate_client import models as m
from kontra.types import Artifact

__all__ = [
    "ArtifactError",
    "ArtifactGate",
    "download",
    "guess_content_type",
    "iter_plaintext",
    "upload",
]

#: Bytes produced or consumed per step (16 age chunks).
_STEP = 1024 * 1024
_TAG = 16
_CT_CHUNK = age.CHUNK_SIZE + _TAG
_ATTEMPTS = 4
_BACKOFF = 0.5
#: How long a download waits for the gate to count its job's delivery. The gate gives the
#: buyer a job's outputs only once the job delivered (409 ``not_delivered`` before: until
#: then the buyer could still cancel for a refund), and it learns of a delivery by
#: observing the seller's workflow -- a sweep after the buyer's own call (``Job.result``, a
#: buyer workflow's ``execute``) already returned the artifact references. One sweep is
#: seconds; this is the bound ``kontra.hire`` gives ``accept`` after ``result``.
_DELIVERY_WAIT = 60.0
_DELIVERY_POLL_START, _DELIVERY_POLL_MAX = 0.2, 2.0

_EXTRA_TYPES = {
    ".jsonl": "application/x-ndjson",
    ".ndjson": "application/x-ndjson",
    ".parquet": "application/vnd.apache.parquet",
    ".yaml": "application/yaml",
    ".yml": "application/yaml",
    ".md": "text/markdown",
}
_COMPRESSED_TYPES = {
    "gzip": "application/gzip",
    "bzip2": "application/x-bzip2",
    "xz": "application/x-xz",
    "br": "application/x-brotli",
}


class ArtifactError(Exception):
    """An upload or download that did not complete with verified bytes."""


class ArtifactGate(Protocol):
    """The part of :class:`kontra.gate_client.GateClient` artifacts use."""

    @property
    def http(self) -> httpx.AsyncClient: ...

    async def create_artifact(self, req: m.ArtifactCreate) -> m.ArtifactUpload: ...

    async def complete_artifact(self, key: str, req: m.ArtifactComplete) -> None: ...

    async def download_artifact(self, key: str) -> m.DownloadUrl: ...


def guess_content_type(path: str | Path) -> str:
    """From the file name; ``application/octet-stream`` when unknown. A compressed file
    (``x.csv.gz``) is its compression's type: that is what the bytes are."""
    name = Path(path).name
    extra = _EXTRA_TYPES.get(Path(name).suffix.lower())
    if extra is not None:
        return extra
    guessed, encoding = mimetypes.guess_type(name, strict=False)
    if encoding is not None:
        return _COMPRESSED_TYPES.get(encoding, "application/octet-stream")
    return guessed or "application/octet-stream"


# --------------------------------------------------------------------------- upload


class _Ciphertext:
    """Random access to the ciphertext of an open file: ``read(start, stop)``.

    Hashing is monotonic: a byte (or chunk) is hashed the first time the sequential upload
    reaches it, so regenerating a range for a retried part never hashes it twice. Calls run
    in a worker thread, one at a time.

    A chunk sealed twice is sealed under the same STREAM nonce, which is only safe if its
    plaintext is the same both times: two different plaintexts under one ChaCha20-Poly1305
    nonce give away their XOR and the Poly1305 key to whoever saw both (the bucket sees a
    failed attempt's bytes too). So a chunk split by a step or part boundary is served from
    the last sealed chunk instead of being read again, and the tag of every chunk a retry
    may regenerate is kept: if the file changed under it, the re-sealed chunk's tag differs
    and the upload stops before that ciphertext leaves the process.
    """

    def __init__(self, f: BinaryIO, size: int, header: bytes, sealer: age.ChunkSealer) -> None:
        self._f, self._size, self._header, self._sealer = f, size, header, sealer
        self._chunks = max(1, -(-size // age.CHUNK_SIZE))
        self.total = len(header) + size + _TAG * self._chunks
        self._plain = sha256()
        self._plain_next = 0
        self._cipher = sha256()
        self._cipher_pos = 0
        self._last: tuple[int, bytes] | None = None
        #: Tags of chunks ``_tags_from`` onwards: those of the part being uploaded.
        self._tags = bytearray()
        self._tags_from = 0

    def _seal(self, index: int) -> bytes:
        if self._last is not None and self._last[0] == index:
            return self._last[1]
        offset = index * age.CHUNK_SIZE
        want = min(age.CHUNK_SIZE, self._size - offset)
        self._f.seek(offset)
        data = self._f.read(want)
        if len(data) != want:
            raise ArtifactError("the file shrank while it was being uploaded")
        sealed = self._sealer.seal(index, data, last=index == self._chunks - 1)
        tag, slot = sealed[-_TAG:], (index - self._tags_from) * _TAG
        if slot < 0 or slot > len(self._tags):
            raise ArtifactError("internal error: chunk sealed out of order")
        if slot < len(self._tags):
            if bytes(self._tags[slot : slot + _TAG]) != tag:
                raise ArtifactError("the file changed while it was being uploaded")
        else:
            self._tags += tag
        if index == self._plain_next:
            self._plain.update(data)
            self._plain_next += 1
        self._last = (index, sealed)
        return sealed

    def stored(self, end: int) -> None:
        """Everything before ``end`` is stored: a retry never reaches back past it, so the
        tags of the chunks wholly before it can go (memory stays one part's worth)."""
        first = max(0, end - len(self._header)) // _CT_CHUNK
        drop = first - self._tags_from
        if drop > 0:
            del self._tags[: drop * _TAG]
            self._tags_from = first

    def read(self, start: int, stop: int) -> bytes:
        out = bytearray()
        head = len(self._header)
        pos = start
        if pos < head:
            out += self._header[pos : min(stop, head)]
            pos = min(stop, head)
        while pos < stop:
            index = (pos - head) // _CT_CHUNK
            chunk_start = head + index * _CT_CHUNK
            sealed = self._seal(index)
            take = min(stop, chunk_start + len(sealed))
            out += sealed[pos - chunk_start : take - chunk_start]
            pos = take
        end = start + len(out)
        if end > self._cipher_pos:
            if start > self._cipher_pos:
                raise ArtifactError("internal error: ciphertext range skipped")
            with memoryview(out) as view:
                self._cipher.update(view[self._cipher_pos - start :])
            self._cipher_pos = end
        return bytes(out)

    def digests(self) -> tuple[str, str]:
        """``(plaintext sha256, ciphertext sha256)``, once everything was read."""
        if self._plain_next != self._chunks or self._cipher_pos != self.total:
            raise ArtifactError("internal error: upload did not cover the whole file")
        return self._plain.hexdigest(), self._cipher.hexdigest()


@dataclass(frozen=True, slots=True)
class _Part:
    number: int
    url: str
    offset: int
    size: int


def _plan(up: m.ArtifactUpload, total: int) -> list[_Part]:
    parts = sorted(up.parts, key=lambda p: p.number)
    if not parts or (up.upload_id is None and len(parts) != 1):
        raise ArtifactError("the gate returned an unusable upload plan")
    out: list[_Part] = []
    offset = 0
    for i, p in enumerate(parts):
        if p.size <= 0 or (i and p.number <= parts[i - 1].number):
            raise ArtifactError("the gate returned an unusable upload plan")
        out.append(_Part(p.number, p.url, offset, p.size))
        offset += p.size
    if offset != total:
        raise ArtifactError(
            f"the gate's upload plan covers {offset} bytes, the ciphertext is {total}"
        )
    return out


def _retryable(status: int) -> bool:
    return status in (408, 429) or status >= 500


async def _put_part(http: httpx.AsyncClient, part: _Part, src: _Ciphertext) -> str:
    async def body() -> AsyncIterator[bytes]:
        pos, end = part.offset, part.offset + part.size
        while pos < end:
            stop = min(end, pos + _STEP)
            yield await asyncio.to_thread(src.read, pos, stop)
            pos = stop

    problem = ""
    for attempt in range(_ATTEMPTS):
        if attempt:
            await asyncio.sleep(_BACKOFF * 2 ** (attempt - 1))
        try:
            resp = await http.put(
                part.url, content=body(), headers={"Content-Length": str(part.size)}
            )
        except httpx.TransportError as e:
            problem = f"{type(e).__name__}: {e}"
            continue
        if resp.is_success:
            src.stored(part.offset + part.size)
            return resp.headers.get("etag", "")
        problem = f"HTTP {resp.status_code}"
        if not _retryable(resp.status_code):
            break
    raise ArtifactError(f"uploading part {part.number} failed: {problem}")


async def upload(
    gate: ArtifactGate,
    path: str | Path,
    *,
    recipients: Sequence[str],
    content_type: str | None = None,
    job_id: str | None = None,
) -> Artifact:
    """Encrypt ``path`` to ``recipients`` and upload it, streaming. ``job_id`` marks a
    seller's output for that job (counted against ``max_output_bytes``); a buyer's input
    has none. ``content_type`` defaults to a guess from the extension."""
    path = Path(path)
    ctype = content_type if content_type is not None else guess_content_type(path)
    if not 1 <= len(ctype) <= 255:
        raise ValueError("content_type must be 1 to 255 characters")
    if job_id is not None and not is_job_id(job_id):
        raise ValueError(f"{job_id!r} is not a job id")
    header, sealer = age.new_header(recipients)
    f = await asyncio.to_thread(path.open, "rb")
    try:
        before = os.fstat(f.fileno())
        src = _Ciphertext(f, before.st_size, header, sealer)
        up = await gate.create_artifact(
            m.ArtifactCreate(size=src.total, content_type=ctype, job_id=job_id)
        )
        etags = [await _put_part(gate.http, part, src) for part in _plan(up, src.total)]
        after = os.fstat(f.fileno())
    finally:
        f.close()
    if (after.st_size, after.st_mtime_ns) != (before.st_size, before.st_mtime_ns):
        raise ArtifactError("the file changed while it was being uploaded")
    if up.upload_id is not None and not all(etags):
        raise ArtifactError("the bucket returned no ETag for a part")
    plain_sha, cipher_sha = src.digests()
    await gate.complete_artifact(
        up.key,
        m.ArtifactComplete(upload_id=up.upload_id, etags=etags, ciphertext_sha256=cipher_sha),
    )
    return Artifact(key=up.key, size=before.st_size, sha256=plain_sha, content_type=ctype)


# --------------------------------------------------------------------------- download


async def _download_url(gate: ArtifactGate, key: str) -> m.DownloadUrl:
    """The gate's GET URL for ``key``, waiting (bounded) while the gate has not yet counted
    the delivery of the artifact's job. Any other refusal is final at once."""
    loop = asyncio.get_running_loop()
    end = loop.time() + _DELIVERY_WAIT
    delay = _DELIVERY_POLL_START
    while True:
        try:
            return await gate.download_artifact(key)
        except GateError as e:
            if (e.status, e.code) != (409, "not_delivered") or loop.time() + delay > end:
                raise
        await asyncio.sleep(delay)
        delay = min(delay * 2, _DELIVERY_POLL_MAX)


async def _ciphertext(gate: ArtifactGate, key: str) -> AsyncIterator[bytes]:
    """The stored ciphertext, in order, resuming after a dropped connection."""
    info = await _download_url(gate, key)
    total, received, problem = info.size, 0, ""
    for attempt in range(_ATTEMPTS):
        if attempt:
            await asyncio.sleep(_BACKOFF * 2 ** (attempt - 1))
            info = await gate.download_artifact(key)  # a fresh URL, in case it expired
        headers = {"Range": f"bytes={received}-"} if received else {}
        try:
            async with gate.http.stream("GET", info.url, headers=headers) as resp:
                if received and resp.status_code == 206:
                    skip = 0
                elif resp.status_code == 200:
                    skip = received  # the server ignored Range: drop what we have
                else:
                    problem = f"HTTP {resp.status_code}"
                    if _retryable(resp.status_code):
                        continue
                    break
                async for piece in resp.aiter_bytes(_STEP):
                    if skip:
                        dropped = min(skip, len(piece))
                        piece, skip = piece[dropped:], skip - dropped
                        if not piece:
                            continue
                    received += len(piece)
                    if received > total:
                        raise ArtifactError("the stored object is larger than announced")
                    yield piece
        except httpx.TransportError as e:
            problem = f"{type(e).__name__}: {e}"
            continue
        if received == total:
            return
        problem = f"connection ended at {received} of {total} bytes"
    raise ArtifactError(f"downloading artifact {key} failed: {problem}")


async def iter_plaintext(
    gate: ArtifactGate, artifact: Artifact, identity: str | Sequence[str]
) -> AsyncIterator[bytes]:
    """The decrypted plaintext, piece by piece, in constant memory. Size and sha256 are
    checked before the last piece is yielded; if they (or the decryption) fail this raises
    :class:`ArtifactError`, and everything yielded so far must be discarded."""
    dec = age.Decryptor(identity)
    digest = sha256()
    seen = 0

    def step(piece: bytes) -> bytes:
        out = dec.update(piece)
        digest.update(out)
        return out

    try:
        async for piece in _ciphertext(gate, artifact.key):
            out = await asyncio.to_thread(step, piece)
            seen += len(out)
            if seen > artifact.size:
                raise ArtifactError("artifact is larger than its recorded size")
            if out:
                yield out
        last = dec.finalize()
    except age.AgeError as e:
        raise ArtifactError(f"artifact {artifact.key} failed to decrypt: {e}") from e
    digest.update(last)
    seen += len(last)
    if seen != artifact.size:
        raise ArtifactError(f"artifact size mismatch: expected {artifact.size}, got {seen}")
    if digest.hexdigest() != artifact.sha256:
        raise ArtifactError("artifact sha256 mismatch")
    if last:
        yield last


def _create_private(path: Path) -> BinaryIO:
    """A new file at ``path``, open for writing and readable by us alone: it receives
    decrypted plaintext, which the process umask (typically 0644) would leave readable by
    every user of the machine. 0600 from creation, not chmod-ed afterwards (no window), and
    exactly 0600 whatever the umask. ``O_EXCL``/``O_NOFOLLOW``: never an existing file or a
    link planted at the name."""
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(path, flags | getattr(os, "O_CLOEXEC", 0), 0o600)
    try:
        if hasattr(os, "fchmod"):
            os.fchmod(fd, 0o600)
        return os.fdopen(fd, "wb")
    except BaseException:
        os.close(fd)
        raise


async def download(
    gate: ArtifactGate, artifact: Artifact, identity: str | Sequence[str], dest: str | Path
) -> Path:
    """Fetch, decrypt and verify into ``dest`` (a file path, or an existing directory to
    put the key's last segment in). Nothing appears at ``dest`` unless it verified, and
    what appears is readable by us alone (0600): it is the decrypted plaintext."""
    dest = Path(dest)
    if dest.is_dir():
        # The key comes from the counterparty's result: never let it pick "..", or a
        # dotfile such as ".bashrc" when ``dest`` is a home directory.
        name = PurePosixPath(artifact.key).name
        if not name or name.startswith("."):
            raise ArtifactError(
                f"artifact key {artifact.key!r} does not name a plain file; pass a file path"
            )
        dest = dest / name
    tmp = dest.with_name(f".{dest.name}.{secrets.token_hex(4)}.part")
    try:
        f = await asyncio.to_thread(_create_private, tmp)
        try:
            async for piece in iter_plaintext(gate, artifact, identity):
                await asyncio.to_thread(f.write, piece)
        finally:
            f.close()
        tmp.replace(dest)
    except BaseException:
        with contextlib.suppress(OSError):
            tmp.unlink()
        raise
    return dest
