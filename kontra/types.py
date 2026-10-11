"""Types a seller may use in ``models.py``. ``Artifact`` is the only Kontra touch allowed
in seller models (D1, §10.8): a reference to bytes in the claim-check bucket, never the
bytes themselves.

The methods reach the process's Kontra runtime (``kontra._runtime``), installed by
``kontra.serve.Worker`` on the seller side and by ``kontra.hire.Kontra`` on the buyer side.
Workflow code never calls them: uploading is I/O, so it happens in activities.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field

__all__ = ["Artifact"]


class Artifact(BaseModel):
    """A file in the claim-check bucket, encrypted (age) to the job's parties.

    ``size`` and ``sha256`` are of the *plaintext*; a download verifies both. The
    contract type is ``#/$defs/KontraArtifact`` (``spec/kontra-types.v1.json``); the two
    must stay field-for-field identical, which ``tests/contract/test_spec.py`` checks.
    """

    model_config = ConfigDict(extra="forbid", frozen=True, title="KontraArtifact")

    #: Opaque, assigned by the gate: 32 lowercase hex characters. Never a path (nothing to
    #: traverse when it names a downloaded file) and never parsed for meaning.
    key: str = Field(min_length=32, max_length=32, pattern=r"^[0-9a-f]{32}$")
    size: int = Field(ge=0)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    content_type: str = Field(min_length=1, max_length=255)

    @classmethod
    async def from_path(
        cls,
        path: str | Path,
        *,
        content_type: str | None = None,
        recipients: list[str] | None = None,
    ) -> Artifact:
        """Encrypt and upload a local file, streaming, in constant memory.

        In a seller activity the recipients are the job's seller and buyer and the
        upload counts against the offering's ``max_output_bytes``. On the buyer side pass
        ``recipients`` (``Kontra.upload`` does this for you).
        """
        from kontra import _runtime
        from kontra.artifacts import upload

        rt = _runtime.current()
        return await upload(
            rt.gate,
            Path(path),
            recipients=recipients or await rt.recipients_for_current_job(),
            content_type=content_type,
            job_id=rt.current_job_id(),
        )

    async def download(self, path: str | Path) -> Path:
        """Fetch, decrypt and verify into ``path``. Returns the path."""
        from kontra import _runtime
        from kontra.artifacts import download

        rt = _runtime.current()
        return await download(rt.gate, self, rt.identity, Path(path))

    async def chunks(self) -> AsyncIterator[bytes]:
        """Decrypted plaintext, chunk by chunk. The sha256 is verified at the end; a
        mismatch raises after the last chunk."""
        from kontra import _runtime
        from kontra.artifacts import iter_plaintext

        rt = _runtime.current()
        async for chunk in iter_plaintext(rt.gate, self, rt.identity):
            yield chunk

    async def lines(self, encoding: str = "utf-8") -> AsyncIterator[str]:
        """Decrypted text, line by line (newline stripped)."""
        buf = b""
        async for chunk in self.chunks():
            buf += chunk
            *complete, buf = buf.split(b"\n")
            for line in complete:
                yield line.decode(encoding)
        if buf:
            yield buf.decode(encoding)

    async def read_bytes(self) -> bytes:
        """The whole plaintext in memory. For small artifacts only."""
        return b"".join([c async for c in self.chunks()])
