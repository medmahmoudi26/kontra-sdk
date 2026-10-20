"""Vanilla Temporal: temporalio only, no kontra import (decisions I29). The input artifact is read and written back,
with a trailer, as the output artifact; the note comes back as it was."""

import tempfile
from pathlib import Path

from models import Artifact, VaultInput, VaultOutput
from temporalio import activity


@activity.defn
async def reseal(inp: VaultInput) -> VaultOutput:
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "data.bin"
        with path.open("wb") as f:
            async for chunk in inp.data.chunks():
                f.write(chunk)
            f.write(b"\nresealed by the seller\n")
        out = await Artifact.from_path(path, content_type="application/octet-stream")
    return VaultOutput(data=out, note=inp.note)
