"""Temporal through the façade: kontra.workflow and kontra.activity only (D1). The file is written on a thread (the worker's
event loop keeps serving), hashed as it is written, and uploaded with `Artifact.from_path`,
which streams it: memory stays flat whatever the size."""

import asyncio
import hashlib
import random
import tempfile
from pathlib import Path

from models import Artifact, BulkInput, BulkOutput

from kontra import activity

BLOCK = 1 << 20


def _write(path: Path, size: int, seed: int) -> str:
    block = random.Random(seed).randbytes(BLOCK)
    digest = hashlib.sha256()
    with path.open("wb") as f:
        left, index = size, 0
        while left > 0:
            chunk = (index.to_bytes(8, "big") + block[8:])[:left]
            f.write(chunk)
            digest.update(chunk)
            left -= len(chunk)
            index += 1
    return digest.hexdigest()


@activity.defn
async def produce(inp: BulkInput) -> BulkOutput:
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "bulk.bin"
        sha256 = await asyncio.to_thread(_write, path, inp.size, inp.seed)
        try:
            artifact = await Artifact.from_path(path, content_type="application/octet-stream")
        except Exception as err:  # the gate refuses an output beyond max_output_bytes
            return BulkOutput(
                files=[], size=inp.size, sha256=sha256, refused=f"{type(err).__name__}: {err}"
            )
    return BulkOutput(files=[artifact], size=inp.size, sha256=sha256, refused="")
