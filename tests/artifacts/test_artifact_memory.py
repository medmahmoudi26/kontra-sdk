"""Acceptance 14: a large artifact uploads and downloads with peak RSS under 256 MB above
baseline, and the download's sha256 matches.

The size comes from ``KONTRA_ARTIFACT_TEST_BYTES`` (default 64 MiB, so it runs with the unit
tests). The acceptance run is 3 GiB::

    KONTRA_ARTIFACT_TEST_BYTES=3221225472 .venv/bin/pytest -s tests/artifacts/test_artifact_memory.py

Disk: the source, the stored ciphertext and the download would be three copies; the source is
deleted after the upload (its sha256 kept), so the peak is two.
"""

from __future__ import annotations

import gc
import os
import threading
import time
from hashlib import sha256
from pathlib import Path
from typing import Any

import psutil

from kontra.artifacts import download, upload
from kontra.codec import age

from .fake_bucket import FakeBucket, make_gate

LIMIT = 256 * 1024 * 1024
BLOCK = 1024 * 1024


class PeakRss:
    """Samples this process's RSS every 5 ms in a thread."""

    def __init__(self) -> None:
        self._proc = psutil.Process()
        self.baseline = self._proc.memory_info().rss
        self.peak = self.baseline
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def _run(self) -> None:
        while not self._stop.is_set():
            self.peak = max(self.peak, self._proc.memory_info().rss)
            time.sleep(0.005)

    def __enter__(self) -> PeakRss:
        self._thread.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self._stop.set()
        self._thread.join()
        self.peak = max(self.peak, self._proc.memory_info().rss)

    @property
    def growth(self) -> int:
        return self.peak - self.baseline


def _generate(path: Path, size: int) -> str:
    """Incompressible-looking content without holding it: a random block, stamped per block."""
    seed = os.urandom(BLOCK)
    h = sha256()
    with path.open("wb") as f:
        for i in range(0, size, BLOCK):
            block = (i // BLOCK).to_bytes(8, "big") + seed[8 : min(BLOCK, size - i)]
            block = block[: min(BLOCK, size - i)]
            f.write(block)
            h.update(block)
    return h.hexdigest()


def _hash_file(path: Path) -> str:
    h = sha256()
    with path.open("rb") as f:
        while block := f.read(BLOCK):
            h.update(block)
    return h.hexdigest()


async def test_large_artifact_constant_memory(tmp_path: Path) -> None:
    size = int(os.environ.get("KONTRA_ARTIFACT_TEST_BYTES", str(64 * 1024 * 1024)))
    _, seller_pub = age.generate_identity()
    buyer_id, buyer_pub = age.generate_identity()
    root = tmp_path / "bucket"
    root.mkdir()
    # A realistic multipart size: the client's memory must not depend on it.
    bucket = FakeBucket(root, part_size=64 * 1024 * 1024)
    gate: Any
    gate, aclose = make_gate(bucket.transport)
    src = tmp_path / "records.jsonl"
    dest = tmp_path / "records.out"
    try:
        expected = _generate(src, size)
        gc.collect()
        with PeakRss() as up_rss:
            t0 = time.monotonic()
            art = await upload(gate, src, recipients=[seller_pub, buyer_pub], job_id="ab" * 16)
            up_secs = time.monotonic() - t0
        src.unlink()
        assert art.sha256 == expected
        assert bucket.stored_bytes(art.key) == age.ciphertext_size(size, 2)

        gc.collect()
        with PeakRss() as down_rss:
            t0 = time.monotonic()
            await download(gate, art, buyer_id, dest)
            down_secs = time.monotonic() - t0
        assert _hash_file(dest) == expected

        mib = 1024 * 1024
        print(
            f"\n[acceptance 14] {size} bytes ({size / mib:.0f} MiB), "
            f"{len(bucket.artifacts[art.key].plan)} parts\n"
            f"  upload:   {up_secs:.1f}s ({size / mib / up_secs:.0f} MiB/s), "
            f"baseline {up_rss.baseline / mib:.1f} MiB, peak {up_rss.peak / mib:.1f} MiB, "
            f"growth {up_rss.growth / mib:.1f} MiB\n"
            f"  download: {down_secs:.1f}s ({size / mib / down_secs:.0f} MiB/s), "
            f"baseline {down_rss.baseline / mib:.1f} MiB, peak {down_rss.peak / mib:.1f} MiB, "
            f"growth {down_rss.growth / mib:.1f} MiB"
        )
        assert up_rss.growth < LIMIT
        assert down_rss.growth < LIMIT
    finally:
        await aclose()
        src.unlink(missing_ok=True)
        dest.unlink(missing_ok=True)
        for key in list(bucket.artifacts):
            bucket.delete(key)
