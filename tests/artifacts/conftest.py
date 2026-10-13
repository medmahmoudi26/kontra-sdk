from __future__ import annotations

from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import pytest

from kontra.codec.age import generate_identity

from .fake_bucket import FakeBucket, make_gate


@pytest.fixture
def seller() -> tuple[str, str]:
    return generate_identity()


@pytest.fixture
def buyer() -> tuple[str, str]:
    return generate_identity()


@pytest.fixture
def bucket(tmp_path: Path) -> FakeBucket:
    root = tmp_path / "bucket"
    root.mkdir()
    # Parts deliberately not a multiple of the 64 KiB age chunk, so parts split chunks.
    return FakeBucket(root, part_size=200_000)


@pytest.fixture
async def gate(bucket: FakeBucket) -> AsyncIterator[Any]:
    client, aclose = make_gate(bucket.transport)
    try:
        yield client
    finally:
        await aclose()
