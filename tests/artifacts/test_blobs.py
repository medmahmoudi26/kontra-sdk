"""GateBlobStore: the codec's claim-check on the gate's presigned blob URLs (``POST /v1/blobs``,
``GET /v1/blobs/{sha256}``), against the fake gate + bucket and the real ``GateClient``."""

from __future__ import annotations

import json
from typing import Any

import pytest
from temporalio.api.common.v1 import Payload

from kontra.codec import CLAIM_CHECK_ENCODING, CodecError, GateBlobStore, KontraCodec
from kontra.codec.age import decrypt

from .fake_bucket import BUCKET, FakeBucket

MARKER = b"KONTRA-BLOB-MARKER-2d81"


def _big() -> Payload:
    return Payload(metadata={"encoding": b"json/plain"}, data=b'"' + MARKER * 500 + b'"')


def _codec(party: tuple[str, str], gate: Any) -> KontraCodec:
    return KontraCodec(
        party[0], [party[1]], role="seller", blobs=GateBlobStore(gate), claim_check_threshold=1024
    )


async def test_claim_check_round_trip_through_the_gate(
    gate: Any, bucket: FakeBucket, seller: Any
) -> None:
    codec = _codec(seller, gate)
    [ref] = await codec.encode([_big()])
    assert ref.metadata["encoding"] == CLAIM_CHECK_ENCODING
    digest = json.loads(ref.data)["sha256"]
    stored = bucket.blobs[digest]
    assert MARKER not in stored  # the bucket holds ciphertext only
    assert Payload.FromString(decrypt(stored, seller[0])) == _big()
    assert await codec.decode([ref]) == [_big()]
    # The bytes went to and came from the presigned bucket URLs.
    assert ("PUT", f"{BUCKET}/blob/{digest}") in bucket.requests
    assert ("GET", f"{BUCKET}/blob/{digest}") in bucket.requests


async def test_existing_blob_is_not_uploaded_again(
    gate: Any, bucket: FakeBucket, seller: Any
) -> None:
    store = GateBlobStore(gate)
    await store.put("a" * 64, b"x")
    puts = [r for r in bucket.requests if r[0] == "PUT"]
    await store.put("a" * 64, b"x")
    assert [r for r in bucket.requests if r[0] == "PUT"] == puts


async def test_failed_upload_is_a_codec_error(gate: Any, bucket: FakeBucket, seller: Any) -> None:
    bucket.blob_put_status = 503
    with pytest.raises(CodecError, match="upload failed: HTTP 503"):
        await _codec(seller, gate).encode([_big()])


async def test_missing_or_altered_blob_is_refused(
    gate: Any, bucket: FakeBucket, seller: Any
) -> None:
    codec = _codec(seller, gate)
    [ref] = await codec.encode([_big()])
    digest = json.loads(ref.data)["sha256"]
    data = bucket.blobs[digest]
    bucket.blobs[digest] = data[:-1] + bytes([data[-1] ^ 1])
    with pytest.raises(CodecError, match="does not match"):
        await codec.decode([ref])
    del bucket.blobs[digest]
    with pytest.raises(CodecError, match="download failed"):
        await codec.decode([ref])
