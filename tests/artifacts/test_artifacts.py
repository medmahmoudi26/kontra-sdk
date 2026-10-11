"""kontra.artifacts against the fake gate + bucket: streaming upload/download, the exact
size announced up front, retries, resume, and every way a download must refuse to count."""

from __future__ import annotations

import os
from hashlib import sha256
from pathlib import Path
from typing import Any

import pyrage
import pytest

from kontra.artifacts import ArtifactError, download, guess_content_type, iter_plaintext, upload
from kontra.codec import age
from kontra.gate_client import GateError
from kontra.types import Artifact

from .fake_bucket import BUCKET, FakeBucket

JOB = "0123456789abcdef0123456789abcdef"


def _write(path: Path, size: int) -> bytes:
    data = os.urandom(size)
    path.write_bytes(data)
    return data


async def _roundtrip(
    gate: Any, bucket: FakeBucket, tmp_path: Path, size: int, seller: Any, buyer: Any
) -> None:
    src = tmp_path / f"in-{size}.bin"
    data = _write(src, size)
    art = await upload(gate, src, recipients=[seller[1], buyer[1]], job_id=JOB)
    assert art.size == size
    assert art.sha256 == sha256(data).hexdigest()
    assert art.content_type == "application/octet-stream"

    stored = bucket.artifacts[art.key]
    # The size announced before any byte was stored is exactly what was stored.
    assert stored.size == age.ciphertext_size(size, 2) == bucket.stored_bytes(art.key)
    assert stored.completed
    assert stored.job_id == JOB
    blob = bucket.read_object(art.key)
    assert stored.ciphertext_sha256 == sha256(blob).hexdigest()
    # A party can decrypt a delivery with any age implementation (disputes, I7).
    assert pyrage.decrypt(blob, [pyrage.x25519.Identity.from_str(buyer[0])]) == data

    for identity in (buyer[0], seller[0]):
        out = await download(gate, art, identity, tmp_path / f"out-{size}-{identity[-4:]}")
        assert out.read_bytes() == data
    assert b"".join([c async for c in iter_plaintext(gate, art, buyer[0])]) == data


@pytest.mark.parametrize(
    "size",
    [0, 1, 65_535, 65_536, 65_537, 131_072, 199_999, 200_000, 200_001, 1_000_003],
)
async def test_roundtrip_sizes(
    gate: Any, bucket: FakeBucket, tmp_path: Path, size: int, seller: Any, buyer: Any
) -> None:
    await _roundtrip(gate, bucket, tmp_path, size, seller, buyer)


async def test_multipart_plan_is_used(
    gate: Any, bucket: FakeBucket, tmp_path: Path, seller: Any
) -> None:
    src = tmp_path / "x.bin"
    _write(src, 1_000_000)
    art = await upload(gate, src, recipients=[seller[1]])
    stored = bucket.artifacts[art.key]
    assert stored.upload_id is not None
    assert len(stored.plan) == 6  # 1_000_000 + overhead over 200_000-byte parts
    puts = [u for meth, u in bucket.requests if meth == "PUT"]
    assert puts == [f"{BUCKET}/put/{art.key}/{n}" for n in range(1, 7)]
    assert stored.job_id is None


async def test_limit_refused_before_any_byte_is_stored(
    gate: Any, bucket: FakeBucket, tmp_path: Path, seller: Any, buyer: Any
) -> None:
    src = tmp_path / "big.bin"
    _write(src, 5000)
    bucket.max_output_bytes = 5000  # plaintext fits, ciphertext does not
    with pytest.raises(GateError) as err:
        await upload(gate, src, recipients=[seller[1], buyer[1]], job_id=JOB)
    assert err.value.status == 413
    assert not [r for r in bucket.requests if r[0] == "PUT"]
    assert not bucket.artifacts
    assert not any(bucket.root.iterdir())


@pytest.mark.parametrize("fault", ["500", "drop"])
async def test_failed_part_is_retried_without_double_hashing(
    gate: Any, bucket: FakeBucket, tmp_path: Path, fault: str, seller: Any, buyer: Any
) -> None:
    bucket.fail_parts = {2: fault, 4: fault}
    src = tmp_path / "r.bin"
    data = _write(src, 900_000)
    art = await upload(gate, src, recipients=[seller[1], buyer[1]])
    assert art.sha256 == sha256(data).hexdigest()
    assert bucket.fail_parts == {}
    # complete_artifact verified the ciphertext sha256 against the stored bytes.
    assert bucket.artifacts[art.key].completed
    assert (await download(gate, art, buyer[0], tmp_path / "r.out")).read_bytes() == data


async def test_part_failing_every_time_gives_up(
    gate: Any, bucket: FakeBucket, tmp_path: Path, seller: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    import kontra.artifacts as arts

    monkeypatch.setattr(arts, "_BACKOFF", 0.0)
    src = tmp_path / "f.bin"
    _write(src, 500_000)

    def always_fail(number: int) -> None:
        bucket.fail_parts[number] = "500"

    bucket.on_part = always_fail
    with pytest.raises(ArtifactError, match="part 1 failed: HTTP 500"):
        await upload(gate, src, recipients=[seller[1]])
    assert not any(a.completed for a in bucket.artifacts.values())


@pytest.mark.parametrize("ignore_range", [False, True])
async def test_download_resumes_after_dropped_connection(
    gate: Any, bucket: FakeBucket, tmp_path: Path, ignore_range: bool, seller: Any
) -> None:
    src = tmp_path / "d.bin"
    data = _write(src, 1_500_000)
    art = await upload(gate, src, recipients=[seller[1]])
    bucket.cut_download_at = 700_001
    bucket.ignore_range = ignore_range
    out = await download(gate, art, seller[0], tmp_path / "d.out")
    assert out.read_bytes() == data
    gets = [u for meth, u in bucket.requests if meth == "GET" and u.startswith(BUCKET)]
    assert len(gets) == 2


@pytest.mark.parametrize("offset", [0, 30, 150, 500, 70_000, 400_000])
async def test_tampered_ciphertext_never_lands(
    gate: Any, bucket: FakeBucket, tmp_path: Path, offset: int, seller: Any
) -> None:
    src = tmp_path / "t.bin"
    _write(src, 400_200)
    art = await upload(gate, src, recipients=[seller[1]])
    bucket.corrupt(art.key, offset)
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    with pytest.raises(ArtifactError):
        await download(gate, art, seller[0], out_dir / "t.out")
    assert list(out_dir.iterdir()) == []  # neither the file nor its temporary


async def test_truncated_object_is_refused(
    gate: Any, bucket: FakeBucket, tmp_path: Path, seller: Any
) -> None:
    src = tmp_path / "t.bin"
    _write(src, 300_000)
    art = await upload(gate, src, recipients=[seller[1]])
    stored = bucket.artifacts[art.key]
    # Drop the last part and pretend the object is that long: a chunk boundary truncation.
    last = len(stored.plan)
    bucket._part_path(art.key, last).unlink()
    stored.size -= stored.plan.pop()
    with pytest.raises(ArtifactError, match="decrypt"):
        await download(gate, art, seller[0], tmp_path / "t.out")
    assert not (tmp_path / "t.out").exists()


@pytest.mark.parametrize("field", ["sha256", "size"])
async def test_recorded_hash_and_size_are_verified(
    gate: Any, bucket: FakeBucket, tmp_path: Path, field: str, seller: Any
) -> None:
    src = tmp_path / "h.bin"
    _write(src, 100_000)
    art = await upload(gate, src, recipients=[seller[1]])
    lie = {"sha256": "0" * 64} if field == "sha256" else {"size": art.size - 1}
    forged = art.model_copy(update=lie)
    with pytest.raises(ArtifactError, match=r"mismatch|larger"):
        await download(gate, forged, seller[0], tmp_path / "h.out")
    assert not (tmp_path / "h.out").exists()
    pieces: list[bytes] = []

    async def consume() -> None:
        async for p in iter_plaintext(gate, forged, seller[0]):
            pieces.append(p)

    with pytest.raises(ArtifactError):
        await consume()
    # The final piece is withheld when verification fails.
    assert sum(map(len, pieces)) < 100_000


async def test_wrong_identity_is_refused(
    gate: Any, bucket: FakeBucket, tmp_path: Path, seller: Any, buyer: Any
) -> None:
    src = tmp_path / "w.bin"
    _write(src, 1000)
    art = await upload(gate, src, recipients=[seller[1]])
    with pytest.raises(ArtifactError, match="no identity matched"):
        await download(gate, art, buyer[0], tmp_path / "w.out")


async def test_source_changing_during_upload_is_refused(
    gate: Any, bucket: FakeBucket, tmp_path: Path, seller: Any
) -> None:
    src = tmp_path / "c.bin"
    _write(src, 600_000)

    def grow(number: int) -> None:
        if number == 2:
            with src.open("ab") as f:
                f.write(b"more")

    bucket.on_part = grow
    with pytest.raises(ArtifactError, match="changed"):
        await upload(gate, src, recipients=[seller[1]])

    def shrink(number: int) -> None:
        if number == 1:
            with src.open("r+b") as f:
                f.truncate(10)

    bucket.on_part = shrink
    with pytest.raises(ArtifactError, match="shrank"):
        await upload(gate, src, recipients=[seller[1]])


async def test_retry_never_reseals_changed_bytes_under_a_used_nonce(
    gate: Any, bucket: FakeBucket, tmp_path: Path, seller: Any
) -> None:
    """A retried part regenerates its ciphertext under the same STREAM nonces. If the file
    changed in between (here in place, same size, mtime put back so nothing else notices),
    sealing the new bytes would show the bucket two plaintexts under one nonce: their XOR,
    and the Poly1305 key. The upload must stop before that ciphertext is sent."""
    src = tmp_path / "n.bin"
    _write(src, 900_000)
    bucket.record_bodies = True
    bucket.fail_parts = {2: "drop"}  # the first PUT of part 2 dies after the body was sent
    head, ct_chunk = age.header_size(1), age.CHUNK_SIZE + 16
    index = (200_000 + 30_000 - head) // ct_chunk  # a chunk inside part 2
    puts = 0

    def change_in_place(number: int) -> None:
        nonlocal puts
        if number != 2:
            return
        puts += 1
        if puts == 2:  # just before the retry
            st = src.stat()
            with src.open("r+b") as f:
                f.seek(index * age.CHUNK_SIZE + 10)
                byte = f.read(1)[0]
                f.seek(index * age.CHUNK_SIZE + 10)
                f.write(bytes([byte ^ 0xFF]))
            os.utime(src, ns=(st.st_atime_ns, st.st_mtime_ns))

    bucket.on_part = change_in_place
    with pytest.raises(ArtifactError, match="changed"):
        await upload(gate, src, recipients=[seller[1]])
    first, second = [body for n, body in bucket.bodies if n == 2]
    common = min(len(first), len(second))
    assert first[:common] == second[:common]  # no offset ever carried two ciphertexts
    assert not any(a.completed for a in bucket.artifacts.values())


@pytest.mark.parametrize("part_size", [200_000, 2_500_000])
async def test_each_chunk_is_sealed_once(
    gate: Any,
    bucket: FakeBucket,
    tmp_path: Path,
    seller: Any,
    part_size: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Part boundaries (200 kB parts) and 1 MiB body steps (2.5 MB parts) split age chunks;
    the split chunk is served from what was sealed, not read and sealed again."""
    sealed: list[int] = []
    real = age.ChunkSealer.seal

    def counting(
        self: age.ChunkSealer, index: int, plaintext: bytes | memoryview, *, last: bool
    ) -> bytes:
        sealed.append(index)
        return real(self, index, plaintext, last=last)

    monkeypatch.setattr(age.ChunkSealer, "seal", counting)
    bucket.part_size = part_size
    src = tmp_path / "s.bin"
    data = _write(src, 2_500_000)
    art = await upload(gate, src, recipients=[seller[1]])
    assert len(bucket.artifacts[art.key].plan) > 1
    assert sealed == list(range(-(-len(data) // age.CHUNK_SIZE)))
    assert (await download(gate, art, seller[0], tmp_path / "s.out")).read_bytes() == data


@pytest.mark.parametrize(
    "key",
    ["art/..", "art/.bashrc", "art/x/.profile", "..", ".bashrc", "0" * 31 + "/", "0" * 32 + "\n"],
)
def test_keys_that_could_name_a_path_are_not_artifacts(key: str) -> None:
    """The key comes from the counterparty's result and may name the downloaded file
    (``download(..., Path.home())``). Keys are opaque 32-hex strings assigned by the gate, so a
    key that could name a parent directory or a dotfile is refused by the type itself."""
    with pytest.raises(ValueError, match="key"):
        Artifact(key=key, size=1, sha256="0" * 64, content_type="x/y")


async def test_directory_destination_uses_the_opaque_key(
    gate: Any, bucket: FakeBucket, tmp_path: Path, seller: Any, buyer: Any
) -> None:
    src = tmp_path / "in.bin"
    src.write_bytes(b"hello")
    art = await upload(gate, src, recipients=[seller[1], buyer[1]], content_type=None, job_id=None)
    out_dir = tmp_path / "home"
    out_dir.mkdir()
    out = await download(gate, art, seller[0], out_dir)
    assert out == out_dir / art.key
    assert out.read_bytes() == b"hello"


async def test_bad_arguments_fail_before_the_gate(
    gate: Any, bucket: FakeBucket, tmp_path: Path, seller: Any
) -> None:
    src = tmp_path / "a.bin"
    _write(src, 10)
    with pytest.raises(ValueError, match="job id"):
        await upload(gate, src, recipients=[seller[1]], job_id="job-1")
    with pytest.raises(age.AgeError):
        await upload(gate, src, recipients=["age1notakey"])
    with pytest.raises(age.AgeError):
        await upload(gate, src, recipients=[])
    with pytest.raises(ValueError, match="content_type"):
        await upload(gate, src, recipients=[seller[1]], content_type="")
    assert bucket.requests == []


async def test_content_type_and_directory_destination(
    gate: Any, bucket: FakeBucket, tmp_path: Path, seller: Any
) -> None:
    src = tmp_path / "records.jsonl"
    data = b'{"a": 1}\n{"a": 2}\n'
    src.write_bytes(data)
    art = await upload(gate, src, recipients=[seller[1]])
    assert art.content_type == "application/x-ndjson"
    assert bucket.artifacts[art.key].content_type == "application/x-ndjson"
    explicit = await upload(gate, src, recipients=[seller[1]], content_type="text/plain")
    assert explicit.content_type == "text/plain"
    out_dir = tmp_path / "dl"
    out_dir.mkdir()
    out = await download(gate, art, seller[0], out_dir)
    assert out == out_dir / art.key.rsplit("/", 1)[-1]
    assert out.read_bytes() == data


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("a.jsonl", "application/x-ndjson"),
        ("a.NDJSON", "application/x-ndjson"),
        ("a.csv", "text/csv"),
        ("a.json", "application/json"),
        ("a.csv.gz", "application/gzip"),
        ("a.parquet", "application/vnd.apache.parquet"),
        ("noext", "application/octet-stream"),
        ("a.weird-ext", "application/octet-stream"),
    ],
)
def test_guess_content_type(name: str, expected: str) -> None:
    assert guess_content_type(name) == expected


def test_artifact_type_accepts_what_upload_returns() -> None:
    # The gate's key must satisfy the Artifact pattern; the fake's keys do.
    Artifact(key="0" * 32, size=0, sha256="0" * 64, content_type="x/y")
