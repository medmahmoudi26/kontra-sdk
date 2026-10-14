"""What Kontra keeps of a job, against the real stack: no plaintext of a 100 MB job in the
bucket or in Temporal's persistence (acceptance 8), heartbeats that reach the buyer in order
while the gate keeps only ciphertext (13), a 3 GB artifact in flat memory that is deleted
after its retention (14), and an oversized upload refused before a byte is stored (15).

The scans read everything the stack stores, as its operator could: every object in the
bucket through the S3 API, and every text-like and binary column of every table in every
database of its Postgres (the gate's, and Temporal's persistence and visibility stores).
They look for each marker as written, hex-encoded and base64-encoded at every alignment.
A control marker, planted in the bucket and in a scratch database first, shows the scans
find what is there."""

from __future__ import annotations

import asyncio
import hashlib
import os
import re
import secrets
import time
import uuid
from pathlib import Path

import httpx
import pytest

from kontra.auth import Session
from kontra.codec import CLAIM_CHECK_ENCODING, ENCODING
from kontra.contract.names import job_workflow_id
from kontra.gate_client import GateError
from kontra.gate_client import models as m
from kontra.hire import Job, Progress
from kontra.profile import Profile

from .support import (
    PeakRss,
    ScanReport,
    Stack,
    free_disk,
    marker,
    needles,
    own_project,
    serving,
    until,
)

pytestmark = [pytest.mark.e2e]

MIB = 1024 * 1024
RSS_LIMIT = 256 * MIB


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        while block := f.read(4 * MIB):
            digest.update(block)
    return digest.hexdigest()


def _report(label: str, scan: ScanReport) -> str:
    return (
        f"{label}: {len(scan.scanned)} places, {sum(scan.scanned.values())} items, "
        f"{scan.bytes / MIB:.1f} MiB read, hits {scan.hits or 'none'}"
    )


def _hits_for(scan: ScanReport, wanted: range) -> dict[str, list[int]]:
    """The scan's hits on the needles whose indexes are in ``wanted``."""
    out: dict[str, list[int]] = {}
    for place, found in scan.hits.items():
        mine = [i for i in found if i in wanted]
        if mine:
            out[place] = mine
    return out


async def _follow(job: Job, into: list[Progress]) -> None:
    async for p in job.progress():
        into.append(p)


async def _drain(task: asyncio.Task[None], within: float) -> bool:
    """Wait for a progress stream to end by itself (the job closed); False if it had not
    after ``within`` seconds (it is cancelled then)."""
    done, _ = await asyncio.wait({task}, timeout=within)
    if done:
        task.result()
        return True
    task.cancel()
    return False


# --------------------------------------------------------------------------- acceptance 8


def _write_input(path: Path, mark: str, size: int) -> None:
    """``size`` random bytes with the marker at the start of every MiB. Random, so that no
    store compresses them: plaintext on a disk would be there byte for byte, where the file
    scans look (SeaweedFS gzips a compressible body before it writes it)."""
    with path.open("wb") as f:
        left = size
        while left > 0:
            block = (mark.encode() + secrets.token_bytes(MIB))[: min(MIB, left)]
            f.write(block)
            left -= len(block)


@pytest.mark.timeout(2400)
async def test_ac08_no_plaintext_of_a_100mb_job_in_the_bucket_or_temporal(
    stack: Stack, tmp_path: Path
) -> None:
    """A job whose input is a 100 MB file (an ``Artifact``) plus a 1 MB inline note (over
    the codec's threshold: claim-checked), both carrying a marker, and whose output is the
    same again (the seller re-encrypts the file to the buyer and returns the note). The
    buyer reads the marker in what it receives; the bucket and Temporal hold none of it."""
    seller = await stack.party("ac08")
    buyer = await stack.party("ac08-buyer")
    key = f"{seller.handle}/vault@1.0.0"
    project = own_project(
        "vault", tmp_path / "vault", f"{seller.handle}/vault", max_output_bytes="200MB"
    )
    mark = marker("ac08")
    size = 100 * 1000 * 1000
    source = tmp_path / "input.bin"
    await asyncio.to_thread(_write_input, source, mark, size)
    note = f"{mark} " + "n" * 1_000_000

    k = buyer.kontra()
    try:
        async with serving(project, seller, [key], log=tmp_path / "serve.log"):
            t = time.monotonic()
            data = await k.upload(key, source, content_type="application/octet-stream")
            job = await k.hire(key, max_price="1.00", data=data, note=note)
            result = await job.result(timeout=1200)
            took = time.monotonic() - t
            assert result.note == note
            received = tmp_path / "output.bin"
            await result.data.download(received)
            await job.accept()
        # The seller's history of the job, as Temporal stores it: every payload is
        # ciphertext or a claim-check reference (here all of them: each is over 256 KiB),
        # but for the header that tells the seller's activities their job id.
        payloads = await seller.history_payloads(job_workflow_id(job.job_id))
        assert result.data.size == size + len(b"\nresealed by the seller\n")
        assert await asyncio.to_thread(_sha256, received) == result.data.sha256
        with received.open("rb") as f:
            assert f.read(len(mark)) == mark.encode()  # the buyer reads the plaintext
        received.unlink()
        source.unlink()
    finally:
        await k.aclose()

    # A control marker, stored where the scans look, proves they would find one.
    control = marker("control")
    planted = f"e2e-control/{uuid.uuid4().hex}"
    s3 = stack.s3()
    s3.put_object(  # random around it: stored as is, so the file scan can see it too
        Bucket=stack.s3_bucket,
        Key=planted,
        Body=secrets.token_bytes(100) + control.encode() + secrets.token_bytes(100),
    )
    control_db = f"e2e_control_{secrets.token_hex(4)}"
    stack.psql("postgres", f"CREATE DATABASE {control_db};")
    try:
        # Compressible and large: Postgres stores it TOASTed and compressed (the SQL scan
        # must decompress to see it); and short, so stored as is (the file scan sees it).
        stack.psql(
            control_db,
            "CREATE TABLE planted (b bytea);"
            f"INSERT INTO planted VALUES (convert_to(repeat('z', 500000) || '{control}', 'UTF8'));"
            f"INSERT INTO planted VALUES (convert_to('{control}', 'UTF8'));",
        )
        real, check = needles(mark), needles(control)
        probes = real + check
        ours, theirs = range(len(real)), range(len(real), len(probes))
        t = time.monotonic()
        bucket = await asyncio.to_thread(stack.scan_bucket, probes)
        database = await asyncio.to_thread(stack.scan_postgres, probes)
        # The disks themselves, as the operator's backups hold them: Postgres's data
        # directory with its write-ahead log (a row written then deleted is still there),
        # and SeaweedFS's volumes (a deleted object's bytes stay until a vacuum). Byte for
        # byte only: what a store compressed (a large Postgres value, a compressible
        # object) is not seen here, but the input and output are random and would not be.
        stack.psql("postgres", "CHECKPOINT;")
        where = (("postgres", stack.postgres_data_dir()), ("s3", "/data"))
        disks = {
            svc: await asyncio.to_thread(stack.scan_files, svc, path, real) for svc, path in where
        }
        disk_controls = {
            svc: await asyncio.to_thread(stack.scan_files, svc, path, check) for svc, path in where
        }
        scan_secs = time.monotonic() - t
    finally:
        s3.delete_object(Bucket=stack.s3_bucket, Key=planted)
        stack.psql("postgres", f"DROP DATABASE {control_db};")
    history_rows = database.scanned.get("temporal.public.history_node.data", 0)
    print(
        f"\n[acceptance 8] job {took:.1f}s (100 MB in, 100 MB out, 1 MB note each way); "
        f"scans {scan_secs:.1f}s, {len(real)} encodings of the marker\n  "
        + _report("bucket", bucket)
        + "\n  "
        + _report("postgres", database)
        + f"\n  Temporal history_node rows scanned: {history_rows}; the seller's history"
        f" of the job: {len(payloads)} payloads, encoded "
        f"{sorted({enc for _, enc, _ in payloads})}"
        + f"\n  files on disk holding the marker: {disks}; the control: {disk_controls}"
    )
    sealed = {ENCODING.decode(), CLAIM_CHECK_ENCODING.decode()}
    assert any(enc in sealed for _, enc, _ in payloads), payloads
    clear = [(where, enc, data) for where, enc, data in payloads if enc not in sealed]
    job_id_header = [
        (where, "json/plain", f'"{job.job_id}"'.encode())
        for where, _, _ in clear
        if where.endswith(".header.fields[kontra-job]")
    ]
    assert clear == job_id_header, clear
    assert planted in _hits_for(bucket, theirs)
    assert any(place.startswith(f"{control_db}.") for place in _hits_for(database, theirs))
    assert all(disk_controls.values()), disk_controls  # the file scans see what is there
    assert bucket.bytes > 2 * size  # the job's ciphertext was read
    assert history_rows > 0  # Temporal's history itself was read, not only its schema
    assert any(p.startswith("kontra.") for p in database.scanned)
    assert _hits_for(bucket, ours) == {}
    assert _hits_for(database, ours) == {}
    assert disks == {key: [] for key in disks}, disks


# --------------------------------------------------------------------------- acceptance 13


@pytest.mark.timeout(900)
async def test_ac13_heartbeats_reach_the_buyer_in_order_and_the_gate_keeps_ciphertext(
    stack: Stack, tmp_path: Path
) -> None:
    """The seller's activity (no Kontra import) heartbeats ``{percent, message}`` 100 times,
    every 1.5 s, under the forwarder's default throttle (one event a second per job, latest
    wins); ``job.progress()`` yields all 100 in order, decrypted. Nothing the stack stores holds a
    message's text. A second job, whose activity never heartbeats, still shows liveness and
    the running activity's name, from history."""
    seller = await stack.party("ac13")
    buyer = await stack.party("ac13-buyer")
    key = f"{seller.handle}/pulse@1.0.0"
    project = own_project("pulse", tmp_path / "pulse", f"{seller.handle}/pulse")
    activities = (project / "activities.py").read_text()
    assert re.search(r"^\s*(from|import)\s+kontra\b", activities, re.M) is None
    tag = marker("ac13")
    k = buyer.kontra()
    try:
        # The seller's own settings: no KONTRA_PROGRESS_INTERVAL (party_env passes no
        # KONTRA_* of ours), so the forwarder throttles as shipped, one event a second.
        async with serving(project, seller, [key], log=tmp_path / "serve.log"):
            job = await k.hire(key, max_price="1.00", beats=100, tag=tag, quiet_seconds=0)
            events: list[Progress] = []
            follow = asyncio.create_task(_follow(job, events))
            assert (await job.result(timeout=300)).beats == 100
            await job.accept()
            ended = await _drain(follow, 90)

            quiet = await k.hire(key, max_price="1.00", beats=0, tag="", quiet_seconds=25)
            quiet_events: list[Progress] = []
            quiet_follow = asyncio.create_task(_follow(quiet, quiet_events))
            assert (await quiet.result(timeout=300)).quiet_seconds == 25
            await quiet.accept()
            quiet_ended = await _drain(quiet_follow, 90)
    finally:
        await k.aclose()

    beats = [p for p in events if p.source is m.ProgressSource.HEARTBEAT]
    print(
        f"\n[acceptance 13] {len(beats)} heartbeats of {len(events)} events "
        f"(stream ended: {ended}); first {beats[0] if beats else None!s}, last "
        f"{beats[-1] if beats else None!s}"
    )
    assert [p.message for p in beats] == [f"beat {i} {tag}" for i in range(1, 101)]
    assert [p.percent for p in beats] == [float(i) for i in range(1, 101)]
    assert {p.activity for p in beats} == {"pulse"}
    seqs = [p.seq for p in events]
    assert seqs == sorted(seqs)
    assert len(set(seqs)) == len(seqs)

    history = [p for p in quiet_events if p.source is m.ProgressSource.HISTORY]
    print(
        f"  quiet job: {len(quiet_events)} events, all from history (stream ended: "
        f"{quiet_ended}): " + "; ".join(str(p) for p in history)
    )
    assert not any(p.source is m.ProgressSource.HEARTBEAT for p in quiet_events)
    assert any("quiet" in p.activities_running for p in history), history
    alive = next(p for p in history if "quiet" in p.activities_running)
    assert str(alive).startswith("alive, in quiet, at ")

    scan = await asyncio.to_thread(stack.scan_postgres, needles(tag))
    bucket = await asyncio.to_thread(stack.scan_bucket, needles(tag))
    stack.psql("postgres", "CHECKPOINT;")
    on_disk = await asyncio.to_thread(
        stack.scan_files, "postgres", stack.postgres_data_dir(), needles(tag)
    )
    print(
        "  "
        + _report("postgres", scan)
        + "\n  "
        + _report("bucket", bucket)
        + f"\n  Postgres files on disk (WAL included) holding a message: {on_disk}"
    )
    assert scan.hits == {}
    assert bucket.hits == {}
    assert on_disk == []
    assert any(p.startswith("kontra.") and "progress" in p for p in scan.scanned)


# --------------------------------------------------------------------------- acceptance 14


@pytest.mark.slow
@pytest.mark.timeout(3600)
async def test_ac14_a_3gb_artifact_in_flat_memory_verified_and_gone_after_retention(
    stack: Stack, tmp_path: Path
) -> None:
    """The seller's activity writes 3 GiB and returns it as an ``Artifact`` (uploaded with
    ``Artifact.from_path``): the seller's worker process stays under 256 MiB above its
    baseline throughout; the buyer downloads it, also under 256 MiB, and the sha256 matches
    the seller's own. Accepted, it is deleted from the bucket after ``artifact_retention``
    (5 s here; the gate's janitor passes once a minute). ``KONTRA_E2E_ARTIFACT_BYTES``
    overrides the size."""
    size = int(os.environ.get("KONTRA_E2E_ARTIFACT_BYTES", str(3 * 1024**3)))
    need = int(2.2 * size) + 2 * 1024**3  # the seller's file or the download, and the bucket
    if free_disk(tmp_path) < need:
        pytest.skip(f"needs {need / 1024**3:.1f} GiB of free disk for a {size}-byte artifact")
    seller = await stack.party("ac14")
    buyer = await stack.party("ac14-buyer")
    key = f"{seller.handle}/bulk@1.0.0"
    project = own_project(
        "bulk",
        tmp_path / "bulk",
        f"{seller.handle}/bulk",
        max_output_bytes="4GB",
        artifact_retention="5s",
        max_duration="1h",
    )
    k = buyer.kontra()
    try:
        async with serving(project, seller, [key], log=tmp_path / "serve.log") as worker:
            # Warm up (imports, the workflow sandbox, a first upload), then the baseline.
            warm = await k.hire(key, max_price="1.00", size=MIB, seed=1)
            assert len((await warm.result(timeout=300)).files) == 1
            await warm.accept()
            t = time.monotonic()
            with PeakRss(worker.pid) as up:
                job = await k.hire(key, max_price="1.00", size=size, seed=7)
                result = await job.result(timeout=3000)
            up_secs = time.monotonic() - t
        assert result.refused == "", result.refused
        (artifact,) = result.files
        assert (artifact.size, artifact.sha256) == (size, result.sha256)
        stored = f"artifacts/{artifact.key}"
        objects = await asyncio.to_thread(stack.objects, "artifacts/")
        assert objects.get(stored, 0) > size  # ciphertext: the plaintext plus age's overhead

        dest = tmp_path / "download.bin"
        t = time.monotonic()
        with PeakRss() as down:
            await artifact.download(dest)
        down_secs = time.monotonic() - t
        assert await asyncio.to_thread(_sha256, dest) == result.sha256
        dest.unlink()

        await job.accept()
        accepted = time.monotonic()

        async def present() -> bool:
            return stored in await asyncio.to_thread(stack.objects, "artifacts/")

        await until(present, lambda here: not here, within=600, interval=5, what="the deletion")
        gone_secs = time.monotonic() - accepted
        with pytest.raises(GateError) as refused:
            await artifact.download(tmp_path / "again.bin")
        assert refused.value.status in (404, 410), refused.value
    finally:
        await k.aclose()
    print(
        f"\n[acceptance 14] {size} bytes ({size / MIB:.0f} MiB)\n"
        f"  seller job (write + encrypt + upload): {up_secs:.1f}s; worker RSS baseline "
        f"{up.baseline / MIB:.1f} MiB, peak {up.peak / MIB:.1f} MiB, growth {up.growth / MIB:.1f} MiB\n"
        f"  buyer download: {down_secs:.1f}s; RSS baseline {down.baseline / MIB:.1f} MiB, "
        f"peak {down.peak / MIB:.1f} MiB, growth {down.growth / MIB:.1f} MiB\n"
        f"  deleted from the bucket {gone_secs:.0f}s after accept (artifact_retention 5s)"
    )
    assert up.growth < RSS_LIMIT
    assert down.growth < RSS_LIMIT


# --------------------------------------------------------------------------- acceptance 15


@pytest.mark.timeout(600)
async def test_ac15_an_upload_beyond_max_output_bytes_is_refused_before_storing(
    stack: Stack, tmp_path: Path
) -> None:
    """With ``max_output_bytes: 1MB``, the seller's activity tries to return a 2 MB file:
    the gate refuses the upload when it is declared (413 ``too_large``), and the bucket gets
    neither an object nor an open multipart upload for it, nor the gate a record of one."""
    seller = await stack.party("ac15")
    buyer = await stack.party("ac15-buyer")
    key = f"{seller.handle}/bulk@1.0.0"
    project = own_project(
        "bulk", tmp_path / "bulk", f"{seller.handle}/bulk", max_output_bytes="1MB"
    )
    objects = await asyncio.to_thread(stack.objects)
    uploads = await asyncio.to_thread(stack.uploads)
    k = buyer.kontra()
    try:
        async with serving(project, seller, [key], log=tmp_path / "serve.log"):
            job = await k.hire(key, max_price="1.00", size=2_000_000, seed=3)
            result = await job.result(timeout=300)
    finally:
        await k.aclose()
    print(f"\n[acceptance 15] the seller's upload of 2000000 bytes: {result.refused}")
    assert result.files == []
    assert "too_large" in result.refused
    assert "max_output_bytes" in result.refused
    assert await asyncio.to_thread(stack.objects) == objects
    assert await asyncio.to_thread(stack.uploads) == uploads
    rows = stack.psql("kontra", f"SELECT count(*) FROM artifacts WHERE job_id = '{job.job_id}';")
    assert rows.strip() == "0"


@pytest.mark.timeout(600)
async def test_ac15_a_body_beyond_its_declared_size_is_refused_before_storing(
    stack: Stack, tmp_path: Path
) -> None:
    """The other way past ``max_output_bytes``: a seller (here its own credentials, by hand,
    as modified code could) declares an output within the budget and sends more. The URL
    the gate presigned signs the declared length, so the bucket itself refuses the body
    and stores nothing; the same URL takes exactly the declared length."""
    seller = await stack.party("ac15b")
    buyer = await stack.party("ac15b-buyer")
    key = f"{seller.handle}/pulse@1.0.0"
    project = own_project(
        "pulse", tmp_path / "pulse", f"{seller.handle}/pulse", max_output_bytes="1MB"
    )
    declared = 1000
    k = buyer.kontra()
    try:
        async with serving(project, seller, [key], log=tmp_path / "serve.log"):
            job = await k.hire(key, max_price="1.00", beats=0, tag="", quiet_seconds=30)
            await until(job.status, lambda v: v.state is m.JobState.WORKING, within=60)
            before = await asyncio.to_thread(stack.objects)
            uploads = await asyncio.to_thread(stack.uploads)
            async with Session(Profile.load(seller.home), home=seller.home) as session:
                req = m.ArtifactCreate(size=declared, job_id=job.job_id)
                plan = await session.gate.create_artifact(req)
            (part,) = plan.parts
            assert part.size == declared
            headers = {"Content-Type": req.content_type}
            async with httpx.AsyncClient(timeout=60) as http:
                over = await http.put(part.url, content=os.urandom(2_000_000), headers=headers)
                after_over = await asyncio.to_thread(stack.objects)
                open_after_over = await asyncio.to_thread(stack.uploads)
                parts_after_over = (
                    []
                    if plan.upload_id is None
                    else stack.s3()
                    .list_parts(
                        Bucket=stack.s3_bucket,
                        Key=f"artifacts/{plan.key}",
                        UploadId=plan.upload_id,
                    )
                    .get("Parts", [])
                )
                exact = await http.put(part.url, content=os.urandom(declared), headers=headers)
            assert (await job.result(timeout=120)).quiet_seconds == 30
            await job.accept()
    finally:
        await k.aclose()
    print(
        f"\n[acceptance 15] {declared} bytes declared, 2000000 sent: HTTP {over.status_code} "
        f"{over.text[:160]!r}; the declared length then: HTTP {exact.status_code}"
    )
    assert over.status_code == 403, over.text
    assert after_over == before  # no object, not even a partial one
    # The plan's own multipart upload, if it made one, is the only new one, and holds no part.
    assert set(open_after_over) - set(uploads) <= {f"artifacts/{plan.key}"}
    assert parts_after_over == []
    assert exact.is_success, exact.text  # the URL was good: only the length was refused
