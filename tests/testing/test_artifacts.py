"""Artifacts and claim-check blobs on the fake gate (§10.8, acceptance 15): presigned-style
URLs signed by the gate itself (method, path, exact length, expiry), ``max_output_bytes``
enforced before anything is stored, access by job role, and retention."""

from __future__ import annotations

import hashlib
import os
from collections.abc import AsyncIterator, MutableMapping
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Any

import httpx
import pytest

from kontra.artifacts import download, upload
from kontra.auth import Session
from kontra.codec import CodecError, GateBlobStore
from kontra.escrow import ObservedStatus
from kontra.gate_client import GateClient, GateError
from kontra.gate_client import models as m
from kontra.testing.fake_gate import FakeGate

from .support import ScriptedControl, json_payload, logged_in, offering_doc, ok_quote, wire

#: A buyer key for a direct `GateClient.hire` call. Real hires make one per job
#: (`Profile.new_pending_job_identity`); a test that drives the route itself only needs a
#: well-formed recipient (decisions I7).
_A_BUYER_KEY = "age1e0q4lyexsefmh6hurumvgl3cmmsfnz4zyfhkd7lfrjcpqc2e250qs6j4el"


PART = 64 * 1024


@dataclass
class Bench:
    gate: FakeGate
    control: ScriptedControl
    transport: httpx.ASGITransport
    seller: Session
    buyer: Session
    stranger: Session
    worker: GateClient
    job: m.HireView
    tmp: Path

    @property
    def recipients(self) -> list[str]:
        return [self.seller.profile.public_key or "", self.buyer.profile.public_key or ""]

    def file(self, name: str, size: int) -> Path:
        p = self.tmp / name
        p.write_bytes(os.urandom(size))
        return p

    async def raw(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(transport=self.transport, base_url=self.gate.base_url)


@pytest.fixture
async def bench(tmp_path: Path) -> AsyncIterator[Bench]:
    control = ScriptedControl(responder=lambda p, h: ok_quote(100))
    gate = FakeGate(control, sweep_interval=0, part_size=PART, resolvers=["judge"])
    async with gate:
        t = httpx.ASGITransport(app=gate.app)
        seller = await logged_in(gate.base_url, tmp_path / "acme", "acme", t)
        buyer = await logged_in(gate.base_url, tmp_path / "bob", "bob", t)
        stranger = await logged_in(gate.base_url, tmp_path / "eve", "eve", t)
        await seller.gate.publish(offering_doc("acme", max_output_bytes="400KB"))
        q = await buyer.gate.quote("acme/enrich", wire(json_payload({})))
        assert q.quote_id is not None
        job = await buyer.gate.hire(q.quote_id, buyer_key=_A_BUYER_KEY)
        token = (await seller.gate.temporal_token()).token

        async def bearer() -> str:
            return token

        worker = GateClient(gate.base_url, token=bearer, transport=t)
        try:
            yield Bench(gate, control, t, seller, buyer, stranger, worker, job, tmp_path)
        finally:
            for s in (seller, buyer, stranger):
                await s.aclose()
            await worker.aclose()


async def test_an_output_round_trips_in_parts(bench: Bench) -> None:
    src = bench.file("records.jsonl", 300 * 1024)
    art = await upload(bench.worker, src, recipients=bench.recipients, job_id=bench.job.job_id)
    stored = bench.gate.artifacts[art.key]
    assert stored.upload_id is not None
    assert len(stored.plan) == 5  # 300 KiB of plaintext + age overhead in 64 KiB parts
    assert stored.completed
    assert art.content_type == "application/x-ndjson"
    assert len(art.key) == 32
    again = await download(
        bench.seller.gate, art, bench.seller.identity(), bench.tmp / "mine.jsonl"
    )
    assert again.read_bytes() == src.read_bytes()
    # The buyer's copy waits for the delivery.
    bench.control.observe_as("acme", f"job-{bench.job.job_id}", ObservedStatus.COMPLETED, b"r")
    await bench.gate.sweep()
    out = await download(bench.buyer.gate, art, bench.buyer.identity(), bench.tmp / "got.jsonl")
    assert out.read_bytes() == src.read_bytes()
    assert src.read_bytes() not in stored.data()  # the gate holds ciphertext
    with pytest.raises(GateError) as e:
        await bench.stranger.gate.download_artifact(art.key)  # outputs: the job's parties only
    assert (e.value.status, e.value.code) == (404, "artifact_not_found")


async def test_the_buyer_fetches_an_output_only_once_the_job_delivered(bench: Bench) -> None:
    """Like the platform gate: a seller that leaks an output's key before delivering (in a
    progress message, say) must not hand the buyer the work while the buyer can still cancel
    for a full refund. The buyer's fetch waits until the gate counted the delivery (state
    ``completed``: in review or later); the seller's never waits; a job that closed without
    a delivery (canceled, failed: refunded) never releases its outputs to the buyer."""

    async def output(job_id: str) -> str:
        art = await upload(
            bench.worker, bench.file("o.bin", 100), recipients=bench.recipients, job_id=job_id
        )
        return art.key

    async def refused(key: str) -> GateError:
        with pytest.raises(GateError) as e:
            await bench.buyer.gate.download_artifact(key)
        assert (e.value.status, e.value.code) == (409, "not_delivered")
        return e.value

    async def hire() -> str:
        q = await bench.buyer.gate.quote("acme/enrich", wire(json_payload({})))
        assert q.quote_id is not None
        return (await bench.buyer.gate.hire(q.quote_id, buyer_key=_A_BUYER_KEY)).job_id

    def script(job_id: str, status: ObservedStatus, result: bytes | None = None) -> None:
        bench.control.observe_as("acme", f"job-{job_id}", status, result)

    done = bench.job.job_id
    key = await output(done)
    assert "submitted" in str(await refused(key))
    script(done, ObservedStatus.RUNNING)
    await bench.gate.sweep()
    assert "working" in str(await refused(key))
    assert (await bench.worker.download_artifact(key)).size > 0  # the seller, any time
    script(done, ObservedStatus.COMPLETED, b"result")
    assert "working" in str(await refused(key))  # completed, but not observed yet
    await bench.gate.sweep()
    view = await bench.buyer.gate.job(done)
    assert (view.state, view.escrow) == (m.JobState.COMPLETED, m.Escrow.HELD)  # in review
    assert (await bench.buyer.gate.download_artifact(key)).size > 0
    await bench.buyer.gate.reject(done, "wrong records")
    assert (await bench.buyer.gate.download_artifact(key)).size > 0  # what is disputed
    with pytest.raises(GateError) as e:
        await bench.stranger.gate.download_artifact(key)
    assert (e.value.status, e.value.code) == (404, "artifact_not_found")

    # The attack this closes: fetch the leaked output, then cancel for a full refund.
    canceled = await hire()
    key = await output(canceled)
    await refused(key)
    assert (await bench.buyer.gate.cancel(canceled)).state is m.JobState.CANCELED
    await refused(key)
    assert (await bench.seller.gate.download_artifact(key)).size > 0

    failed = await hire()
    key = await output(failed)
    script(failed, ObservedStatus.RUNNING)
    await bench.gate.sweep()
    script(failed, ObservedStatus.FAILED)
    await bench.gate.sweep()
    assert (await bench.buyer.gate.job(failed)).state is m.JobState.FAILED
    await refused(key)
    assert (await bench.seller.gate.download_artifact(key)).size > 0


async def test_the_buyer_keeps_its_outputs_through_settlement(bench: Bench) -> None:
    """Delivered is for good: whatever settles the escrow afterwards -- the buyer's accept,
    a dispute and the resolver's split, the review window closing -- the buyer still
    fetches its outputs (until retention deletes them). The resolver is no party to the
    job and fetches none, as before."""
    judge = await logged_in(bench.gate.base_url, bench.tmp / "judge", "judge", bench.transport)

    async def delivered_output() -> tuple[str, str]:
        q = await bench.buyer.gate.quote("acme/enrich", wire(json_payload({})))
        assert q.quote_id is not None
        job_id = (await bench.buyer.gate.hire(q.quote_id, buyer_key=_A_BUYER_KEY)).job_id
        art = await upload(
            bench.worker, bench.file("o.bin", 100), recipients=bench.recipients, job_id=job_id
        )
        bench.control.observe_as("acme", f"job-{job_id}", ObservedStatus.COMPLETED, b"result")
        await bench.gate.sweep()
        assert (await bench.buyer.gate.job(job_id)).state is m.JobState.COMPLETED
        return job_id, art.key

    try:
        accepted, key = await delivered_output()
        assert (await bench.buyer.gate.accept(accepted)).escrow is m.Escrow.RELEASED
        assert (await bench.buyer.gate.download_artifact(key)).size > 0

        disputed, key = await delivered_output()
        await bench.buyer.gate.reject(disputed, "half the records are empty")
        split = m.ResolveRequest(buyer_minor=50, seller_minor=50, note="half were fine")
        assert (await judge.gate.resolve(disputed, split)).escrow is m.Escrow.RESOLVED
        assert (await bench.buyer.gate.download_artifact(key)).size > 0
        assert (await bench.seller.gate.download_artifact(key)).size > 0
        with pytest.raises(GateError) as e:
            await judge.gate.download_artifact(key)
        assert (e.value.status, e.value.code) == (404, "artifact_not_found")

        lapsed, key = await delivered_output()
        bench.gate.advance(timedelta(hours=2))  # past the 1h review window
        await bench.gate.sweep()
        assert (await bench.buyer.gate.job(lapsed)).escrow is m.Escrow.RELEASED
        assert (await bench.buyer.gate.download_artifact(key)).size > 0
    finally:
        await judge.aclose()


async def test_an_input_is_readable_by_any_party_holding_the_key(bench: Bench) -> None:
    src = bench.file("in.bin", 1000)
    art = await upload(bench.buyer.gate, src, recipients=[bench.seller.profile.public_key or ""])
    assert bench.gate.artifacts[art.key].upload_id is None  # one part: a single PUT
    url = await bench.stranger.gate.download_artifact(art.key)
    assert url.size == bench.gate.artifacts[art.key].size


async def test_outputs_above_max_output_bytes_are_refused_before_storing(bench: Bench) -> None:
    big = bench.file("big.bin", 401 * 1000)
    puts: list[str] = []
    seen = bench.gate.app

    async def spy(scope: MutableMapping[str, Any], receive: Any, send: Any) -> None:
        if scope.get("type") == "http" and scope.get("method") == "PUT":
            puts.append(str(scope.get("path")))
        await seen(scope, receive, send)

    spied = GateClient(
        bench.gate.base_url,
        token=bench.worker._tokens,  # pyright: ignore[reportPrivateUsage]
        transport=httpx.ASGITransport(app=spy),
    )
    with pytest.raises(GateError) as e:
        await upload(spied, big, recipients=bench.recipients, job_id=bench.job.job_id)
    assert (e.value.status, e.value.code) == (413, "too_large")
    assert e.value.detail is not None
    assert e.value.detail["max_output_bytes"] == 400 * 1000
    assert puts == []
    assert bench.gate.artifacts == {}
    # the limit is per job, cumulative
    await upload(
        bench.worker,
        bench.file("a.bin", 250 * 1000),
        recipients=bench.recipients,
        job_id=bench.job.job_id,
    )
    with pytest.raises(GateError) as e:
        await upload(
            bench.worker,
            bench.file("b.bin", 250 * 1000),
            recipients=bench.recipients,
            job_id=bench.job.job_id,
        )
    assert e.value.code == "too_large"
    await spied.aclose()


async def test_inputs_have_their_own_limit() -> None:
    gate = FakeGate(None, sweep_interval=0, max_input_bytes=1000)
    async with gate:
        t = httpx.ASGITransport(app=gate.app)
        tok = await GateClient(gate.base_url, transport=t).dev_login("bob")

        async def bearer() -> str:
            return tok.access_token

        bob = GateClient(gate.base_url, token=bearer, transport=t)
        assert (await bob.create_artifact(m.ArtifactCreate(size=1000))).upload_id is None
        with pytest.raises(GateError) as e:
            await bob.create_artifact(m.ArtifactCreate(size=1001))
        assert (e.value.status, e.value.code) == (413, "too_large")
        with pytest.raises(GateError) as e:
            await bob.create_artifact(m.ArtifactCreate(size=1, content_type=""))
        assert e.value.code == "invalid_request"


async def test_only_the_sellers_open_job_takes_outputs(bench: Bench) -> None:
    req = m.ArtifactCreate(size=10, job_id=bench.job.job_id)
    with pytest.raises(GateError) as e:
        await bench.buyer.gate.create_artifact(req)
    assert (e.value.status, e.value.code) == (403, "forbidden")
    with pytest.raises(GateError) as e:
        await bench.stranger.gate.create_artifact(req)
    assert (e.value.status, e.value.code) == (404, "job_not_found")
    with pytest.raises(GateError) as e:
        await bench.worker.create_artifact(m.ArtifactCreate(size=10, job_id="0" * 32))
    assert e.value.code == "job_not_found"
    await bench.buyer.gate.cancel(bench.job.job_id)
    with pytest.raises(GateError) as e:
        await bench.worker.create_artifact(req)
    assert (e.value.status, e.value.code) == (409, "not_open")


async def test_signed_urls_bind_method_path_length_and_time(bench: Bench) -> None:
    up = await bench.worker.create_artifact(m.ArtifactCreate(size=100, job_id=bench.job.job_id))
    url = up.parts[0].url
    async with await bench.raw() as http:
        r = await http.put(url, content=b"x" * 99)
        assert (r.status_code, r.json()["code"]) == (403, "length_mismatch")
        r = await http.put(url.replace("sig=", "sig=0"), content=b"x" * 100)
        assert (r.status_code, r.json()["code"]) == (403, "signature_mismatch")
        r = await http.put(url.replace("len=100", "len=99"), content=b"x" * 99)
        assert (r.status_code, r.json()["code"]) == (403, "signature_mismatch")
        r = await http.get(url)  # a PUT signature does not open a GET
        assert r.status_code in (403, 405)

        def chunks() -> object:
            async def gen() -> AsyncIterator[bytes]:
                yield b"x" * 100

            return gen()

        r = await http.put(url, content=chunks())  # type: ignore[arg-type]  # no Content-Length
        assert (r.status_code, r.json()["code"]) == (411, "length_required")
        bench.gate.advance(timedelta(hours=2))
        r = await http.put(url, content=b"x" * 100)
        assert (r.status_code, r.json()["code"]) == (403, "url_expired")
        bench.gate.advance(-timedelta(hours=2))
        r = await http.put(url, content=b"x" * 100)
        assert r.status_code == 200
        assert r.headers["etag"].startswith('"')


async def test_complete_checks_parts_and_digest(bench: Bench) -> None:
    up = await bench.worker.create_artifact(
        m.ArtifactCreate(size=PART + 10, job_id=bench.job.job_id)
    )
    data = os.urandom(PART + 10)
    good = hashlib.sha256(data).hexdigest()

    async def complete(client: GateClient, **kw: object) -> GateError:
        fields: dict[str, object] = {
            "upload_id": up.upload_id,
            "etags": etags,
            "ciphertext_sha256": good,
        }
        fields.update(kw)
        with pytest.raises(GateError) as e:
            await client.complete_artifact(up.key, m.ArtifactComplete.model_validate(fields))
        return e.value

    etags: list[str] = []
    async with await bench.raw() as http:
        r = await http.put(up.parts[0].url, content=data[:PART])
        etags.append(r.headers["etag"])
        assert (await complete(bench.worker)).code == "bad_upload"  # part 2 is missing
        r = await http.put(up.parts[1].url, content=data[PART:])
        etags.append(r.headers["etag"])
    assert (await complete(bench.worker, ciphertext_sha256="0" * 64)).code == "bad_upload"
    assert (await complete(bench.worker, upload_id="nope")).code == "bad_upload"
    assert (await complete(bench.worker, etags=list(reversed(etags)))).code == "bad_upload"
    assert (await complete(bench.buyer.gate)).code == "artifact_not_found"  # not the uploader's
    done = m.ArtifactComplete(upload_id=up.upload_id, etags=etags, ciphertext_sha256=good)
    await bench.worker.complete_artifact(up.key, done)
    await bench.worker.complete_artifact(up.key, done)  # idempotent
    url = await bench.worker.download_artifact(up.key)
    async with await bench.raw() as http:
        r = await http.get(url.url, headers={"Range": "bytes=10-"})
        assert r.status_code == 206
        assert r.headers["content-range"] == f"bytes 10-{len(data) - 1}/{len(data)}"
        assert r.content == data[10:]
        r = await http.get(url.url)
        assert (r.status_code, r.content) == (200, data)
        r = await http.put(up.parts[0].url, content=data[:PART])  # sealed once completed
        assert r.status_code == 404


async def test_claim_check_blobs(bench: Bench) -> None:
    store = GateBlobStore(bench.worker)
    data = os.urandom(5000)
    digest = hashlib.sha256(data).hexdigest()
    await store.put(digest, data)
    assert await GateBlobStore(bench.buyer.gate).get(digest) == data
    exists = await bench.worker.create_blob(m.BlobCreate(sha256=digest, size=len(data)))
    assert exists.exists  # content-addressed: the second writer uploads nothing
    with pytest.raises(GateError) as e:
        await bench.worker.get_blob("1" * 64)
    assert (e.value.status, e.value.code) == (404, "blob_not_found")
    with pytest.raises(GateError) as e:
        await bench.worker.create_blob(m.BlobCreate(sha256="2" * 64, size=64 * 1024 * 1024 + 1))
    assert (e.value.status, e.value.code) == (413, "too_large")
    with pytest.raises(CodecError):  # bytes that do not hash to the name are refused
        await store.put("3" * 64, b"not those bytes")
    with pytest.raises(GateError):
        await bench.worker.get_blob("3" * 64)


async def test_retention_deletes_outputs_inputs_and_orphans(bench: Bench) -> None:
    out = await upload(
        bench.worker, bench.file("o.bin", 100), recipients=bench.recipients, job_id=bench.job.job_id
    )
    inp = await upload(bench.buyer.gate, bench.file("i.bin", 100), recipients=bench.recipients)
    orphan = await bench.worker.create_artifact(m.ArtifactCreate(size=10, job_id=bench.job.job_id))
    bench.control.observe_as("acme", f"job-{bench.job.job_id}", ObservedStatus.COMPLETED, b"r")
    await bench.gate.sweep()
    await bench.buyer.gate.accept(bench.job.job_id)
    bench.gate.advance(timedelta(hours=23))
    await bench.gate.sweep()
    assert {out.key, inp.key, orphan.key} <= set(bench.gate.artifacts)
    bench.gate.advance(timedelta(hours=2))  # artifact_retention (1d) after the escrow closed
    await bench.gate.sweep()
    assert out.key not in bench.gate.artifacts
    assert orphan.key not in bench.gate.artifacts  # never completed: aborted after 24 h
    assert inp.key in bench.gate.artifacts
    with pytest.raises(GateError) as e:
        await bench.buyer.gate.download_artifact(out.key)
    assert e.value.code == "artifact_not_found"
    bench.gate.advance(timedelta(days=7))
    await bench.gate.sweep()
    assert inp.key not in bench.gate.artifacts


# --------------------------------------------------------------------------- review regressions


async def test_an_output_cannot_be_completed_once_its_job_closed(bench: Bench) -> None:
    """The platform gate refuses (and aborts) an output completed after its job closed: a
    seller cannot attach outputs to a job the buyer canceled or that failed."""
    data = os.urandom(100)
    up = await bench.worker.create_artifact(m.ArtifactCreate(size=100, job_id=bench.job.job_id))
    async with await bench.raw() as http:
        r = await http.put(up.parts[0].url, content=data)
        assert r.status_code == 200
    await bench.buyer.gate.cancel(bench.job.job_id)
    done = m.ArtifactComplete(
        upload_id=up.upload_id, etags=[], ciphertext_sha256=hashlib.sha256(data).hexdigest()
    )
    with pytest.raises(GateError) as e:
        await bench.worker.complete_artifact(up.key, done)
    assert (e.value.status, e.value.code) == (409, "not_open")
    assert up.key not in bench.gate.artifacts
    with pytest.raises(GateError) as e:
        await bench.buyer.gate.download_artifact(up.key)
    assert e.value.code == "artifact_not_found"


async def test_malformed_completions_and_urls_are_refusals_not_crashes(bench: Bench) -> None:
    up = await bench.worker.create_artifact(m.ArtifactCreate(size=10, job_id=bench.job.job_id))
    for bad in ("é" * 64, "A" * 64, "0" * 63):
        with pytest.raises(GateError) as e:
            await bench.worker.complete_artifact(
                up.key, m.ArtifactComplete(upload_id=None, etags=[], ciphertext_sha256=bad)
            )
        assert (e.value.status, e.value.code) == (422, "invalid_request")
    async with await bench.raw() as http:
        r = await http.put(up.parts[0].url.replace("sig=", "sig=%C3%A9"), content=b"x" * 10)
        assert (r.status_code, r.json()["code"]) == (403, "signature_mismatch")
    for bad_create in (
        m.ArtifactCreate(size=0, job_id=bench.job.job_id),
        m.ArtifactCreate(size=1, content_type="text/plain\r\nX: 1"),
        m.ArtifactCreate(size=1, content_type="tëxt/plain"),
    ):
        with pytest.raises(GateError) as e:
            await bench.worker.create_artifact(bad_create)
        assert (e.value.status, e.value.code) == (422, "invalid_request")


async def test_a_blob_upload_url_is_not_invalidated_by_another_request(bench: Bench) -> None:
    """The PUT is checked against the length its own URL was signed for; another party
    asking for the same hash with another size changes nothing for the first uploader."""
    data = os.urandom(3000)
    digest = hashlib.sha256(data).hexdigest()
    first = await bench.worker.create_blob(m.BlobCreate(sha256=digest, size=len(data)))
    await bench.stranger.gate.create_blob(m.BlobCreate(sha256=digest, size=7))
    async with await bench.raw() as http:
        r = await http.put(first.url, content=data)
        assert r.status_code == 200, r.text
    assert await GateBlobStore(bench.buyer.gate).get(digest) == data
