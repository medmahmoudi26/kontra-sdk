"""The job directory (buyer keys, gate on a miss) and the seller-key cache."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import pytest

from kontra.codec.age import generate_identity
from kontra.contract.names import new_job_id
from kontra.gate_client import GateError
from kontra.gate_client import models as m
from kontra.serve import JobDirectory, SellerKeyCache

BUYER = generate_identity()[1]
HASH = "sha256:" + "0" * 64


class SellerJobs:
    def __init__(self, answers: dict[str, m.SellerJobView]) -> None:
        self.answers = answers
        self.calls: list[str] = []

    async def seller_job(self, job_id: str) -> m.SellerJobView:
        self.calls.append(job_id)
        await asyncio.sleep(0.01)
        if job_id not in self.answers:
            raise GateError(404, "job_not_found", "no such job")
        return self.answers[job_id]


def view(job_id: str, key: str = BUYER) -> m.SellerJobView:
    return m.SellerJobView(
        job_id=job_id,
        offering="acme/upper@1.0.0",
        buyer_public_key=key,
        contract_hash=HASH,
        max_duration_seconds=60,
    )


async def test_recorded_jobs_need_no_gate() -> None:
    gate = SellerJobs({})
    jobs = JobDirectory(gate)
    job = new_job_id()
    jobs.record(job, BUYER, HASH)
    assert await jobs.buyer_key(job) == BUYER
    assert (await jobs.lookup(job)).contract_hash == HASH
    assert gate.calls == []


async def test_a_miss_asks_the_gate_once_however_many_ask() -> None:
    job = new_job_id()
    gate = SellerJobs({job: view(job)})
    jobs = JobDirectory(gate)
    keys = await asyncio.gather(*(jobs.buyer_key(job) for _ in range(10)))
    assert keys == [BUYER] * 10
    assert gate.calls == [job]
    assert await jobs.buyer_key(job) == BUYER


async def test_one_caller_giving_up_does_not_cancel_the_others() -> None:
    """The gate lookup is shared, not owned by whoever asked first: a progress send that
    times out must not turn the codec's concurrent lookup (encrypting the job's result)
    into a CancelledError."""
    job = new_job_id()
    gate = SellerJobs({job: view(job)})
    jobs = JobDirectory(gate)
    first = asyncio.create_task(jobs.buyer_key(job))
    await asyncio.sleep(0)  # the first caller started the lookup
    second = asyncio.create_task(jobs.buyer_key(job))
    await asyncio.sleep(0)
    first.cancel()
    assert await second == BUYER
    with pytest.raises(asyncio.CancelledError):
        await first
    assert gate.calls == [job]
    assert jobs.cached(job) is not None
    assert gate.calls == [job]


async def test_misses_that_fail_are_not_cached() -> None:
    gate = SellerJobs({})
    jobs = JobDirectory(gate)
    job = new_job_id()
    with pytest.raises(GateError):
        await jobs.buyer_key(job)
    gate.answers[job] = view(job)
    assert await jobs.buyer_key(job) == BUYER
    assert len(gate.calls) == 2


async def test_bad_answers_and_bad_ids_are_refused() -> None:
    a, b = new_job_id(), new_job_id()
    gate = SellerJobs({a: view(b), b: view(b, key="not-a-key")})
    jobs = JobDirectory(gate)
    with pytest.raises(LookupError, match="answered for job"):
        await jobs.lookup(a)
    with pytest.raises(ValueError, match="not an age X25519 recipient"):
        await jobs.lookup(b)
    with pytest.raises(LookupError, match="not a job id"):
        await jobs.lookup("job-../../x")
    with pytest.raises(ValueError, match="not a job id"):
        jobs.record("nope", BUYER, HASH)
    with pytest.raises(ValueError, match="recipient"):
        jobs.record(new_job_id(), "age1nope", HASH)
    assert "job-../../x" not in gate.calls


async def test_the_directory_is_bounded() -> None:
    jobs = JobDirectory(SellerJobs({}), limit=3)
    ids = [new_job_id() for _ in range(5)]
    for j in ids:
        jobs.record(j, BUYER, HASH)
    assert len(jobs) == 3
    assert jobs.cached(ids[0]) is None
    assert jobs.cached(ids[-1]) is not None


class Endpoints:
    def __init__(self) -> None:
        self.calls = 0

    async def by_endpoint(self, endpoint: str) -> m.CatalogEntry:
        self.calls += 1
        return m.CatalogEntry(
            offering="acme/upper@1.0.0",
            party="acme",
            name="acme/upper",
            version="1.0.0",
            description="d",
            contract_hash=HASH,
            document={},
            card={},
            endpoint=endpoint if endpoint != "kliar" else "kother",
            service="acme.upper",
            task_queue="acme/upper@1.0.0",
            recipients=[BUYER],
            published_at=datetime.now(UTC),
        )


async def test_seller_keys_come_from_the_catalog_and_are_cached() -> None:
    gate = Endpoints()
    keys = SellerKeyCache(gate)
    assert await keys.seller_recipients("k" + "1" * 32) == (BUYER,)
    assert await keys.seller_recipients("k" + "1" * 32) == (BUYER,)
    assert gate.calls == 1
    with pytest.raises(LookupError):
        await keys.seller_recipients("kliar")
    keys.remember("k" + "2" * 32, [BUYER, BUYER])
    assert await keys.seller_recipients("k" + "2" * 32) == (BUYER,)
    assert gate.calls == 2
    # One key where a set belongs would iterate as characters: refused, not split.
    with pytest.raises(TypeError, match="not one key"):
        keys.remember("k" + "3" * 32, BUYER)  # type: ignore[arg-type]
