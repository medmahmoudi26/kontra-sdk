"""The ``kontra-job`` header: plain JSON, validated on the way in, never trusted otherwise."""

from __future__ import annotations

from temporalio.api.common.v1 import Payload

from kontra.contract.names import new_job_id
from kontra.serve import JOB_HEADER
from kontra.serve.interceptors import job_from_headers, job_header


def test_the_header_round_trips() -> None:
    job = new_job_id()
    p = job_header(job)
    assert p.metadata["encoding"] == b"json/plain"
    assert job_from_headers({JOB_HEADER: p}) == job


def test_anything_else_is_ignored() -> None:
    def plain(data: bytes) -> Payload:
        return Payload(metadata={"encoding": b"json/plain"}, data=data)

    for headers in (
        {},
        {JOB_HEADER: plain(b'"job-123"')},
        {JOB_HEADER: plain(b'"' + b"a" * 31 + b'G"')},
        {JOB_HEADER: plain(b'"' + b"a" * 32 + b'\\n"')},
        {JOB_HEADER: plain(b"12")},
        {JOB_HEADER: plain(b"{")},
        {JOB_HEADER: plain(b'"' + b"a" * 100 + b'"')},
        {JOB_HEADER: Payload(metadata={"encoding": b"binary/kontra-age"}, data=b"x")},
        {"other": job_header(new_job_id())},
    ):
        assert job_from_headers(headers) is None, headers
