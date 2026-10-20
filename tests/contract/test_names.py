"""The principals the Temporal authorizer knows, from the Python side (decisions I30).

``temporal/authorizer/testdata/principals.json`` is the one file both languages read: the Go
authorizer's ``TestPrincipalsGolden`` checks ``RelaySubject`` and its constants against it,
and this file checks :mod:`kontra.contract.names` against the same vectors, so the gate's
construction of a relay subject (``names.relay_subject``) and the authorizer's cannot drift
apart. It also pins the workflow-stream handler names against the installed ``temporalio``:
they are Public Preview, and a rename upstream would otherwise silently deny every relay.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from kontra.contract import names
from kontra.contract.names import (
    PLATFORM_NAMESPACE,
    RESERVED_HANDLES,
    STREAM_OFFSET_QUERY,
    STREAM_POLL_UPDATE,
    WORKER_CONTROLLER_SUBJECT,
    NameError_,
    is_handle,
    job_workflow_id,
    relay_subject,
)

GOLDEN = Path(__file__).resolve().parents[2] / "temporal/authorizer/testdata/principals.json"


@pytest.fixture(scope="module")
def golden() -> dict[str, Any]:
    data: dict[str, Any] = json.loads(GOLDEN.read_text())
    assert set(data) == {
        "platform_namespace",
        "relay",
        "stream_offset_query",
        "stream_poll_update",
        "worker_controller_subject",
    }
    assert data["relay"], "the golden file has no relay vectors"
    return data


def test_relay_subject_reproduces_the_golden_vectors(golden: dict[str, Any]) -> None:
    for v in golden["relay"]:
        assert job_workflow_id(v["job_id"]) == v["workflow_id"]
        assert relay_subject(v["namespace"], v["job_id"]) == v["subject"]


def test_constants_equal_the_golden_values(golden: dict[str, Any]) -> None:
    assert golden["platform_namespace"] == PLATFORM_NAMESPACE
    assert golden["worker_controller_subject"] == WORKER_CONTROLLER_SUBJECT
    assert golden["stream_poll_update"] == STREAM_POLL_UPDATE
    assert golden["stream_offset_query"] == STREAM_OFFSET_QUERY


def test_the_platform_namespace_is_a_reserved_handle() -> None:
    """No party can ever register it, so the controller's namespace is nobody's."""
    assert is_handle(PLATFORM_NAMESPACE)
    assert PLATFORM_NAMESPACE in RESERVED_HANDLES


def test_non_party_subjects_are_not_handles(golden: dict[str, Any]) -> None:
    """A party token's ``sub`` is its handle, so no party can hold one of these."""
    subjects = [WORKER_CONTROLLER_SUBJECT, *(v["subject"] for v in golden["relay"])]
    for subject in subjects:
        assert "/" in subject
        assert not is_handle(subject)


@pytest.mark.parametrize(
    ("namespace", "job_id"),
    [
        (PLATFORM_NAMESPACE, "0" * 32),  # reserved
        ("temporal-system", "0" * 32),  # reserved
        ("", "0" * 32),
        ("acme/job-" + "0" * 32, "0" * 32),  # would let one subject split two ways
        ("Acme", "0" * 32),
        ("acme\n", "0" * 32),
        ("acme", "job-" + "0" * 32),  # a workflow id is not a job id
        ("acme", "0" * 31),
        ("acme", "G" * 32),
        ("acme", "0" * 32 + "\n"),
    ],
)
def test_relay_subject_refuses_what_is_not_a_party_and_a_job(namespace: str, job_id: str) -> None:
    with pytest.raises(NameError_):
        relay_subject(namespace, job_id)


def test_the_relay_names_are_exported() -> None:
    for name in (
        "PLATFORM_NAMESPACE",
        "STREAM_OFFSET_QUERY",
        "STREAM_POLL_UPDATE",
        "WORKER_CONTROLLER_SUBJECT",
        "relay_subject",
    ):
        assert name in names.__all__


def test_the_installed_temporalio_uses_the_same_stream_handlers(golden: dict[str, Any]) -> None:
    """The authorizer allows a relay exactly these two handler names; the stream's publish
    signal is not one of them. A temporalio that renames them fails here, not in prod."""
    from temporalio.contrib.workflow_streams import _stream  # pyright: ignore[reportPrivateUsage]

    assert golden["stream_poll_update"] == _stream._POLL_UPDATE  # pyright: ignore[reportPrivateUsage]
    assert golden["stream_offset_query"] == _stream._OFFSET_QUERY  # pyright: ignore[reportPrivateUsage]
    assert _stream._PUBLISH_SIGNAL not in (STREAM_POLL_UPDATE, STREAM_OFFSET_QUERY)  # pyright: ignore[reportPrivateUsage]
