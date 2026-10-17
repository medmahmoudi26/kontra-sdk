"""Seam ②, the models of devices and job-key wraps (decisions I25): what each accepts and
refuses before a gate looks at it, and that no existing answer changed."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from kontra import _jobkeys as jk
from kontra.codec import age
from kontra.gate_client import models as m

AGE = "age1" + "q" * 58
JOB = "0" * 32


def test_a_device_label_is_printable_ascii() -> None:
    m.AddDevice(label="Firefox on Linux", public_key=AGE, kind="browser")
    for bad in ("", "x" * 65, "café", "tab\there", "line\n"):
        with pytest.raises(ValidationError):
            m.AddDevice(label=bad, public_key=AGE, kind="cli")
    with pytest.raises(ValidationError):
        m.AddDevice.model_validate({"label": "x", "public_key": AGE, "kind": "fleet"})


def test_a_proof_and_a_wrap_have_their_shapes() -> None:
    secret, _ = age.generate_identity()
    wrap = jk.wrap_job_key(JOB, secret, age.generate_identity()[1])
    m.JobKeyShare(job_id=JOB, proof="p" * 43, wraps=[m.WrapIn(recipient=AGE, wrap=wrap)])
    for proof in ("p" * 42, "p" * 44, "p" * 42 + "=", "p" * 42 + "+"):
        with pytest.raises(ValidationError):
            m.JobKeyShare(job_id=JOB, proof=proof, wraps=[])
    for bad in ("not base64!", "A" * (4 * -(-m.WRAP_MAX // 3) + 4), "-_-_"):
        with pytest.raises(ValidationError):
            m.WrapIn(recipient=AGE, wrap=bad)
    with pytest.raises(ValidationError):
        m.ShareJobKeys(
            items=[m.JobKeyShare(job_id=JOB, proof="p" * 43, wraps=[])] * (m.SHARE_ITEMS_MAX + 1)
        )
    many = [m.WrapIn(recipient=AGE, wrap=wrap)] * (m.DEVICES_MAX + 1)
    with pytest.raises(ValidationError):
        m.JobKeyShare(job_id=JOB, proof="p" * 43, wraps=many)


def test_the_proof_stays_out_of_repr() -> None:
    share = m.JobKeyShare(job_id=JOB, proof="s" * 43, wraps=[])
    assert "s" * 43 not in repr(share)


#: JobView's fields as 0.2.0-0.2.3 know them (and refuse any other).
_JOB_VIEW_0_2_3 = {
    "job_id",
    "offering",
    "buyer",
    "seller",
    "state",
    "escrow",
    "amount_minor",
    "currency",
    "price",
    "created_at",
    "started_at",
    "deadline",
    "delivery",
    "review_deadline",
    "reason",
    "operation_id",
    "workflow_id",
}


def test_no_existing_answer_gained_a_field() -> None:
    """0.2.0-0.2.3 parse every answer strictly. ``JobView.buyer_key`` (decisions I25) is
    in the JSON only when a client asks (``?include=buyer_key``): a view without it dumps
    exactly what those releases know, never a ``null`` they would refuse. Every other model
    they know is unchanged."""
    assert set(m.JobView.model_fields) == _JOB_VIEW_0_2_3 | {"buyer_key"}
    assert not m.JobView.model_fields["buyer_key"].is_required()
    from ._support import minimal

    plain = m.JobView.model_validate(minimal(m.JobView))
    assert set(plain.model_dump(mode="json")) <= _JOB_VIEW_0_2_3
    assert "buyer_key" not in plain.model_dump_json()
    asked = plain.model_copy(update={"buyer_key": "age1" + "q" * 58})
    assert asked.model_dump(mode="json")["buyer_key"] == "age1" + "q" * 58
    assert set(m.Me.model_fields) == {
        "party",
        "namespace",
        "keys",
        "temporal_address",
        "created_at",
        "dev",
    }
    assert m.JobKey.model_fields["buyer_key"].is_required()


def test_the_constants_are_one() -> None:
    assert (
        (jk.DEVICES_MAX, jk.WRAP_MAX, jk.CHALLENGE_MAX, jk.SHARE_ITEMS_MAX)
        == (
            m.DEVICES_MAX,
            m.WRAP_MAX,
            m.CHALLENGE_MAX,
            m.SHARE_ITEMS_MAX,
        )
        == (32, 1024, 512, 100)
    )
