"""kontra._jobkeys (decisions I25): the proof that a share's sender holds the job's key, the
shape of a wrap a gate stores, and wrapping and unwrapping a job key for a device -- what
both gates and both clients share."""

from __future__ import annotations

import base64
import shutil
import subprocess
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pyrage
import pytest
from pyrage import x25519

from kontra import _jobkeys as jk
from kontra.codec import age
from kontra.gate_client import models as m

JOB = "0123456789abcdef0123456789abcdef"
OTHER_JOB = "f" * 32
SESSION = "a" * 32
KEY = jk.proof_key(b"the gate's signing secret")
#: The start of a bucket, so moving within it is exact.
T0 = datetime.fromtimestamp(1_800_000_000 // jk.BUCKET_SECONDS * jk.BUCKET_SECONDS, UTC)


@pytest.fixture
def job() -> tuple[str, str]:
    return age.generate_identity()


@pytest.fixture
def device() -> tuple[str, str]:
    return age.generate_identity()


# --------------------------------------------------------------------------- the proof


def test_a_challenge_opens_to_this_sessions_proof(job: tuple[str, str]) -> None:
    secret, public = job
    challenge = jk.make_challenge(KEY, JOB, SESSION, public, T0)
    assert len(base64.b64decode(challenge)) <= m.CHALLENGE_MAX
    proof = jk.open_challenge(challenge, secret)
    assert proof == jk.expected_proof(KEY, JOB, SESSION, int(T0.timestamp()) // 600)
    assert jk.check_proof(KEY, JOB, SESSION, proof, T0)


def test_the_previous_bucket_is_good_and_older_ones_are_not(job: tuple[str, str]) -> None:
    secret, public = job
    proof = jk.open_challenge(jk.make_challenge(KEY, JOB, SESSION, public, T0), secret)
    bucket = timedelta(seconds=jk.BUCKET_SECONDS)
    assert jk.check_proof(KEY, JOB, SESSION, proof, T0 + bucket - timedelta(seconds=1))
    assert jk.check_proof(KEY, JOB, SESSION, proof, T0 + 2 * bucket - timedelta(seconds=1))
    assert not jk.check_proof(KEY, JOB, SESSION, proof, T0 + 2 * bucket)
    assert not jk.check_proof(KEY, JOB, SESSION, proof, T0 - timedelta(seconds=1))  # the future


@pytest.mark.parametrize(
    "what",
    ["another job", "another session", "a rotated key", "tampered", "malformed", "padded"],
)
def test_a_proof_is_refused(what: str, job: tuple[str, str]) -> None:
    secret, public = job
    proof = jk.open_challenge(jk.make_challenge(KEY, JOB, SESSION, public, T0), secret)
    key, job_id, session = KEY, JOB, SESSION
    if what == "another job":
        job_id = OTHER_JOB
    elif what == "another session":
        session = "b" * 32
    elif what == "a rotated key":
        key = jk.proof_key(b"the next signing secret")
    elif what == "tampered":
        proof = ("B" if proof[0] != "B" else "C") + proof[1:]
    elif what == "malformed":
        proof = proof[:-1] + "="
    else:
        proof += "="
    assert not jk.check_proof(key, job_id, session, proof, T0)


def test_a_challenge_opens_with_the_jobs_key_only(job: tuple[str, str]) -> None:
    _, public = job
    challenge = jk.make_challenge(KEY, JOB, SESSION, public, T0)
    with pytest.raises(jk.InvalidWrap, match="does not open"):
        jk.open_challenge(challenge, age.generate_identity()[0])
    with pytest.raises(jk.InvalidWrap, match="base64"):
        jk.open_challenge("not base64!", job[0])
    with pytest.raises(jk.InvalidWrap, match="too large"):
        jk.open_challenge(base64.b64encode(b"x" * (m.CHALLENGE_MAX + 1)).decode(), job[0])
    not_a_proof = base64.b64encode(age.encrypt(b"hello", [public])).decode()
    with pytest.raises(jk.InvalidWrap, match="no proof"):
        jk.open_challenge(not_a_proof, job[0])


def test_the_proof_key_needs_a_secret() -> None:
    with pytest.raises(ValueError, match="secret"):
        jk.proof_key(b"")
    assert jk.proof_key(b"a") != jk.proof_key(b"b")


# --------------------------------------------------------------------------- the wrap


def test_a_wrap_round_trips_and_is_bound_to_its_job(
    job: tuple[str, str], device: tuple[str, str]
) -> None:
    secret, public = job
    wrap = jk.wrap_job_key(JOB, secret, device[1])
    raw = jk.check_wrap_shape(wrap)
    assert len(raw) <= m.WRAP_MAX
    assert b"AGE-SECRET-KEY" not in raw.upper()
    assert jk.unwrap_job_key(wrap, device[0], public) == secret
    with pytest.raises(jk.JobKeyMismatch):
        jk.unwrap_job_key(wrap, device[0], age.generate_identity()[1])  # moved to another job
    with pytest.raises(jk.InvalidWrap, match="does not open"):
        jk.unwrap_job_key(wrap, age.generate_identity()[0], public)  # another device


def test_the_plaintext_is_the_cli_key_file(job: tuple[str, str], device: tuple[str, str]) -> None:
    """``age -d -i ~/.kontra/key wrap > ~/.kontra/jobs/<id>.key`` works by hand: the plaintext
    is the key file (rage reads it, as the age CLI does)."""
    secret, public = job
    wrap = jk.wrap_job_key(JOB, secret, device[1])
    plain = pyrage.decrypt(base64.b64decode(wrap), [x25519.Identity.from_str(device[0])])
    assert plain.decode() == f"# job: {JOB}\n# public key: {public}\n{secret}\n"
    assert plain.decode() == jk.key_file_text(JOB, secret)


@pytest.mark.skipif(shutil.which("age") is None, reason="no age CLI on this machine")
def test_the_age_cli_opens_a_wrap(
    tmp_path: Path, job: tuple[str, str], device: tuple[str, str]
) -> None:
    key = tmp_path / "key"
    key.write_text(device[0] + "\n")
    wrap = tmp_path / "wrap"
    wrap.write_bytes(base64.b64decode(jk.wrap_job_key(JOB, job[0], device[1])))
    out = subprocess.run(
        ["age", "-d", "-i", str(key), str(wrap)], check=True, capture_output=True, text=True
    )
    assert out.stdout == jk.key_file_text(JOB, job[0])


def test_a_wrap_made_by_rage_opens_too(job: tuple[str, str], device: tuple[str, str]) -> None:
    """Another conforming writer (the console's typage writes no grease; rage does, and its
    files are refused by the shape -- see below -- but open all the same)."""
    secret, public = job
    sealed = pyrage.encrypt(
        jk.key_file_text(JOB, secret).encode(), [x25519.Recipient.from_str(device[1])]
    )
    plain = age.decrypt(sealed, device[0]).decode()
    assert plain.endswith(f"{secret}\n")
    del public


def _wrap_bytes(job: tuple[str, str], device: tuple[str, str]) -> bytes:
    return base64.b64decode(jk.wrap_job_key(JOB, job[0], device[1]))


@pytest.mark.parametrize(
    "what",
    [
        "two stanzas",
        "a passphrase stanza",
        "another stanza type",
        "a grease stanza",
        "oversize",
        "no MAC line",
        "no payload",
        "bad base64",
        "a non-canonical share",
        "the key file in the clear",
    ],
)
def test_a_wrap_of_the_wrong_shape_is_refused(
    what: str, job: tuple[str, str], device: tuple[str, str]
) -> None:
    good = _wrap_bytes(job, device)
    text = jk.key_file_text(JOB, job[0]).encode()
    header, payload = good[:168], good[168:]
    if what == "two stanzas":
        bad = age.encrypt(text, [device[1], age.generate_identity()[1]])
    elif what == "a passphrase stanza":
        bad = header.replace(b"-> X25519 ", b"-> scrypt ")[:-1] + b"\n" + payload
    elif what == "another stanza type":
        bad = header.replace(b"-> X25519 ", b"-> ssh-ed ") + payload
    elif what == "a grease stanza":
        bad = pyrage.encrypt(text, [x25519.Recipient.from_str(device[1])])
    elif what == "oversize":
        bad = good + b"\0" * (m.WRAP_MAX + 1 - len(good))
    elif what == "no MAC line":
        bad = header.replace(b"--- ", b"+++ ") + payload
    elif what == "no payload":
        bad = header + payload[:20]
    elif what == "a non-canonical share":
        share = header[32:75]
        last = share[-1:]
        alphabet = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
        bumped = alphabet[(alphabet.index(last) | 1) % 64 : (alphabet.index(last) | 1) % 64 + 1]
        bad = (
            header[:32] + share[:-1] + (bumped if bumped != last else b"B") + header[75:] + payload
        )
    else:
        bad = text
    wrap = base64.b64encode(bad).decode()
    if what == "bad base64":
        wrap = wrap[:-4] + "!!!!"
    with pytest.raises(jk.InvalidWrap):
        jk.check_wrap_shape(wrap)


def test_a_wrap_holding_two_keys_is_refused(job: tuple[str, str], device: tuple[str, str]) -> None:
    two = f"{jk.key_file_text(JOB, job[0])}{age.generate_identity()[0]}\n"
    wrap = base64.b64encode(age.encrypt(two.encode(), [device[1]])).decode()
    with pytest.raises(jk.InvalidWrap, match="exactly one"):
        jk.unwrap_job_key(wrap, device[0], job[1])
    nothing = base64.b64encode(age.encrypt(b"# just a comment\n", [device[1]])).decode()
    with pytest.raises(jk.InvalidWrap, match="exactly one"):
        jk.unwrap_job_key(nothing, device[0], job[1])


# --------------------------------------------------------------------------- the notices


def test_the_notices_quote_the_label_and_carry_no_link() -> None:
    at = datetime(2026, 10, 17, 14, 2, tzinfo=UTC)
    for text in (
        jk.device_added_mail(
            'Work "laptop" \\',
            "browser",
            at,
            via="a link by email",
            signed_in=at,
            session_prefix="a1b2c3d4e5f6",
        ),
        jk.device_removed_mail(
            'Work "laptop" \\', "cli", at, via="GitHub", signed_in=at, session_prefix="a1b2c3d4e5f6"
        ),
    ):
        assert '"Work \\"laptop\\" \\\\"' in text
        assert "a1b2c3d4e5f6" in text
        assert "14:02 UTC" in text
        assert "http" not in text
    assert "(a browser)" in jk.device_added_mail(
        "x", "browser", at, via="v", signed_in=at, session_prefix="0" * 12
    )
