"""How a buyer's call is tied to its job: the hold it carried, matched by
:func:`~kontra._control.hold_digest` (decisions I15). Escrow must count every call the
seller's handler accepted for the job, so every spelling of an issued hold that the
handler's verifier accepts has to match, and nothing the gate did not sign may."""

from __future__ import annotations

import base64
from typing import Any

from kontra._control import hold_digest
from kontra.serve.hold import HOLD_AUDIENCE, HoldRefused, HoldVerifier
from kontra.testing.fake_gate import _KeyRing  # pyright: ignore[reportPrivateUsage]

#: The order of the P-256 group: (r, s) and (r, N - s) are both valid ECDSA signatures.
P256_N = 0xFFFFFFFF00000000FFFFFFFFFFFFFFFFBCE6FAADA7179E84F3B9CAC2FC632551


def claims(job_id: str = "a" * 32) -> dict[str, Any]:
    return {
        "iss": "https://gate.test",
        "aud": HOLD_AUDIENCE,
        "sub": job_id,
        "iat": 1_800_000_000,
        "exp": 1_800_003_600,
        "job_id": job_id,
        "offering": "acme/enrich@1.0.0",
        "seller": "acme",
        "seller_ns": "acme",
        "buyer": "bob",
        "buyer_ns": "bob",
        "buyer_key": "age1" + "q" * 58,
        "amount_minor": 1000,
        "currency": "USD",
        "contract_hash": "sha256:" + "0" * 64,
        "max_duration_seconds": 3600,
    }


def b64(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode()


def unb64(text: str) -> bytes:
    return base64.urlsafe_b64decode(text + "=" * (-len(text) % 4))


def respellings(token: str) -> dict[str, str]:
    """Strings that are not ``token`` but that the seller's handler is handed and accepts
    as it: what a buyer can put in its call's ``kontra-hold`` header."""
    signed, _, sig = token.rpartition(".")
    raw = unb64(sig)
    other_s = raw[:32] + (P256_N - int.from_bytes(raw[32:], "big")).to_bytes(32, "big")
    return {
        "the other ES256 signature (r, N - s)": f"{signed}.{b64(other_s)}",
        "base64 padding on the signature": f"{token}==",
        "spaces and tabs HTTP drops": f" \t {token}\t ",
        "all three": f"\t{signed}.{b64(other_s)}== ",
    }


class _Gate:
    def __init__(self, ring: _KeyRing) -> None:
        self.ring = ring

    async def jwks(self) -> dict[str, Any]:
        return self.ring.jwks()


async def test_every_spelling_the_handler_accepts_matches_the_issued_hold() -> None:
    ring = _KeyRing()
    verifier = HoldVerifier(_Gate(ring))
    await verifier.load()
    issued = ring.sign(claims())
    for what, spelled in respellings(issued).items():
        assert spelled != issued
        # What the handler is handed: HTTP has dropped the spaces and tabs around it.
        handed = spelled.strip(" \t")
        assert (await verifier.verify(handed)).job_id == "a" * 32, what
        assert hold_digest(spelled) == hold_digest(issued), what


async def test_a_hold_the_gate_did_not_sign_for_this_job_does_not_match() -> None:
    ring = _KeyRing()
    issued = ring.sign(claims())
    others = {
        "another job's hold": ring.sign(claims("b" * 32)),
        "a key the gate does not publish, over the same claims": _KeyRing().sign(claims()),
        "the claims re-spelled (the signature no longer covers them)": issued.replace(".", ". ", 1),
        "a value with no signed part": "not-a-token",
        "the empty header": "",
    }
    verifier = HoldVerifier(_Gate(ring))
    await verifier.load()
    for what, value in others.items():
        assert hold_digest(value) != hold_digest(issued), what
        try:
            seen = (await verifier.verify(value.strip(" \t"))).job_id
        except HoldRefused:
            continue
        assert seen != "a" * 32, what  # the handler would not have run *this* job either


def test_a_new_signature_over_the_same_claims_is_the_same_hold() -> None:
    """``start`` re-signs the hire's claims: both tokens are the gate's statement of the
    same hold, and either in the buyer's call ties it to the job."""
    ring = _KeyRing()
    first, second = ring.sign(claims()), ring.sign(claims())
    assert first != second  # ECDSA signatures are randomized
    assert hold_digest(first) == hold_digest(second)
