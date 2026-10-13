"""``HoldVerifier``: ES256 against the gate's JWKS, offline, refetched only for a new kid."""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import ec
from jwt.algorithms import ECAlgorithm

from kontra.codec.age import generate_identity
from kontra.contract.names import new_job_id
from kontra.serve import HoldRefused, HoldVerifier
from kontra.serve.hold import JWKS_FILE


class Signer:
    def __init__(self, kid: str) -> None:
        self.kid = kid
        self.key = ec.generate_private_key(ec.SECP256R1())

    def jwk(self) -> dict[str, Any]:
        jwk: dict[str, Any] = json.loads(ECAlgorithm.to_jwk(self.key.public_key()))
        return {**jwk, "kid": self.kid, "use": "sig", "alg": "ES256"}

    def sign(self, claims: dict[str, Any], **header: Any) -> str:
        return jwt.encode(claims, self.key, algorithm="ES256", headers={"kid": self.kid, **header})


class JwksGate:
    def __init__(self, *signers: Signer) -> None:
        self.signers = list(signers)
        self.calls = 0
        self.down = False

    async def jwks(self) -> dict[str, Any]:
        self.calls += 1
        if self.down:
            raise ConnectionError("gate down")
        return {"keys": [s.jwk() for s in self.signers]}


def claims(**changes: Any) -> dict[str, Any]:
    job = new_job_id()
    now = int(time.time())
    return {
        "iss": "http://gate",
        "aud": "kontra:hold",
        "sub": job,
        "iat": now,
        "exp": now + 600,
        "job_id": job,
        "offering": "acme/upper@1.0.0",
        "seller": "acme",
        "seller_ns": "acme",
        "buyer": "bob",
        "buyer_ns": "bob",
        "buyer_key": generate_identity()[1],
        "amount_minor": 100,
        "currency": "USD",
        "contract_hash": "sha256:" + "0" * 64,
        "max_duration_seconds": 600,
        **changes,
    }


async def test_a_gate_signed_token_verifies(tmp_path: Path) -> None:
    gate_key = Signer("k1")
    v = HoldVerifier(JwksGate(gate_key), home=tmp_path)
    await v.load()
    c = claims()
    got = await v.verify(gate_key.sign(c))
    assert got.job_id == c["job_id"]
    assert got.amount_minor == 100
    assert v.key_ids == {"k1"}
    cached = tmp_path / JWKS_FILE
    assert json.loads(cached.read_text())["keys"][0]["kid"] == "k1"
    assert cached.stat().st_mode & 0o777 == 0o600


@pytest.mark.parametrize(
    ("token", "message"),
    [
        (None, "execute needs the kontra-hold header"),
        ("", "execute needs the kontra-hold header"),
        ("x" * 9000, "too large"),
        ("not.a.jwt", "not a JWT"),
    ],
)
async def test_junk_is_refused(token: Any, message: str) -> None:
    v = HoldVerifier(JwksGate(Signer("k1")))
    await v.load()
    with pytest.raises(HoldRefused, match=message):
        await v.verify(token)


async def test_forgeries_are_refused_without_asking_the_gate() -> None:
    gate_key, other = Signer("k1"), Signer("k1")
    gate = JwksGate(gate_key)
    v = HoldVerifier(gate)
    await v.load()
    c = claims()
    good = gate_key.sign(c)
    head, _, sig = good.split(".")
    tampered = f"{head}.{jwt.utils.base64url_encode(json.dumps({**c, 'amount_minor': 1}).encode()).decode()}.{sig}"
    cases = {
        tampered: "signature does not verify",
        other.sign(c): "signature does not verify",  # the gate's kid, another key
        jwt.encode(c, "s" * 32, algorithm="HS256", headers={"kid": "k1"}): "not signed with ES256",
        jwt.encode(c, None, algorithm="none", headers={"kid": "k1"}): "not signed with ES256",
        gate_key.sign({**c, "aud": "kontra:api"}): "not a hold token",
        jwt.encode(c, gate_key.key, algorithm="ES256"): "names no signing key",
        gate_key.sign(
            {k: v for k, v in c.items() if k != "seller_ns"}
        ): "malformed claims (seller_ns)",
        gate_key.sign({k: v for k, v in c.items() if k != "exp"}): "MissingRequiredClaimError",
        gate_key.sign({**c, "extra": 1}): "malformed claims (extra)",
    }
    for token, message in cases.items():
        with pytest.raises(HoldRefused) as refused:
            await v.verify(token)
        assert message in str(refused.value), (message, refused.value)
    assert gate.calls == 1  # the load: no re-fetch for a kid we know


async def test_an_unknown_kid_refetches_once_per_interval() -> None:
    now = [1000.0]
    old, new = Signer("old"), Signer("new")
    gate = JwksGate(old)
    v = HoldVerifier(gate, refetch_interval=60, clock=lambda: now[0])
    await v.load()
    gate.signers = [old, new]  # the gate rotated its key just after we fetched
    with pytest.raises(HoldRefused, match="a key the gate does not publish"):
        await v.verify(new.sign(claims()))
    assert gate.calls == 1  # too soon after the last fetch
    now[0] += 61
    assert (await v.verify(new.sign(claims()))).seller == "acme"
    assert gate.calls == 2
    for _ in range(5):  # junk kids cannot make the handler hammer the gate
        with pytest.raises(HoldRefused):
            await v.verify(Signer("junk").sign(claims()))
    assert gate.calls == 2
    now[0] += 61
    with pytest.raises(HoldRefused):
        await v.verify(Signer("junk").sign(claims()))
    assert gate.calls == 3
    gate.down = True
    now[0] += 61
    with pytest.raises(HoldRefused):  # a failed refetch keeps the keys we have
        await v.verify(Signer("junk").sign(claims()))
    assert await v.verify(old.sign(claims()))


async def test_the_cached_jwks_is_the_fallback_when_the_gate_is_down(tmp_path: Path) -> None:
    gate_key = Signer("k1")
    gate = JwksGate(gate_key)
    await HoldVerifier(gate, home=tmp_path).load()
    gate.down = True
    v = HoldVerifier(gate, home=tmp_path)
    await v.load()
    assert (await v.verify(gate_key.sign(claims()))).offering == "acme/upper@1.0.0"
    with pytest.raises(RuntimeError, match="no usable cached copy"):
        await HoldVerifier(gate, home=tmp_path / "empty").load()
    with pytest.raises(RuntimeError, match="cannot verify hold tokens"):
        await HoldVerifier(gate).load()


async def test_a_cache_others_could_have_written_is_ignored(tmp_path: Path) -> None:
    gate_key = Signer("k1")
    gate = JwksGate(gate_key)
    await HoldVerifier(gate, home=tmp_path).load()
    os.chmod(tmp_path / JWKS_FILE, 0o666)  # anyone could plant a key here
    gate.down = True
    with pytest.raises(RuntimeError, match="no usable cached copy"):
        await HoldVerifier(gate, home=tmp_path).load()


async def test_a_jwks_without_a_usable_key_is_refused() -> None:
    class Odd(JwksGate):
        async def jwks(self) -> dict[str, Any]:
            self.calls += 1
            rsa_like = {"kty": "RSA", "kid": "r", "n": "x", "e": "AQAB"}
            private = {**Signer("p").jwk(), "d": "secret"}
            return {"keys": [rsa_like, private, "junk"]}

    with pytest.raises(RuntimeError, match="no ES256 P-256 signing key"):
        await HoldVerifier(Odd()).load()
