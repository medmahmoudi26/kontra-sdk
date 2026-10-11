"""Offline verification of hold tokens (D10, §10.5, decisions I10).

A hold token is an ES256 JWT the gate signs when it locks a buyer's money. The seller's
handler verifies it before starting a job, *without calling the gate*: the job-start path
never depends on the gate being up, and a forged or altered token costs the forger nothing
but a refusal.

The keys are the gate's JWKS, fetched when the worker starts and kept in memory and in
``$KONTRA_HOME/jwks.json`` (the fallback when the gate is unreachable at start). A token
naming a key id we do not know triggers one re-fetch, at most once per
``refetch_interval``: a key rotation is picked up, and a stream of junk tokens cannot turn
the handler into a JWKS-fetching amplifier against the gate.

The claim checks that need the served offering (seller, offering, contract hash, expiry)
live in :mod:`kontra.serve.handler`; this module answers one question: did the gate sign
this, for holds?
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any, Protocol

import jwt
from pydantic import ValidationError

from kontra.gate_client import models as m
from kontra.profile import _read_private, _write_private  # pyright: ignore[reportPrivateUsage]

__all__ = ["HOLD_AUDIENCE", "HOLD_HEADER", "JWKS_FILE", "HoldRefused", "HoldVerifier", "JwksGate"]

log = logging.getLogger("kontra.serve.hold")

#: The Nexus header carrying the hold token (D10).
HOLD_HEADER = "kontra-hold"
HOLD_AUDIENCE = "kontra:hold"
JWKS_FILE = "jwks.json"
#: A hold token is a few hundred bytes; anything far larger is not one.
_MAX_TOKEN = 8 * 1024
_MAX_KID = 128
_MAX_JWKS = 256 * 1024


class HoldRefused(Exception):
    """The token does not prove a hold for this job. The message is safe to send back to the
    caller: it names what failed, never the token or the input."""


class JwksGate(Protocol):
    async def jwks(self) -> dict[str, Any]: ...


def _keys_from(jwks: object) -> dict[str, jwt.PyJWK]:
    """``kid -> key`` for the P-256 signing keys of a JWKS; everything else is ignored."""
    if not isinstance(jwks, Mapping):
        raise ValueError("a JWKS is a JSON object")
    raw_keys = jwks.get("keys")  # pyright: ignore[reportUnknownMemberType]
    if not isinstance(raw_keys, list):
        raise ValueError("a JWKS has a list of keys")
    out: dict[str, jwt.PyJWK] = {}
    for raw in raw_keys:  # pyright: ignore[reportUnknownVariableType]
        if not isinstance(raw, Mapping):
            continue
        jwk: dict[str, Any] = {str(k): v for k, v in raw.items()}  # pyright: ignore[reportUnknownVariableType]
        kid = jwk.get("kid")
        if (
            not isinstance(kid, str)
            or not 0 < len(kid) <= _MAX_KID
            or jwk.get("kty") != "EC"
            or jwk.get("crv") != "P-256"
            or jwk.get("alg", "ES256") != "ES256"
            or jwk.get("use", "sig") != "sig"
            or "d" in jwk  # a private key has no business in a JWKS; refuse to use it
        ):
            continue
        try:
            out[kid] = jwt.PyJWK.from_dict(jwk, algorithm="ES256")
        except jwt.PyJWTError:
            continue
    return out


class HoldVerifier:
    """``await verifier.load()`` once (worker start), then ``await verifier.verify(token)``.

    ``home`` is where ``jwks.json`` is cached (None: memory only). ``clock`` returns Unix
    seconds; tests move it.
    """

    def __init__(
        self,
        gate: JwksGate,
        *,
        home: Path | None = None,
        refetch_interval: float = 300.0,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._gate = gate
        self._home = home
        self._refetch_interval = refetch_interval
        self._clock = clock
        self._keys: dict[str, jwt.PyJWK] = {}
        self._fetched_at: float | None = None
        self._lock = asyncio.Lock()

    @property
    def key_ids(self) -> frozenset[str]:
        return frozenset(self._keys)

    @property
    def cache_path(self) -> Path | None:
        return self._home / JWKS_FILE if self._home is not None else None

    async def load(self) -> None:
        """Fetch the JWKS from the gate (and cache it), or fall back to the cached copy.
        Raises if neither yields a usable key: a worker that cannot verify holds must not
        accept jobs."""
        try:
            await self._fetch()
            return
        except Exception as exc:
            gate_error = exc
        cached = self._read_cache()
        if not cached:
            raise RuntimeError(
                f"cannot verify hold tokens: the gate's JWKS could not be fetched ({gate_error}) "
                f"and there is no usable cached copy at {self.cache_path}"
            ) from gate_error
        log.warning(
            "the gate's JWKS could not be fetched (%s); using the cached copy in %s",
            gate_error,
            self.cache_path,
        )
        self._keys = cached

    async def _fetch(self) -> None:
        self._fetched_at = self._clock()
        jwks = await self._gate.jwks()
        keys = _keys_from(jwks)
        if not keys:
            raise ValueError("the gate's JWKS has no ES256 P-256 signing key")
        self._keys = keys
        self._write_cache(jwks)

    def _read_cache(self) -> dict[str, jwt.PyJWK]:
        path = self.cache_path
        if path is None:
            return {}
        try:
            raw = _read_private(path)
        except FileNotFoundError:
            return {}
        except Exception as exc:  # unsafe owner/mode, unreadable: never trust it
            log.warning("ignoring the cached JWKS at %s: %s", path, exc)
            return {}
        try:
            return _keys_from(json.loads(raw[:_MAX_JWKS]))
        except ValueError as exc:
            log.warning("ignoring the cached JWKS at %s: %s", path, exc)
            return {}

    def _write_cache(self, jwks: Mapping[str, Any]) -> None:
        path = self.cache_path
        if path is None:
            return
        try:
            path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            _write_private(path, json.dumps(jwks, sort_keys=True).encode(), replace=True)
        except OSError as exc:  # the in-memory keys still work
            log.warning("could not cache the gate's JWKS in %s: %s", path, exc)

    async def _key(self, kid: str) -> jwt.PyJWK:
        key = self._keys.get(kid)
        if key is not None:
            return key
        async with self._lock:
            key = self._keys.get(kid)
            if key is not None:
                return key
            last = self._fetched_at
            if last is None or self._clock() - last >= self._refetch_interval:
                try:
                    await self._fetch()
                except Exception as exc:
                    log.warning("re-fetching the gate's JWKS for key %r failed: %s", kid, exc)
                key = self._keys.get(kid)
        if key is None:
            raise HoldRefused("hold token refused: it is signed by a key the gate does not publish")
        return key

    async def verify(self, token: object) -> m.HoldClaims:
        """The token's claims if the gate signed it for a hold, else :class:`HoldRefused`.
        Expiry, seller and contract are the caller's to check (they depend on what is
        served); this checks the signature, the algorithm and the audience."""
        if not isinstance(token, str) or not token:
            raise HoldRefused(f"no hold token: execute needs the {HOLD_HEADER} header")
        if len(token) > _MAX_TOKEN:
            raise HoldRefused("hold token refused: it is too large to be one")
        try:
            header = jwt.get_unverified_header(token)
        except jwt.PyJWTError:
            raise HoldRefused("hold token refused: it is not a JWT") from None
        if header.get("alg") != "ES256":
            raise HoldRefused("hold token refused: it is not signed with ES256")
        kid = header.get("kid")
        if not isinstance(kid, str) or not 0 < len(kid) <= _MAX_KID:
            raise HoldRefused("hold token refused: it names no signing key")
        key = await self._key(kid)
        try:
            claims: dict[str, Any] = jwt.decode(
                token,
                key.key,
                algorithms=["ES256"],
                audience=HOLD_AUDIENCE,
                # Expiry is checked by the handler against its own clock, with the message
                # a seller and buyer can act on; iat/nbf are not: clock skew must not refuse.
                options={
                    "verify_exp": False,
                    "verify_iat": False,
                    "verify_nbf": False,
                    "require": ["aud", "exp", "iat", "sub"],
                },
            )
        except jwt.InvalidAudienceError:
            raise HoldRefused("hold token refused: it is not a hold token (audience)") from None
        except jwt.InvalidSignatureError:
            raise HoldRefused("hold token refused: the signature does not verify") from None
        except jwt.PyJWTError as exc:
            raise HoldRefused(f"hold token refused: {type(exc).__name__}") from None
        try:
            return m.HoldClaims.model_validate(claims)
        except ValidationError as exc:
            fields = sorted({".".join(map(str, e["loc"])) or "claims" for e in exc.errors()})
            raise HoldRefused(
                f"hold token refused: malformed claims ({', '.join(fields)})"
            ) from None
