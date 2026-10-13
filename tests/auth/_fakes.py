"""A just-big-enough gate for auth tests: rotating refresh tokens with reuse detection,
expiring access tokens, Temporal tokens, /me, the device flow and dev login."""

from __future__ import annotations

import asyncio
import base64
import json
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx

from kontra.gate_client import models as m
from kontra.profile import Profile

T0 = datetime(2026, 10, 11, 12, 0, tzinfo=UTC)


class FakeClock:
    def __init__(self, start: datetime = T0) -> None:
        self.now = start

    def __call__(self) -> datetime:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += timedelta(seconds=seconds)


class FakeSleep:
    """Advances the clock instead of sleeping. After ``budget`` sleeps it parks the caller
    (until cancelled) and sets ``parked``, so a test can step a background loop exactly."""

    def __init__(self, clock: FakeClock, budget: int = 1_000) -> None:
        self.clock = clock
        self.budget = budget
        self.delays: list[float] = []
        self.parked = asyncio.Event()

    async def __call__(self, delay: float) -> None:
        self.delays.append(delay)
        if len(self.delays) > self.budget:
            self.parked.set()
            await asyncio.Event().wait()
        self.clock.advance(delay)
        await asyncio.sleep(0)


def ok(model: Any) -> httpx.Response:
    return httpx.Response(200, json=model.model_dump(mode="json"))


def err(status: int, code: str, message: str = "no", detail: Any = None) -> httpx.Response:
    return httpx.Response(status, json={"code": code, "message": message, "detail": detail})


class FakeGate:
    def __init__(
        self,
        clock: FakeClock,
        *,
        party: str = "alice",
        refresh_token: str = "r0",
        access_ttl: int = 900,
        temporal_ttl: int = 900,
        dev: bool = True,
        skew: float = 0,
        jwt: bool = False,
    ) -> None:
        self.clock = clock
        self.party = party
        self.refresh_token = refresh_token
        self.access_ttl = access_ttl
        self.temporal_ttl = temporal_ttl
        self.dev = dev
        #: The gate's clock minus ours, in seconds (Temporal token timestamps use the gate's).
        self.skew = skew
        #: Issue Temporal tokens as JWT-shaped strings carrying iat/exp, like the real gate.
        self.jwt = jwt
        #: Like the real gate (RFC 9700 §4.14.2): a used refresh token presented again
        #: revokes the session, after which no refresh token works.
        self.used: set[str] = set()
        self.revoked = False
        #: False: hand the same refresh token back (a gate that does not rotate).
        self.rotate = True
        self.auth_time = T0 - timedelta(hours=1)
        self.public_key: str | None = None
        self.answer_party: str | None = None  # make the token route lie about the party
        self.access: dict[str, datetime] = {}
        self.issued = 0
        self.temporal_issued = 0
        self.temporal_failures = 0
        self.requests: list[httpx.Request] = []
        self.device_script: list[httpx.Response] = []
        self.device = m.DeviceStart(
            device_code="dev-code",
            user_code="ABCD-EFGH",
            verification_uri="https://gate.test/device",
            verification_uri_complete="https://gate.test/device?code=ABCD-EFGH",
            interval=5,
            expires_in=600,
        )
        self.transport = httpx.MockTransport(self._handle_async)

    def calls(self, path: str) -> int:
        return sum(1 for r in self.requests if r.url.path == path)

    def _tokens(self) -> m.TokenResponse:
        self.issued += 1
        access = f"acc{self.issued}"
        self.access[access] = self.clock() + timedelta(seconds=self.access_ttl)
        if self.rotate:
            self.refresh_token = f"r{self.issued}"
        return m.TokenResponse(
            access_token=access,
            expires_in=self.access_ttl,
            refresh_token=self.refresh_token,
            party=self.answer_party or self.party,
            auth_time=self.auth_time,
        )

    def _authed(self, r: httpx.Request) -> bool:
        token = r.headers.get("authorization", "").removeprefix("Bearer ")
        exp = self.access.get(token)
        return exp is not None and self.clock() < exp

    def me(self) -> m.Me:
        return m.Me(
            party=self.party,
            namespace=self.party,
            public_key=self.public_key,
            temporal_address="temporal.test:7400",
            created_at=T0,
            dev=self.dev,
        )

    async def _handle_async(self, r: httpx.Request) -> httpx.Response:
        await asyncio.sleep(0)  # a real network round trip lets other tasks run
        return self.handle(r)

    def handle(self, r: httpx.Request) -> httpx.Response:
        self.requests.append(r)
        path = r.url.path
        body: dict[str, Any] = json.loads(r.content) if r.content else {}
        if path == "/v1/auth/token":
            if body["grant_type"] == "refresh_token":
                presented = body["refresh_token"]
                if presented in self.used:
                    self.revoked = True
                if self.revoked or presented != self.refresh_token:
                    return err(400, "invalid_grant", "refresh token was already used")
                if self.rotate:
                    self.used.add(presented)
                return ok(self._tokens())
            if self.device_script:
                return self.device_script.pop(0)
            return ok(self._tokens())
        if path == "/v1/auth/device":
            return ok(self.device)
        if path == "/v1/auth/dev-login":
            self.party = body["handle"]
            return ok(self._tokens())
        if not self._authed(r):
            return err(401, "unauthenticated", "token rejected")
        if path == "/v1/me":
            return ok(self.me())
        if path == "/v1/me/key":
            self.public_key = body["public_key"]
            return ok(self.me())
        if path == "/v1/auth/temporal-token":
            if self.temporal_failures:
                self.temporal_failures -= 1
                return err(503, "unavailable")
            self.temporal_issued += 1
            now = self.clock() + timedelta(seconds=self.skew)
            expires = now + timedelta(seconds=self.temporal_ttl)
            token = f"jwt{self.temporal_issued}"
            if self.jwt:
                token = jwt_like(
                    {
                        "jti": token,
                        "iat": int(now.timestamp()),
                        "exp": int(expires.timestamp()),
                    }
                )
            return ok(
                m.TemporalToken(
                    token=token,
                    expires_at=expires,
                    namespace=self.party,
                    temporal_address="temporal.test:7400",
                )
            )
        return err(404, "not_found")


def jwt_like(claims: dict[str, Any]) -> str:
    """An unsigned JWT-shaped string: what matters to the client is the claims segment."""

    def seg(obj: dict[str, Any]) -> str:
        return base64.urlsafe_b64encode(json.dumps(obj).encode()).rstrip(b"=").decode()

    return f"{seg({'alg': 'ES256', 'typ': 'JWT'})}.{seg(claims)}.c2ln"


def make_profile(**over: Any) -> Profile:
    fields: dict[str, Any] = {
        "gate_url": "http://gate.test",
        "temporal_address": "127.0.0.1:1",
        "party": "alice",
        "namespace": "alice",
        "refresh_token": "r0",
        "auth_time": T0 - timedelta(hours=1),
        "public_key": None,
        "tls": False,
        "dev": True,
    }
    fields.update(over)
    return Profile(**fields)
