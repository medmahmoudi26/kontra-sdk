"""A whole marketplace in one process: a Temporal dev server, the fake gate, and parties.

``async with Marketplace() as mp:`` starts a dev server with standalone Nexus
(:mod:`kontra.testing.devserver`), wires a :class:`~kontra.testing.fake_gate.FakeGate` to
it through :class:`kontra._control.Control` and serves the gate on a free port.
``await mp.party("acme")`` then gives a :class:`TestParty`: logged in through the gate's
dev login exactly as ``kontra login --dev`` would (its own ``KONTRA_HOME`` in a temporary
directory, a profile, an age key registered with the gate, a :class:`kontra.auth.Session`)
and funded with play money. A seller's or buyer's whole job lifecycle -- quote, hire,
delivery, review window, timeout and refund -- runs in a unit test with no containers
(PRD §19.3). ``async with mp.serve(worker, party):`` runs a :class:`kontra.serve.Worker` as
that party for the block, as ``kontra serve`` would with the party's profile, but for one
setting: its stream upload budget is :data:`SERVE_UPLOAD_BUDGET`.

Defaults are fast: the sweeper runs every 0.2 s and the refund grace is 1 s, so deadlines
and review windows can be driven with ``mp.gate.advance(...)`` within a test's patience.
"""

from __future__ import annotations

import asyncio
import contextlib
import shutil
import tempfile
from collections.abc import AsyncGenerator
from dataclasses import dataclass, field
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING, Any

from temporalio.client import Client

from kontra._control import Control
from kontra.auth import Session, login
from kontra.contract.names import validate_handle
from kontra.gate_client import GateClient
from kontra.gate_client import models as m
from kontra.ledger import Money
from kontra.profile import Profile
from kontra.testing.devserver import DevServer, start_dev_server
from kontra.testing.fake_gate import FakeGate

if TYPE_CHECKING:
    from kontra.hire import Kontra
    from kontra.serve import Worker

__all__ = ["SERVE_UPLOAD_BUDGET", "Marketplace", "TestParty"]

#: The stream upload budget (seconds) of a worker :meth:`Marketplace.serve` runs, where
#: ``kontra serve`` gives 2 (:data:`kontra.codec.stream.STREAM_UPLOAD_BUDGET`, the codec's
#: ``stream_upload_budget``). A test machine under load can take longer than 2 s to store one
#: poll answer's 64 items in the fake gate's bucket, and an item not stored in time is a stub,
#: a gap for the buyer: a test that counts every item would lose some. 8 s still leaves the
#: worker time to complete the workflow task within Temporal's 10 s timeout. A test of the
#: stubs a slow bucket leaves passes its own (``mp.serve(..., stream_upload_budget=0.5)``).
SERVE_UPLOAD_BUDGET = 8.0


@dataclass
class TestParty:
    """A party logged in to the marketplace's gate, with its own ``KONTRA_HOME``."""

    __test__ = False  # not a pytest test class, whatever its name says

    handle: str
    home: Path
    profile: Profile
    session: Session
    marketplace: Marketplace = field(repr=False)
    _buyers: list[Kontra] = field(default_factory=list["Kontra"], repr=False)

    @property
    def gate(self) -> GateClient:
        """The party's authenticated gate client (tokens refresh themselves)."""
        return self.session.gate

    @property
    def identity(self) -> str:
        """The party's secret age identity (``AGE-SECRET-KEY-1...``)."""
        return Profile.identity(self.home)

    @property
    def public_key(self) -> str:
        assert self.profile.public_key is not None
        return self.profile.public_key

    @property
    def namespace(self) -> str:
        return self.profile.namespace

    def env(self) -> dict[str, str]:
        """Environment for a subprocess acting as this party (``kontra`` CLI, a worker)."""
        return {"KONTRA_HOME": str(self.home)}

    async def temporal_client(self, **kwargs: Any) -> Client:
        """A Temporal client for the party's namespace, authenticated with its worker JWT
        as ``kontra serve`` would be (the dev server accepts it without checking)."""
        return await self.session.connect_temporal(**kwargs)

    async def balance(self, currency: str = "USD") -> m.Balance:
        return await self.gate.balance(currency)

    def kontra(self, **options: Any) -> Kontra:
        """A :class:`kontra.hire.Kontra` buying as this party (on its session; closed with
        the party). ``options`` are ``Kontra``'s keywords (``start_timeout``, ``client``)."""
        from kontra.hire import Kontra

        buyer = Kontra(session=self.session, **options)
        self._buyers.append(buyer)
        return buyer

    async def aclose(self) -> None:
        for buyer in self._buyers:
            await buyer.aclose()
        self._buyers.clear()
        await self.session.aclose()


class Marketplace:
    """Dev server + fake gate + parties, torn down together.

    ``dev_server``: reuse a running :class:`~kontra.testing.devserver.DevServer` (e.g. one
    shared by a test session) instead of starting one; it is not stopped on exit.
    ``temporal=False``: no Temporal at all (a gate whose sellers are offline). Other
    keyword arguments go to :class:`~kontra.testing.fake_gate.FakeGate`.
    """

    def __init__(
        self,
        *,
        dev_server: DevServer | None = None,
        temporal: bool = True,
        sweep_interval: float = 0.2,
        grace: timedelta = timedelta(seconds=1),
        resolvers: tuple[str, ...] = (),
        **gate_options: Any,
    ) -> None:
        self._given_dev = dev_server
        self._temporal = temporal
        self._gate_options: dict[str, Any] = {
            "sweep_interval": sweep_interval,
            "grace": grace,
            "resolvers": resolvers,
            "signup_grant": 0,  # parties are funded explicitly by party(balance=...)
            **gate_options,
        }
        self._dev: DevServer | None = None
        self._control: Control | None = None
        self._gate: FakeGate | None = None
        self._root: Path | None = None
        self.parties: dict[str, TestParty] = {}

    # ------------------------------------------------------------------ lifecycle

    async def __aenter__(self) -> Marketplace:
        try:
            await self._start()
        except BaseException:
            await self.aclose()
            raise
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()

    async def _start(self) -> None:
        self._root = Path(tempfile.mkdtemp(prefix="kontra-marketplace-"))
        if self._temporal:
            self._dev = self._given_dev or await start_dev_server()
            self._control = Control(self._dev.client, self._dev.http_base)
            self._gate_options.setdefault("temporal_address", self._dev.grpc_address)
        self._gate = FakeGate(self._control, **self._gate_options)
        await self._gate.serve()

    async def aclose(self) -> None:
        for party in list(self.parties.values()):
            await party.aclose()
        self.parties.clear()
        if self._gate is not None:
            if self._control is not None:
                # The relay's HTTP client was made on the gate's loop: close it there.
                await self._gate.run(self._control.aclose)
            await self._gate.stop()
        self._control = None
        if self._dev is not None and self._dev is not self._given_dev:
            await self._dev.stop()
        self._dev = None
        if self._root is not None:
            shutil.rmtree(self._root, ignore_errors=True)
            self._root = None

    # ------------------------------------------------------------------ parts

    @property
    def gate(self) -> FakeGate:
        if self._gate is None:
            raise RuntimeError("the marketplace is not running; use `async with Marketplace()`")
        return self._gate

    @property
    def base_url(self) -> str:
        return self.gate.base_url

    @property
    def dev_server(self) -> DevServer:
        if self._dev is None:
            raise RuntimeError("this marketplace runs without Temporal")
        return self._dev

    @property
    def control(self) -> Control:
        if self._control is None:
            raise RuntimeError("this marketplace runs without Temporal")
        return self._control

    def kontra(self, party: TestParty | str, **options: Any) -> Kontra:
        """``party.kontra(**options)``: a buyer client for one of this marketplace's parties."""
        who = self.parties[party] if isinstance(party, str) else party
        return who.kontra(**options)

    def client(self, namespace: str) -> Client:
        """A raw Temporal client for ``namespace`` (no token), sharing the dev server's
        connection: for test scaffolding, not for code under test."""
        return self.dev_server.client_for(namespace)

    async def party(
        self, handle: str, balance: Decimal | int | str = "100.00", *, resolver: bool = False
    ) -> TestParty:
        """Create and log in party ``handle`` (its namespace is registered on the way), fund
        it with ``balance`` USD of play money, and make it a dispute resolver if asked."""
        validate_handle(handle)
        if handle in self.parties:
            raise ValueError(f"party {handle!r} already exists in this marketplace")
        if self._root is None:
            raise RuntimeError("the marketplace is not running; use `async with Marketplace()`")
        home = self._root / handle
        home.mkdir(mode=0o700)
        profile = await login(self.base_url, handle=handle, dev=True, home=home)
        session = Session(profile, home=home)
        party = TestParty(handle, home, profile, session, self)
        self.parties[handle] = party
        amount = Money.from_decimal(balance, "USD")
        if amount.amount_minor > 0:
            await session.gate.topup(amount.amount_minor)
        if resolver:
            self.gate.resolvers = self.gate.resolvers | {handle}
        return party

    @contextlib.asynccontextmanager
    async def serve(
        self,
        worker: Worker,
        party: TestParty | str,
        *,
        session: Session | None = None,
        client: Client | None = None,
        stream_upload_budget: float = SERVE_UPLOAD_BUDGET,
    ) -> AsyncGenerator[Worker]:
        """Run ``worker`` as ``party`` for the block: what ``kontra serve`` does with that
        party's profile (on a dev profile its unpublished offerings are published), until it
        polls. The block exits with a graceful shutdown; a worker that cannot start raises
        here (a hash-lock refusal, say) instead of yielding.

        ``session`` replaces the party's own (e.g. one on a request-counting transport);
        ``client`` shares an existing Temporal connection instead of dialling a new one.
        ``stream_upload_budget``: the worker's codec's (:data:`SERVE_UPLOAD_BUDGET`).
        """
        who = self.parties[party] if isinstance(party, str) else party
        run = asyncio.create_task(
            worker.run_async(
                session=session or who.session,
                client=client,
                handle_signals=False,
                stream_upload_budget=stream_upload_budget,
            ),
            name=f"kontra-serve {who.handle}",
        )
        ready = asyncio.create_task(worker.wait_ready())
        await asyncio.wait([run, ready], return_when=asyncio.FIRST_COMPLETED)
        if run.done() or not worker.serving:
            ready.cancel()
            await run  # the start failure, if any
            raise RuntimeError("the worker stopped before it was ready")
        try:
            yield worker
        finally:
            worker.shutdown()
            await run
