"""Seam ⑥, the Protocol itself (decisions I32): its member set is pinned, none of its members
authors workflows, no Temporal concept is in its types, it imports no temporalio, content
leaves it only sealed, and escrow's stops map onto it totally."""

from __future__ import annotations

import inspect
import os
import re
import subprocess
import sys
import typing
from collections.abc import Callable
from dataclasses import is_dataclass
from enum import Enum
from hashlib import sha256

import pytest
from temporalio.api.common.v1 import Payload

from kontra import _control, engine
from kontra.codec import age
from kontra.contract.names import OfferingId, Version
from kontra.engine import (
    AGE_MAGIC,
    MEMBERS,
    STOPS,
    BuyerStart,
    Engine,
    JobPlan,
    NotSealed,
    Observed,
    Reach,
    Sealed,
    SealedPayload,
    Stop,
)
from kontra.engine.temporal import TemporalEngine
from kontra.escrow import EffectKind, Observation, ObservedStatus, Provenance
from kontra.testing import FakeEngine
from kontra.testing.engine_contract import protocol_members

#: Words that name an engine's own concepts (Temporal's), or a workflow-authoring act. None
#: may be a member's name, a parameter's, or a field of a type the Protocol hands over.
TEMPORAL_WORDS = frozenset(
    {
        "namespace",
        "queue",
        "workflow",
        "activity",
        "activities",
        "nexus",
        "endpoint",
        "history",
        "signal",
        "query",
        "update",
        "run",
        "child",
    }
)
AUTHORING_WORDS = frozenset(
    {
        "execute",
        "activity",
        "workflow",
        "schedule",
        "timer",
        "sleep",
        "register",
        "define",
        "defn",
        "decorate",
        "spawn",
        "continue",
        "signal",
        "query",
        "update",
        "child",
    }
)


def _words(name: str) -> set[str]:
    """The words of an identifier or a value, however it is spelled: ``snake_case``,
    ``CamelCase``, ``SCREAMING_CASE``, or any mix (``NexusReach``, ``nexus_never``)."""
    return {w.lower() for w in re.findall(r"[A-Z]?[a-z]+|[A-Z]+(?![a-z])|[0-9]+", name)}


def test_the_walk_reads_words_in_every_spelling() -> None:
    """A Temporal word hides in no spelling: a class name (``NexusReach``), an enum value
    (``"nexus_never_dispatched"``), a member or field name, an acronym."""
    assert _words("NexusReach") == {"nexus", "reach"}
    assert _words("nexus_never_dispatched") == {"nexus", "never", "dispatched"}
    assert _words("NEVER_DISPATCHED") == {"never", "dispatched"}
    assert _words("task_queue") == {"task", "queue"}
    assert _words("HTTPWorkflowRun") == {"http", "workflow", "run"}
    assert _words("result_sha256") == {"result", "sha", "256"}
    assert _words("ObservedStatus") & TEMPORAL_WORDS == set()


def test_the_member_set_is_pinned() -> None:
    """Adding a member is a decision (I32), not a refactor: this test fails on any change."""
    assert protocol_members(Engine) == MEMBERS
    assert {
        "name",
        "isolate",
        "expose",
        "quote",
        "bind",
        "start",
        "observe",
        "liveness",
        "cancel",
        "fence",
        "stream",
    } == MEMBERS
    if sys.version_info >= (3, 12):
        assert Engine.__protocol_attrs__ == MEMBERS  # type: ignore[attr-defined]  # pyright: ignore[reportAttributeAccessIssue]


def test_no_member_authors_workflows() -> None:
    """The guardrail: "the day someone wants ``engine.execute_activity(...)``, that is the
    facade crawling back". No member is named for a workflow-authoring act, and the only
    callable any member takes is the gate's own predicate ``closed`` -- no member takes
    seller code to run."""
    for member in MEMBERS:
        assert not _words(member) & AUTHORING_WORDS, member
    callables: list[tuple[str, str]] = []
    for member in MEMBERS - {"name"}:
        hints = typing.get_type_hints(getattr(Engine, member))
        for param, hint in hints.items():
            if param != "return" and "Callable" in str(hint):
                callables.append((member, param))
    assert callables == [("stream", "closed")]


#: The one exemption, by name: the catalog's :class:`~kontra.contract.names.OfferingId`, which
#: ``expose``, ``quote`` and ``JobPlan`` take, is an offering's id, not a type of this seam. Its
#: ``namespace`` and ``task_queue`` (and ``service_name``, which no word here catches) are
#: ``kontra.contract.names`` derivations -- constructed, never parsed -- that only the Temporal
#: adapter reads; another adapter ignores them (decisions I32).
EXEMPT = frozenset({("OfferingId", "namespace"), ("OfferingId", "task_queue")})


def _dataclasses_in(hint: object) -> set[type]:
    found: set[type] = set()
    for arg in (hint, *typing.get_args(hint)):
        if is_dataclass(arg) and isinstance(arg, type):
            found.add(arg)
        elif arg is not hint:
            found |= _dataclasses_in(arg)
    return found


def _enums_in(hint: object) -> set[type[Enum]]:
    found: set[type[Enum]] = set()
    for arg in (hint, *typing.get_args(hint)):
        if isinstance(arg, type) and issubclass(arg, Enum):
            found.add(arg)
        elif arg is not hint:
            found |= _enums_in(arg)
    return found


def test_no_temporal_concept_is_in_the_protocols_types() -> None:
    """Designed against Golem: no namespace, task queue, run id, Nexus endpoint or history in
    a member, a parameter, or a public attribute (field, property or method) of anything the
    Protocol hands over or takes, or of the types inside those -- but :data:`EXEMPT` -- nor
    in a value of an enum among them (:class:`~kontra.engine.Reach`'s included)."""
    types: set[type] = {Observation, engine.StreamItem, engine.Liveness, engine.Isolation}
    for member in sorted(MEMBERS - {"name"}):
        fn = getattr(Engine, member)
        for param in inspect.signature(fn).parameters:
            assert not _words(param) & TEMPORAL_WORDS, (member, param)
        for hint in typing.get_type_hints(fn).values():
            types |= _dataclasses_in(hint)
    enums: set[type[Enum]] = set()
    for member in sorted(MEMBERS - {"name"}):
        for hint in typing.get_type_hints(getattr(Engine, member)).values():
            enums |= _enums_in(hint)
    todo, seen = list(types), set[type]()
    while todo:
        cls = todo.pop()
        if cls in seen:
            continue
        seen.add(cls)
        for hint in typing.get_type_hints(cls).values():
            todo.extend(_dataclasses_in(hint))
            enums |= _enums_in(hint)
    assert {OfferingId, Version, JobPlan, Observation, SealedPayload, BuyerStart} <= seen
    assert {Reach, ObservedStatus, Provenance, Stop} <= enums
    for enum in enums:
        assert not _words(enum.__name__) & TEMPORAL_WORDS, enum.__name__
        for value in enum:
            assert not _words(value.name) & TEMPORAL_WORDS, (enum.__name__, value.name)
            assert not _words(str(value.value)) & TEMPORAL_WORDS, (enum.__name__, value.value)
    hits = {
        (cls.__name__, name)
        for cls in seen
        for name in dir(cls)
        if not name.startswith("_") and _words(name) & TEMPORAL_WORDS
    }
    assert hits == EXEMPT
    for cls in seen:
        assert not _words(cls.__name__) & TEMPORAL_WORDS, cls.__name__


class _Signer:
    issuer = "https://gate.test"

    def sign(self, claims: object) -> str:
        return "token"


def test_both_adapters_conform() -> None:
    assert isinstance(FakeEngine(), Engine)
    temporal = TemporalEngine(object(), signer=_Signer(), address="127.0.0.1:7233")  # type: ignore[arg-type]  # pyright: ignore[reportArgumentType]
    assert isinstance(temporal, Engine)
    assert temporal.name == "temporal"


def test_kontra_engine_imports_no_temporalio() -> None:
    """The Protocol is engine-neutral down to its imports (checked in a fresh interpreter)."""
    code = (
        "import sys, kontra.engine; "
        "print(sorted(m for m in sys.modules if m.split('.')[0] == 'temporalio'))"
    )
    out = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        check=True,
        env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
    ).stdout.strip()
    assert out == "[]"


def test_hold_digest_moved_and_control_re_exports_it() -> None:
    """kontra-cloud imports it from kontra._control: the same function, one definition."""
    assert _control.hold_digest is engine.hold_digest
    signed = "aGVhZGVy.Y2xhaW1z"
    assert engine.hold_digest(f" \t{signed}.sig ") == sha256(signed.encode()).hexdigest()


def test_stops_cover_every_stop_effect_and_keep_their_values() -> None:
    """Escrow's effects are persisted (kontra-cloud ``jobs.pending_stop``, owner decision OD9):
    their values stay, and each maps onto a Stop."""
    stops = {k for k in EffectKind if k.value.endswith("_workflow") or k.value.endswith("_run")}
    assert set(STOPS) == stops
    assert {k.value for k in stops} == {"cancel_workflow", "terminate_workflow", "terminate_run"}
    assert STOPS[EffectKind.CANCEL_WORKFLOW] == (Stop.ASK, False)
    assert STOPS[EffectKind.TERMINATE_WORKFLOW] == (Stop.END, False)
    assert STOPS[EffectKind.TERMINATE_RUN] == (Stop.END, True)


def _envelope(encoding: bytes, data: bytes, **meta: bytes) -> bytes:
    metadata = {"encoding": encoding, "kontra-v": b"1", **meta}
    return Payload(metadata=metadata, data=data).SerializeToString(deterministic=True)


#: A real age file (to a key nobody keeps), and the shapes that only look like one.
AGE = age.encrypt(b"ciphertext", [age.generate_identity()[1]])
LOOKALIKES = (
    AGE_MAGIC + b'{"secret":"plaintext after the version line"}',
    AGE_MAGIC + b"-> X25519 stanza\n--- mac\nciphertext",
    AGE[: AGE.index(b"\n---") + 1],  # a header cut before its MAC line
    AGE[: -(len(b"ciphertext") + 16)],  # the header and the nonce, no sealed chunk
)
REF = b'{"sha256":"' + b"a" * 64 + b'","size":10}'


@pytest.mark.parametrize(
    "data",
    [
        _envelope(b"binary/kontra-age", AGE),
        _envelope(b"binary/kontra-claim-check", REF),
    ],
    ids=["kontra-age", "claim-check"],
)
def test_a_sealed_payload_is_the_codecs_envelope(data: bytes) -> None:
    assert SealedPayload(data).data == data


@pytest.mark.parametrize(
    "data",
    [
        b"",
        b"not a payload \xff\xff",
        Payload(metadata={"encoding": b"json/plain"}, data=b'"plain"').SerializeToString(),
        _envelope(b"json/plain", b'"plain"'),
        _envelope(b"binary/kontra-age", b"no age file"),
        _envelope(b"binary/kontra-claim-check", b'{"sha256":"nope","size":1}'),
        _envelope(b"binary/kontra-age", AGE, **{"kontra-plaintext": b"1"}),
        Payload(
            metadata={"encoding": b"binary/kontra-age", "kontra-v": b"2"}, data=AGE
        ).SerializeToString(deterministic=True),
        Payload(metadata={"encoding": b"binary/kontra-age"}, data=AGE).SerializeToString(),
        _envelope(b"binary/kontra-age", AGE) + b"\x00",
        *(_envelope(b"binary/kontra-age", like) for like in LOOKALIKES),
    ],
    ids=[
        "empty",
        "not-a-payload",
        "json-plain",
        "json-plain-versioned",
        "age-without-age",
        "bad-reference",
        "extra-metadata",
        "version-2",
        "unversioned",
        "trailing-byte",
        "plaintext-after-the-version-line",
        "a-header-that-does-not-parse",
        "no-mac-line",
        "no-sealed-chunk",
    ],
)
def test_anything_else_is_not_sealed(data: bytes) -> None:
    with pytest.raises(NotSealed):
        SealedPayload(data)


def test_a_stream_item_is_an_age_file() -> None:
    """Structurally, not by its first line: plaintext behind ``age-encryption.org/v1`` is not
    an age file (no key is needed to tell)."""
    assert Sealed(AGE).data == AGE
    for bad in (b"", b"plaintext", AGE[1:], *LOOKALIKES):
        with pytest.raises(NotSealed):
            Sealed(bad)


def test_an_observation_carries_a_result_only_as_a_delivery() -> None:
    """The type guarantee behind "ciphertext only": a result travels only with a verified
    completion, a verified completion only with its result (escrow pays it as a delivery),
    and the digest and size are of exactly its bytes -- never of bytes nobody handed over."""
    data = _envelope(b"binary/kontra-age", AGE)
    sealed = SealedPayload(data)
    ok = Observation(
        ObservedStatus.COMPLETED,
        provenance=Provenance.VERIFIED,
        pin="run-1",
        result_sha256=sha256(data).hexdigest(),
        result_size=len(data),
    )
    assert Observed(ok, sealed).result == sealed
    bad: list[Callable[[], object]] = [
        lambda: Observed(replace_(ok, status=ObservedStatus.RUNNING), sealed),
        lambda: Observed(replace_(ok, provenance=Provenance.NOT_FROM_BUYER), sealed),
        lambda: Observed(replace_(ok, result_sha256="0" * 64), sealed),
        lambda: Observed(replace_(ok, result_size=1), sealed),
        lambda: Observed(replace_(ok, pin="a pin with spaces"), sealed),
        lambda: Observed(replace_(ok, pin="p" * 129), sealed),
        # A delivery without its bytes: with the digest of something else, or with none.
        lambda: Observed(ok),
        lambda: Observed(replace_(ok, result_sha256=None, result_size=None)),
        # A digest on what is no delivery.
        lambda: Observed(replace_(ok, status=ObservedStatus.UNSEALED)),
        lambda: Observed(replace_(ok, status=ObservedStatus.RUNNING, result_sha256=None)),
    ]
    for make in bad:
        with pytest.raises(ValueError, match=r"result|digest|pin"):
            make()


def test_reach_is_a_field_checked_against_the_observation() -> None:
    """Decisions I40: ``reach`` is a field of what ``observe`` hands over, not a member (the
    pin above is unchanged), and a verified lineage -- begun by the buyer's start -- cannot be
    handed over as anything but STARTED (or None: the engine cannot say)."""
    assert "observe" in MEMBERS
    assert not {"reach", "describe", "blame"} & MEMBERS
    data = _envelope(b"binary/kontra-age", AGE)
    delivered = Observation(
        ObservedStatus.COMPLETED,
        provenance=Provenance.VERIFIED,
        pin="run-1",
        result_sha256=sha256(data).hexdigest(),
        result_size=len(data),
    )
    nothing = Observation(ObservedStatus.NOT_FOUND)
    start = BuyerStart(made_at=None, gives_up_at=None, withdrawal_asked=True)
    assert Observed(nothing).reach is None  # the default blames no one
    assert Observed(delivered, SealedPayload(data), reach=Reach.STARTED).reach is Reach.STARTED
    assert Observed(delivered, SealedPayload(data)).reach is None
    for reach in Reach:
        assert Observed(nothing, reach=reach, buyer_start=start).buyer_start == start
    for bad in set(Reach) - {Reach.STARTED}:
        with pytest.raises(ValueError, match="verified"):
            Observed(delivered, SealedPayload(data), reach=bad)
    with pytest.raises(ValueError, match="with its reach"):
        Observed(nothing, buyer_start=start)
    with pytest.raises(ValueError, match="a Reach"):
        Observed(nothing, reach="started")  # type: ignore[arg-type]  # pyright: ignore[reportArgumentType]


def replace_(obs: Observation, **changes: object) -> Observation:
    from dataclasses import replace

    return replace(obs, **changes)  # type: ignore[arg-type]


def test_a_plan_is_checked_when_it_is_made() -> None:
    from kontra.contract.names import OfferingId, new_endpoint_name, new_job_id

    acme = OfferingId.parse("acme/enrich@1.0.0")
    JobPlan(new_job_id(), acme, new_endpoint_name(), "bob", "acme")
    for args in (
        ("not-a-job-id", acme, new_endpoint_name(), "bob", "acme"),
        (new_job_id(), acme, new_endpoint_name(), "Bob!", "acme"),
        (new_job_id(), acme, new_endpoint_name(), "bob", "other"),  # not the seller's offering
    ):
        with pytest.raises(ValueError, match=r"job id|party handles|not the seller's"):
            JobPlan(*args)
