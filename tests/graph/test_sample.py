"""kontra._sample: values of a type drawn from its JSON Schema, by mode, for ``kontra graph``'s
mock runs (decisions I27). No Temporal here."""

from __future__ import annotations

import datetime as dt
import enum
import ipaddress
import json
import math
import re
import uuid
from decimal import Decimal
from typing import Annotated, Any, Literal, Self

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st
from jsonschema import Draft202012Validator
from pydantic import (
    UUID1,
    UUID4,
    UUID5,
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    HttpUrl,
    TypeAdapter,
    create_model,
    model_validator,
)

from kontra._parallel import Page
from kontra._sample import Unsampleable, draw, from_schema, mode, sample
from kontra.types import Artifact


def valid(tp: Any, value: Any) -> Any:
    """``value`` as the payload converter reads it (``validate_json``), or an error."""
    return TypeAdapter(tp).validate_json(json.dumps(value))


class Inner(BaseModel):
    items: list[int]
    deep: dict[str, list[int]] = {}


class Shape(BaseModel):
    flag: bool
    count: Annotated[int, Field(ge=2, le=50)]
    free: int
    ratio: Annotated[float, Field(gt=0, lt=1)]
    step: Annotated[int, Field(multiple_of=7, ge=1)]
    name: Annotated[str, Field(min_length=3, max_length=10)]
    color: Literal["red", "green", "blue"]
    maybe: int | None
    optional: str = "default"
    items: list[int]
    some: Annotated[list[int], Field(min_length=2, max_length=4)]
    pair: tuple[int, str, bool]
    nested: Inner
    rows: list[Inner] = []


def test_the_modes_by_run() -> None:
    assert [mode(run) for run in range(6)] == ["min", "max", "large", *["random"] * 3]


def test_min_mode() -> None:
    v = draw(Shape, 0)
    assert v["flag"] is False
    assert v["count"] == 2  # the minimum, 0 being out of bounds
    assert v["free"] == 0
    assert 0 < v["ratio"] < 1e-300  # just above the exclusive minimum
    assert v["step"] == 7  # the first multiple at or above the minimum
    assert v["name"] == "aaa"  # the shortest valid
    assert v["color"] == "red"  # the first enum value
    assert v["maybe"] is None  # null where null is allowed
    assert "optional" not in v
    assert "rows" not in v
    assert v["items"] == []
    assert len(v["some"]) == 2
    assert v["pair"] == [0, "", False]
    assert v["nested"] == {"items": []}
    assert valid(Shape, v).optional == "default"


def test_max_mode() -> None:
    v = draw(Shape, 1)
    assert v["flag"] is True
    assert v["count"] == 50  # the maximum
    assert v["free"] == 1000  # a large value without one
    assert 1 - 1e-15 < v["ratio"] < 1  # just below the exclusive maximum
    assert v["step"] == 1001  # 1000 rounded up to a multiple of 7
    assert v["name"] == "z" * 10  # the longest valid
    assert v["color"] == "blue"  # the last enum value
    assert v["maybe"] == 1000  # never null
    assert v["optional"]
    assert v["rows"]
    assert len(v["items"]) == 3
    assert len(v["some"]) == 3
    assert len(v["nested"]["items"]) == 3
    assert len(v["nested"]["deep"]) == 3
    valid(Shape, v)


def test_large_mode_lengthens_the_arrays_near_the_top() -> None:
    v = draw(Shape, 2)
    assert len(v["items"]) == 201  # a top-level array: a batching loop goes round
    assert len(v["nested"]["items"]) == 201  # two properties deep
    assert len(v["some"]) == 4  # maxItems still holds
    assert all(len(row["items"]) == 3 for row in v["rows"])  # inside another array: max
    assert all(len(d) == 3 for d in v["nested"]["deep"].values())  # three deep: max
    assert len(draw(list[int], 2)) == 201  # the value itself is the top
    valid(Shape, v)


def test_random_mode_is_seeded() -> None:
    values = [draw(Shape, run) for run in range(3, 40)]
    for v in values:
        valid(Shape, v)
        assert len(v["items"]) <= 5
        assert 2 <= len(v["some"]) <= 4
    assert {v["flag"] for v in values} == {True, False}
    assert {v["maybe"] is None for v in values} == {True, False}  # null with p=1/2
    assert {"optional" in v for v in values} == {True, False}
    assert len({v["color"] for v in values}) == 3


def test_booleans_take_both_values_within_two_runs() -> None:
    class Verdict(BaseModel):
        flag: bool
        hit: bool | None = None

    assert [draw(Verdict, run)["flag"] for run in (0, 1)] == [False, True]
    assert [bool(draw(Verdict, run).get("hit")) for run in (0, 1)] == [False, True]


@pytest.mark.parametrize("run", range(8))
def test_the_same_run_and_seed_draw_the_same_value(run: int) -> None:
    assert draw(Shape, run, seed=3) == draw(Shape, run, seed=3)


def test_another_seed_draws_other_random_values() -> None:
    assert [draw(Shape, run, seed=0) for run in range(3)] == [
        draw(Shape, run, seed=1) for run in range(3)
    ]  # min, max and large take no randomness
    assert draw(Shape, 3, seed=0) != draw(Shape, 3, seed=1)


@pytest.mark.parametrize("run", range(5))
def test_formats(run: int) -> None:
    class Formats(BaseModel):
        any_id: uuid.UUID
        v1: UUID1
        v4: UUID4
        v5: UUID5
        when: AwareDatetime
        day: dt.date
        at: dt.time
        took: dt.timedelta
        url: HttpUrl
        ip: ipaddress.IPv4Address
        ip6: ipaddress.IPv6Address
        net: ipaddress.IPv4Network
        amount: Decimal
        raw: bytes

    v = draw(Formats, run)
    parsed = valid(Formats, v)
    assert parsed.v1.version == 1
    assert parsed.v4.version == 4
    assert parsed.v5.version == 5
    assert parsed.when.tzinfo is not None
    assert str(parsed.url).startswith("https://")


def test_prefix_items_are_tuples() -> None:
    for run in range(6):
        v = draw(tuple[int, str, bool], run)
        assert len(v) == 3
        assert isinstance(v[0], int)
        assert isinstance(v[1], str)
        assert isinstance(v[2], bool)
    assert draw(tuple[int, ...], 1) == [1000, 1000, 1000]


@pytest.mark.parametrize(
    "pattern",
    [
        r"^[0-9a-f]{32}$",
        r"^https?://[^\s]+$",
        r"^[A-Za-z0-9.-]+(:[0-9]{1,5})?$",
        r"^[A-Z]{2}-\d{4}$",
        r"^(foo|bar)+\.(com|net)$",
        r"^\w+@\w+\.example$",
        r"^a.c$",
        r"[0-9]",  # unanchored: JSON Schema searches
    ],
)
def test_patterns_are_followed(pattern: str) -> None:
    tp = Annotated[str, Field(pattern=pattern)]
    for run in range(6):
        assert re.search(pattern, draw(tp, run))


def test_a_pattern_it_cannot_follow_is_unsampleable() -> None:
    class Odd(BaseModel):
        model_config = ConfigDict(regex_engine="python-re")

        code: str = Field(pattern=r"^(?=.*\d)[a-z0-9]{8}$")

    assert sample(Odd, 0) is None
    with pytest.raises(Unsampleable, match="lookaround"):
        draw(Odd, 0)

    class WithExamples(Odd):
        code: str = Field(pattern=r"^(?=.*\d)[a-z0-9]{8}$", examples=["abc12345", "zz99zz99"])

    assert [draw(WithExamples, run)["code"] for run in (0, 1)] == ["abc12345", "zz99zz99"]

    class WithDefault(Odd):
        code: str = Field(pattern=r"^(?=.*\d)[a-z0-9]{8}$", default="abc12345")

    assert draw(WithDefault, 1) == {"code": "abc12345"}


def test_recursion_is_bounded() -> None:
    class Node(BaseModel):
        value: int
        kids: list[Node] = []
        next: Node | None = None

    for run in range(6):
        valid(Node, draw(Node, run))

    def depth(v: Any) -> int:
        below = [*(v.get("kids") or []), *([v["next"]] if v.get("next") else [])]
        return 1 + max((depth(k) for k in below), default=0)

    assert depth(draw(Node, 1)) <= 4  # drawn as min mode draws from depth 4 on

    class Loop(BaseModel):
        child: Loop

    assert sample(Loop, 0) is None
    with pytest.raises(Unsampleable, match="nested deeper"):
        draw(Loop, 1)


class Target(BaseModel):
    """legacy-actor's: a model validator the schema cannot say."""

    url: str | None = None
    host: str | None = None

    @model_validator(mode="after")
    def _one(self) -> Self:
        if (self.url is None) == (self.host is None):
            raise ValueError("exactly one of url and host")
        return self


@pytest.mark.parametrize("run", range(6))
def test_what_validation_refuses_is_drawn_again(run: int) -> None:
    class Targets(BaseModel):
        targets: list[Target] = Field(min_length=1)

    v = draw(Targets, run)
    assert len(valid(Targets, v).targets) == {0: 1, 1: 3, 2: 201}.get(run, len(v["targets"]))


def test_a_union_branch_that_cannot_be_drawn_gives_way() -> None:
    lookahead = Annotated[str, Field(pattern=r"^(?=x)x$")]

    class Either(BaseModel):
        model_config = ConfigDict(regex_engine="python-re")

        value: int | lookahead

    # Max mode tries the last branch first: it gives way to the first.
    assert draw(Either, 1) == {"value": 1000}
    assert all(isinstance(draw(Either, run)["value"], int) for run in range(8))


def test_artifacts_and_pages() -> None:
    for run in range(5):
        valid(Artifact, draw(Artifact, run))
        page = valid(Page[str], draw(Page[str], run))
        assert page.next is None  # Any is null: a pages= loop ends


def test_types_without_a_schema_are_unsampleable() -> None:
    class Opaque:
        pass

    assert sample(Opaque, 0) is None
    with pytest.raises(Unsampleable, match="no JSON Schema"):
        draw(Opaque, 0)


def test_from_schema_reads_refs_and_all_of() -> None:
    schema: dict[str, Any] = {
        "$defs": {
            "Base": {"type": "object", "properties": {"a": {"type": "integer"}}, "required": ["a"]},
            "Name": {"type": "string", "minLength": 2, "format": "email"},
        },
        "allOf": [
            {"$ref": "#/$defs/Base"},
            {
                "properties": {"b": {"$ref": "#/$defs/Name"}, "c": {"type": ["null", "boolean"]}},
                "required": ["b", "c"],
            },
        ],
    }
    validator = Draft202012Validator(schema, format_checker=Draft202012Validator.FORMAT_CHECKER)
    for run in range(8):
        v = from_schema(schema, run)
        assert set(v) >= {"a", "b", "c"}
        validator.validate(v)


class Color(enum.Enum):
    RED = "red"
    BLUE = "blue"


_LEAVES: list[Any] = [
    bool,
    int,
    float,
    str,
    Annotated[int, Field(ge=-5, le=5)],
    Annotated[float, Field(gt=0, le=1e6)],
    Annotated[int, Field(multiple_of=3, gt=10)],
    Annotated[str, Field(min_length=2, max_length=6)],
    Annotated[str, Field(pattern=r"^[a-z]{3}-[0-9]{2}$")],
    Literal["a", "b", 3],
    Color,
    uuid.UUID,
    dt.datetime,
    dt.date,
    Decimal,
    dt.timedelta,
    ipaddress.IPv4Address,
    HttpUrl,
    bytes,
]


def _types() -> st.SearchStrategy[Any]:
    def wrap(inner: st.SearchStrategy[Any]) -> st.SearchStrategy[Any]:
        return st.one_of(
            inner.map(lambda t: list[t]),
            inner.map(lambda t: Annotated[list[t], Field(min_length=1, max_length=3)]),
            st.tuples(inner, inner).map(lambda ts: tuple[ts[0], ts[1]]),
            inner.map(lambda t: dict[str, t]),
            inner.map(lambda t: t | None),
            st.lists(inner, min_size=1, max_size=4).map(_model),
        )

    return st.recursive(st.sampled_from(_LEAVES), wrap, max_leaves=8)


_models = iter(range(10**9))


def _model(fields: list[Any]) -> type[BaseModel]:
    return create_model(  # pyright: ignore[reportCallIssue, reportUnknownVariableType]
        f"M{next(_models)}",
        **{f"f{i}": (t, ...) if i % 2 == 0 else (t | None, None) for i, t in enumerate(fields)},
    )


@settings(max_examples=60, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(tp=_types(), seed=st.integers(0, 2**16))
def test_every_sample_is_valid(tp: Any, seed: int) -> None:
    for run in range(6):
        valid(tp, draw(tp, run, seed))


def test_numbers_stay_in_bounds() -> None:
    narrow = Annotated[float, Field(ge=0.5, le=0.5)]
    assert [draw(narrow, run) for run in range(4)] == [0.5] * 4
    negative = Annotated[int, Field(lt=-3)]
    assert draw(negative, 0) == -4
    assert draw(negative, 1) == -4
    assert all(draw(negative, run) < -3 for run in range(3, 10))
    assert not math.isnan(draw(float, 3))
