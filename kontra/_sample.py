"""Values of a type, drawn from its JSON Schema: what ``kontra graph``'s mock runs start a
workflow with and answer its activities with (decisions I27).

A mock run is only as varied as the values it is fed: whether a run takes the ``if`` on an
activity's result, or how many times it goes round a loop over its input, is the value's
doing. So each run index draws in a mode of its own, deterministically, and the runs of one
command cover the corners before they wander:

* run 0, **min**: false, 0 (or the minimum), the shortest string, ``[]`` (or ``minItems``),
  null where null is allowed, optional properties left out, the first enum value;
* run 1, **max**: true, the maximum (else 1000), the longest string, 3 items, every
  optional property present and not null, the last enum value;
* run 2, **large**: as max, but an array near the top (no deeper than two properties, and in
  no other array) gets 201 items, so a loop that batches its input (by 50, by 100) goes
  round more than once and folds;
* run 3 on, **random**: ``random.Random(seed * 1_000_003 + run)``; null with p=1/2 where it
  is allowed, each optional property with p=1/2, arrays of 0 to 5 items.

A boolean therefore takes both values within any two runs (0 and 1): ``if result.flag``
shows both branches from two runs on, however deep it sits. Only a recursion is cut short:
where a ``$ref`` repeats on the way down (a tree's children), the rest is drawn as min mode
draws it, so a recursive model ends.

The value is drawn from ``TypeAdapter(tp).json_schema(mode="validation")`` -- ``$ref`` and
``$defs``, ``anyOf``/``oneOf``/``allOf``, ``const``/``enum``, type lists, objects (a
mapping's keys from ``propertyNames`` or ``patternProperties``), arrays
(``prefixItems`` tuples), strings (lengths, formats, a ``pattern`` followed by a small
generator over Python's own regex parser), numbers (bounds, ``multipleOf``) and
``default`` -- and then validated by the same adapter, as the payload converter will read
it (``validate_json``). The schema cannot say everything a model checks (a model
validator's "exactly one of"), so where validation fails, what each error points at is
drawn again from the run's random stream (a mapping whose key was refused, whole), in the
order of the errors, for a bounded number of rounds; a datetime's offset that the type
refuses or wants is taken off or put on in place. A type no valid
value was found for raises :class:`Unsampleable` (:func:`draw`), or gives None
(:func:`sample`): a pattern with a lookaround or a backreference and no ``examples`` or
``default``, a recursion that never ends, a type pydantic has no schema for.

In-house, not ``hypothesis-jsonschema``: that has had no release since 2024-02, does not
read pydantic's 2020-12 output (``prefixItems``, ``format: uuid``, recursive models), and
draws at random outside ``@given``, where a branch test would be flaky (decisions I27).
Pure: no I/O, and no state between calls but a cache of the patterns read.
"""

from __future__ import annotations

import base64
import contextlib
import functools
import importlib
import json
import math
import random
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, TypeAlias, TypeVar, cast

from pydantic import TypeAdapter, ValidationError

__all__ = ["Unsampleable", "draw", "from_schema", "mode", "sample"]

MIN = "min"
MAX = "max"
LARGE = "large"
RANDOM = "random"
_MODES = (MIN, MAX, LARGE)

#: Items an array gets in max mode, and in large mode near the top.
_MAX_ITEMS = 3
_LARGE_ITEMS = 201
#: How deep "near the top" is, in properties.
_LARGE_DEPTH = 2
#: Random mode's most items (unless ``minItems`` asks for more).
_RANDOM_ITEMS = 5
#: Max mode's number without a maximum, and the span random mode draws from without bounds.
_BIG = 1000
#: Max mode's string without a ``maxLength``; random mode's longest string past the minimum.
_LONG = 64
_RANDOM_LONG = 16
#: The longest string drawn: a ``maxLength`` past it is not drawn to the end.
_LONGEST = 10_000
#: From this deep on (path elements), values are drawn as min mode draws them, $ref or not:
#: a schema nested this deep without recursing still ends in bounded size.
_SHALLOW = 16
#: Deeper than this, nothing is drawn: a recursion min mode cannot end.
_DEEPEST = 32
#: ``$ref`` and ``allOf`` hops followed for one value.
_HOPS = 64
#: Rounds of drawing again what validation refused.
_ROUNDS = 32
#: Repeats a pattern's ``+``, ``*`` or ``{n,}`` gives past its minimum, in max mode.
_REPEATS = 3

#: Where a value sits in the drawn whole: property names and array indexes.
_Path = tuple[str | int, ...]
_T = TypeVar("_T")
_Schema = Mapping[str, Any]
#: How a ``format`` is drawn: ``(mode, rng) -> str``.
_Format = Callable[[str, random.Random], str]
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
#: What a ``uri`` and its kin, a ``hostname`` and an ``email`` are drawn from.
_HOSTS = ("example.com", "zz.example.net")
_ALPHABET = "abcdefghijklmnopqrstuvwxyz0123456789"
#: The characters a pattern's classes are drawn from: printable ASCII but space.
_PRINTABLE = [chr(c) for c in range(0x21, 0x7F)]
#: What a class that matches none of them is drawn from (``\s``).
_SPACES = [" ", "\t"]
#: Formats that are a plain string within its lengths (``bytes``, ``SecretStr``).
_PLAIN_FORMATS = frozenset({"binary", "password"})
#: Validation errors repaired in place rather than drawn again: a datetime with an offset
#: where a naive one is wanted, and without one where an aware one is.
_NAIVE, _AWARE = "timezone_naive", "timezone_aware"


class Unsampleable(ValueError):
    """No valid value of the type was found: the message says what stood in the way."""


def mode(run: int) -> str:
    """The mode run ``run`` draws in: ``min``, ``max``, ``large``, then ``random``."""
    return _MODES[run] if 0 <= run < len(_MODES) else RANDOM


def _rng(run: int, seed: int) -> random.Random:
    return random.Random(seed * 1_000_003 + run)  # noqa: S311 -- reproducible, not secret


def _name(tp: object) -> str:
    return getattr(tp, "__name__", None) or repr(tp)


def draw(tp: Any, run: int, seed: int = 0) -> Any:
    """A JSON value that ``TypeAdapter(tp).validate_json`` accepts, drawn in run ``run``'s
    mode (see the module docstring); the same ``tp``, ``run`` and ``seed`` give the same
    value. :class:`Unsampleable` when none was found."""
    try:
        adapter: TypeAdapter[Any] = TypeAdapter(tp)
        schema = adapter.json_schema(mode="validation")
    except Exception as e:  # pydantic has no schema for it (an arbitrary class)
        raise Unsampleable(f"{_name(tp)} has no JSON Schema: {e}") from None
    drawer = _Drawer(schema, _rng(run, seed))
    try:
        value = drawer.draw(schema, mode(run), ())
        problems = ""
        for _ in range(_ROUNDS):
            try:
                adapter.validate_json(json.dumps(value))
            except ValidationError as e:
                errors = e.errors()
                problems = "; ".join(f"{_where(err['loc'])}: {err['msg']}" for err in errors)
                value = drawer.again(value, [(tuple(err["loc"]), err["type"]) for err in errors])
                continue
            except Exception as e:  # a validator that raised, not refused: all of it again
                problems = f"{type(e).__name__}: {e}"
                value = drawer.again(value, [((), "")])
                continue
            return value
    except Unsampleable:
        raise
    except Exception as e:  # what drawing ran into: the type is unsampleable, said why
        raise Unsampleable(f"{_name(tp)}: {type(e).__name__}: {e}") from None
    raise Unsampleable(f"no valid {_name(tp)} in {_ROUNDS} rounds ({problems})")


def sample(tp: Any, run: int, seed: int = 0) -> Any:
    """:func:`draw`, or None when no valid value was found."""
    try:
        return draw(tp, run, seed)
    except Unsampleable:
        return None


def from_schema(schema: _Schema, run: int, seed: int = 0) -> Any:
    """A JSON value drawn from ``schema`` alone, in run ``run``'s mode, with nothing to
    validate it against (a tool's arguments, an agent's output). :class:`Unsampleable`
    when part of it cannot be drawn."""
    return _Drawer(schema, _rng(run, seed)).draw(schema, mode(run), ())


def _where(loc: Sequence[str | int]) -> str:
    return ".".join(str(part) for part in loc) or "the value"


def _items(value: object) -> Sequence[Any] | None:
    return cast("Sequence[Any]", value) if isinstance(value, list) else None


def _object(value: object) -> dict[str, Any] | None:
    return cast("dict[str, Any]", value) if isinstance(value, dict) else None


def _bounded(n: int, lo: int, hi: int | None) -> int:
    return max(lo, n if hi is None else min(n, hi))


class _Drawer:
    """One draw of one schema: the schema each value was drawn from, by where it sits, so
    the parts validation refused can be drawn again."""

    def __init__(self, root: _Schema, rng: random.Random) -> None:
        self._root = root
        self._rng = rng
        #: Each place's schema, and the ``$ref``s followed on the way down to it.
        self._drawn: dict[_Path, tuple[object, frozenset[str]]] = {}

    # ------------------------------------------------------------------ schemas

    def _pointer(self, ref: str) -> object:
        """The schema a local ``$ref`` (``#/$defs/Name``) points at."""
        if not ref.startswith("#"):
            raise Unsampleable(f"a $ref outside the schema: {ref}")
        found: object = self._root
        for part in ref[1:].split("/")[1:]:
            step = _object(found)
            key = part.replace("~1", "/").replace("~0", "~")
            if step is None or key not in step:
                raise Unsampleable(f"a $ref to nothing: {ref}")
            found = step[key]
        return found

    def _resolved(
        self, schema: object, hops: int = 0, refs: set[str] | None = None
    ) -> dict[str, Any]:
        """``schema`` with its ``$ref`` followed (its own keywords win) and its ``allOf``
        merged into it, each ``$ref`` followed added to ``refs``. ``true`` is any value;
        ``false`` none."""
        if schema is True:
            return {}
        if not isinstance(schema, Mapping):
            raise Unsampleable(f"a schema that allows no value: {schema!r}")
        if hops > _HOPS:
            raise Unsampleable("a $ref or allOf that never ends")
        out = dict(cast("_Schema", schema))
        ref = out.pop("$ref", None)
        if isinstance(ref, str):
            if refs is not None:
                refs.add(ref)
            out = {**self._resolved(self._pointer(ref), hops + 1, refs), **out}
        parts = _items(out.pop("allOf", None)) or ()
        for part in parts:
            more = self._resolved(part, hops + 1, refs)
            properties = {**(out.get("properties") or {}), **(more.get("properties") or {})}
            required = [*(out.get("required") or []), *(more.get("required") or [])]
            out = {**out, **more}
            if properties:
                out["properties"] = properties
            if required:
                out["required"] = list(dict.fromkeys(required))
        return out

    # ------------------------------------------------------------------ drawing

    def draw(
        self, schema: object, how: str, path: _Path, refs: frozenset[str] = frozenset()
    ) -> Any:
        """A value for ``schema`` at ``path`` of the whole, drawn in mode ``how`` below the
        ``$ref``s ``refs``, and ``schema`` noted as what that place is drawn again from."""
        self._drawn[path] = (schema, refs)
        return self._value(schema, how, path, refs)

    def _value(self, schema: object, how: str, path: _Path, refs: frozenset[str]) -> Any:
        if len(path) > _DEEPEST:
            raise Unsampleable(f"nested deeper than {_DEEPEST} at {_where(path)}")
        followed: set[str] = set()
        s = self._resolved(schema, refs=followed)
        if followed & refs or len(path) >= _SHALLOW:
            how = MIN  # a recursion: the rest as min mode draws it, so it ends
        refs = refs | followed
        if "const" in s:
            return s["const"]
        enum = _items(s.get("enum"))
        if enum:
            return self._pick(enum, how)
        branches = _items(s.get("anyOf")) or _items(s.get("oneOf"))
        if branches:
            return self._union(branches, how, path, refs)
        kind = s.get("type")
        kinds = _items(kind)
        if kinds:
            return self._union([{**s, "type": k} for k in kinds], how, path, refs)
        if kind is None:
            if "properties" in s or "additionalProperties" in s:
                kind = "object"
            elif "items" in s or "prefixItems" in s:
                kind = "array"
        if kind == "object":
            return self._object(s, how, path, refs)
        if kind == "array":
            return self._array(s, how, path, refs)
        if kind == "string":
            return self._string(s, how)
        if kind in ("integer", "number"):
            return self._number(s, how, integer=kind == "integer")
        if kind == "boolean":
            return self._rng.random() < 0.5 if how == RANDOM else how != MIN
        if kind == "null":
            return None
        if kind is None:  # any value: its default, else null
            return s.get("default")
        raise Unsampleable(f"an unknown type {kind!r} at {_where(path)}")

    def _pick(self, choices: Sequence[Any], how: str) -> Any:
        if how == MIN:
            return choices[0]
        if how == RANDOM:
            return self._rng.choice(choices)
        return choices[-1]

    def _union(
        self, branches: Sequence[object], how: str, path: _Path, refs: frozenset[str]
    ) -> Any:
        """One branch: null first in min mode (when one is null), the last other branch
        first in max mode, the first in min mode, any at random; a branch that cannot be
        drawn gives way to the next."""
        nulls = [b for b in branches if self._resolved(b).get("type") == "null"]
        others = [b for b in branches if not any(b is n for n in nulls)]
        if nulls and (how == MIN or not others or (how == RANDOM and self._rng.random() < 0.5)):
            return None
        if how == RANDOM:
            others = self._rng.sample(others, len(others))
        elif how != MIN:
            others = others[::-1]
        reasons: list[str] = []
        for branch in others:
            try:
                return self._value(branch, how, path, refs)  # drawn again from the union
            except Unsampleable as e:
                reasons.append(str(e))
        if nulls:
            return None
        raise Unsampleable("; ".join(reasons))

    def _count(self, s: _Schema, how: str, lo_key: str, hi_key: str, large: bool) -> int:
        lo = s.get(lo_key)
        lo = lo if isinstance(lo, int) and lo > 0 else 0
        hi = s.get(hi_key)
        hi = hi if isinstance(hi, int) and hi >= lo else None
        if how == MIN:
            return lo
        if how == RANDOM:
            return self._rng.randint(lo, max(lo, _bounded(_RANDOM_ITEMS, 0, hi)))
        return _bounded(_LARGE_ITEMS if large else _MAX_ITEMS, lo, hi)

    def _object(self, s: _Schema, how: str, path: _Path, refs: frozenset[str]) -> dict[str, Any]:
        properties = _object(s.get("properties")) or {}
        required = set(_items(s.get("required")) or ())
        out: dict[str, Any] = {}
        for name, sub in properties.items():
            if name in required:
                out[name] = self.draw(sub, how, (*path, name), refs)
            elif how == MIN or (how == RANDOM and self._rng.random() < 0.5):
                continue
            else:
                with contextlib.suppress(Unsampleable):  # optional: else left out
                    out[name] = self.draw(sub, how, (*path, name), refs)
        extra = s.get("additionalProperties", True)
        if properties or extra is False:
            return out
        n = self._count(s, how, "minProperties", "maxProperties", large=False)
        names = s.get("propertyNames")
        patterned = list((_object(s.get("patternProperties")) or {}).items())
        if patterned:  # ``dict[Annotated[str, pattern], X]``: keys drawn for the pattern
            pattern, extra = patterned[0]
            names = {"pattern": pattern}
        # A mapping (``dict[str, X]``): keys that read as numbers too (``dict[int, X]``;
        # at random, from 1 to 1000, so a refused ``"0"`` is drawn again as one a
        # ``PositiveInt`` or a ``ge=`` takes), or drawn from ``propertyNames``
        # (``dict[Literal[...], X]``, ``dict[Enum, X]``).
        if names is not None:
            keys = self._keys(names, how, n)
        elif how == RANDOM:
            keys = [str(k) for k in sorted(self._rng.sample(range(1, _BIG + 1), n))]
        else:
            keys = [str(i) for i in range(n)]
        for key in keys:
            out[key] = self.draw(extra if extra is not True else {}, how, (*path, key), refs)
        return out

    def _keys(self, names: object, how: str, n: int) -> list[str]:
        """Up to ``n`` distinct keys valid for ``propertyNames``: from its ``enum`` (or
        ``const``) in the mode's order, else strings drawn for it (a min or max draw is the
        same string every time, so one key)."""
        s = self._resolved(names)
        choices = [s["const"]] if "const" in s else list(_items(s.get("enum")) or ())
        if choices:
            if how == RANDOM:
                choices = self._rng.sample(choices, len(choices))
            elif how != MIN:
                choices = choices[::-1]
            return [str(c) for c in choices[:n]]
        drawn = (self._string(s, how) for _ in range(n))
        return list(dict.fromkeys(drawn))

    def _array(self, s: _Schema, how: str, path: _Path, refs: frozenset[str]) -> list[Any]:
        prefix = _items(s.get("prefixItems")) or ()
        rest = s.get("items", True)
        near_top = len(path) <= _LARGE_DEPTH and not any(isinstance(p, int) for p in path)
        n = self._count(s, how, "minItems", "maxItems", large=how == LARGE and near_top)
        if rest is False:
            n = min(n, len(prefix))
        lo = s.get("minItems")
        lo = lo if isinstance(lo, int) else 0
        out: list[Any] = []
        for i in range(n):
            sub = prefix[i] if i < len(prefix) else rest
            try:
                out.append(self.draw(sub, how, (*path, i), refs))
            except Unsampleable:
                if i < max(lo, len(prefix)):
                    raise
                break  # fewer items are valid too
        return out

    def _number(self, s: _Schema, how: str, *, integer: bool) -> int | float:
        def bound(key: str) -> float | None:
            v = s.get(key)
            return float(v) if isinstance(v, int | float) and not isinstance(v, bool) else None

        lo, hi = bound("minimum"), bound("maximum")
        x_lo, x_hi = bound("exclusiveMinimum"), bound("exclusiveMaximum")
        if x_lo is not None:
            above = math.floor(x_lo) + 1 if integer else math.nextafter(x_lo, math.inf)
            lo = above if lo is None else max(lo, above)
        if x_hi is not None:
            below = math.ceil(x_hi) - 1 if integer else math.nextafter(x_hi, -math.inf)
            hi = below if hi is None else min(hi, below)
        if integer:
            lo = None if lo is None else float(math.ceil(lo))
            hi = None if hi is None else float(math.floor(hi))
        if how == MIN:
            value = 0.0 if (lo is None or lo <= 0) and (hi is None or hi >= 0) else lo
            value = hi if value is None else value
        elif how == RANDOM:
            start = lo if lo is not None else min(0.0, hi if hi is not None else 0.0)
            end = hi if hi is not None else max(start, 0.0) + _BIG
            value = self._rng.uniform(start, end)
            # Two decimals, as an amount has (a Decimal's decimal_places and max_digits are
            # not in its schema), when that stays in bounds.
            rounded = float(round(value)) if integer else round(value, 2)
            value = rounded if start <= rounded <= end else value
        else:
            value = hi if hi is not None else max(lo if lo is not None else 0.0, float(_BIG))
        assert value is not None
        step = bound("multipleOf")
        if step is not None and step > 0:
            if not math.isfinite(value / step):
                raise Unsampleable(f"no multiple of {step:g} near {value:g} is a float")
            snapped = math.ceil(value / step) * step
            value = snapped if hi is None or snapped <= hi else math.floor(value / step) * step
        return int(value) if integer else float(value)

    def _string(self, s: _Schema, how: str) -> str:
        fmt, pattern = s.get("format"), s.get("pattern")
        if fmt in _PLAIN_FORMATS:  # bytes, a secret: a string of its length
            fmt = None
        drawn = _FORMATS.get(fmt) if isinstance(fmt, str) else None
        lo = s.get("minLength")
        lo = lo if isinstance(lo, int) and lo > 0 else 0
        hi = s.get("maxLength")
        hi = hi if isinstance(hi, int) and hi >= lo else None
        if pattern is not None or (fmt is not None and drawn is None and fmt != "base64"):
            examples = _items(s.get("examples"))
            if examples:
                return str(self._pick(examples, how))
            if isinstance(s.get("default"), str):
                return s["default"]
        if isinstance(pattern, str):
            return _Pattern(self._rng, how).draw(pattern, lo, hi)
        if drawn is not None:
            return drawn(how, self._rng)
        content = s.get("contentSchema")
        if content is not None:  # ``Json[T]``: a T, as JSON text (validated with the whole)
            return json.dumps(_Drawer(self._root, self._rng).draw(content, how, ()))
        if fmt == "base64":  # ``Base64Bytes``: the lengths are the decoded value's
            return base64.b64encode(self._plain(how, lo, hi).encode()).decode()
        return self._plain(how, lo, hi)

    def _plain(self, how: str, lo: int, hi: int | None) -> str:
        """A string of letters and digits, ``lo`` to ``hi`` long, in mode ``how``."""
        if how == MIN:
            return "a" * lo
        if how == RANDOM:
            n = self._rng.randint(
                lo, lo + _RANDOM_LONG if hi is None else min(hi, lo + _RANDOM_LONG)
            )
            return "".join(self._rng.choice(_ALPHABET) for _ in range(n))
        return "z" * max(lo, min(_LONGEST, _LONG if hi is None else hi))

    # ------------------------------------------------------------------ again

    def again(self, value: Any, errors: Sequence[tuple[Sequence[str | int], str]]) -> Any:
        """``value`` with what each error (its location, its type) points at drawn again in
        random mode: the deepest part of the location the value holds (a missing property's
        object, a union's member, the mapping a refused key is in), outermost first, each
        once, in the order of the errors (never of a set's: that changes with the process's
        string hashing, and so would the values). A datetime's offset the type refuses (or
        wants) is taken off (or put on) in place instead."""
        paths: dict[_Path, None] = {}
        for loc, kind in errors:
            if kind in (_NAIVE, _AWARE):
                found, rezoned = _rezoned(value, loc, naive=kind == _NAIVE)
                if found:
                    value = rezoned
                    continue
            if "[key]" in loc:  # (..., key, "[key]"): the mapping, with fresh keys
                loc = loc[: max(0, list(loc).index("[key]") - 1)]
            path: _Path = ()
            here: Any = value
            for part in loc:
                step: Any
                listed, mapped = _items(here), _object(here)
                if isinstance(part, int) and listed is not None and 0 <= part < len(listed):
                    step = listed[part]
                elif isinstance(part, str) and mapped is not None and part in mapped:
                    step = mapped[part]
                else:
                    break
                path, here = (*path, part), step
            while path not in self._drawn:
                path = path[:-1]
            paths[path] = None
        for path in sorted(paths, key=len):  # stable: the errors' order among equals
            if any(path[: len(p)] == p for p in paths if len(p) < len(path)):
                continue  # drawn again with its ancestor
            schema, refs = self._drawn[path]
            fresh = self.draw(schema, RANDOM, path, refs)
            if not path:
                value = fresh
                continue
            parent: Any = value
            for part in path[:-1]:
                parent = parent[part]
            parent[path[-1]] = fresh
        return value


def _rezoned(value: Any, loc: Sequence[str | int], *, naive: bool) -> tuple[bool, Any]:
    """``value`` with the datetime drawn at ``loc`` given no offset (``naive``) or ``Z``, in
    place; whether a datetime string was there to change."""
    parent: Any = None
    here: Any = value
    for part in loc:
        listed, mapped = _items(here), _object(here)
        if isinstance(part, int) and listed is not None and 0 <= part < len(listed):
            parent, here = listed, listed[part]
        elif isinstance(part, str) and mapped is not None and part in mapped:
            parent, here = mapped, mapped[part]
        else:
            return False, value
    if not isinstance(here, str) or "T" not in here or naive != here.endswith("Z"):
        return False, value
    changed = here[:-1] if naive else here + "Z"
    if not loc:
        return True, changed
    parent[loc[-1]] = changed
    return True, value


# --------------------------------------------------------------------------- formats


def _moment(how: str, rng: random.Random) -> datetime:
    if how == MIN:
        return _EPOCH
    if how == RANDOM:
        return _EPOCH + timedelta(seconds=rng.randrange(4_102_444_800))
    return datetime(2099, 12, 31, 23, 59, 59, tzinfo=UTC)


def _bits(how: str, rng: random.Random, width: int) -> int:
    """0 in min mode, every bit set in max and large mode, random bits in random mode."""
    if how == MIN:
        return 0
    return rng.getrandbits(width) if how == RANDOM else (1 << width) - 1


def _uuid(version: int) -> _Format:
    """A UUID of ``version`` (RFC 9562 variant), as ``uuid.UUID(version=)`` builds one for
    1 to 5 and Python 3.12 does not for 6 to 8."""

    def drawn(how: str, rng: random.Random) -> str:
        bits = _bits(how, rng, 128)
        bits = (bits & ~(0xC000 << 48)) | (0x8000 << 48)
        bits = (bits & ~(0xF000 << 64)) | (version << 76)
        return str(uuid.UUID(int=bits))

    return drawn


def _host(how: str, rng: random.Random) -> str:
    if how == RANDOM:
        return "".join(rng.choice(_ALPHABET[:26]) for _ in range(rng.randint(1, 8))) + ".example"
    return _HOSTS[0] if how == MIN else _HOSTS[1]


def _ipv4(how: str, rng: random.Random) -> str:
    bits = _bits(how, rng, 32)
    return ".".join(str((bits >> shift) & 0xFF) for shift in (24, 16, 8, 0))


def _ipv6(how: str, rng: random.Random) -> str:
    bits = _bits(how, rng, 128)
    return ":".join(f"{(bits >> shift) & 0xFFFF:x}" for shift in range(112, -1, -16))


def _date_time(how: str, rng: random.Random) -> str:
    return _moment(how, rng).isoformat().replace("+00:00", "Z")


def _date(how: str, rng: random.Random) -> str:
    return _moment(how, rng).date().isoformat()


def _time(how: str, rng: random.Random) -> str:
    return _moment(how, rng).time().isoformat()


def _duration(how: str, rng: random.Random) -> str:
    return f"PT{_bits(how, rng, 16)}S"


def _email(how: str, rng: random.Random) -> str:
    return f"{_host(how, rng).split('.')[0]}@{_HOSTS[0]}"


def _url(how: str, rng: random.Random) -> str:
    return f"https://{_host(how, rng)}/"


def _constant(text: str) -> _Format:
    return lambda how, rng: text


def _suffixed(drawn: _Format, suffix: str) -> _Format:
    return lambda how, rng: drawn(how, rng) + suffix


_FORMATS: dict[str, _Format] = {
    "date-time": _date_time,
    "date": _date,
    "time": _time,
    "duration": _duration,
    "uuid": _uuid(4),
    **{f"uuid{v}": _uuid(v) for v in (1, 3, 4, 5, 6, 7, 8)},
    "email": _email,
    "hostname": _host,
    "ipv4": _ipv4,
    "ipv6": _ipv6,
    "ipvanyaddress": _ipv4,
    "ipv4network": _constant("0.0.0.0/0"),
    "ipv6network": _constant("::/0"),
    "ipvanynetwork": _constant("0.0.0.0/0"),
    "ipv4interface": _suffixed(_ipv4, "/32"),
    "ipv6interface": _suffixed(_ipv6, "/128"),
    "ipvanyinterface": _suffixed(_ipv4, "/32"),
    **dict.fromkeys(
        ("uri", "url", "iri", "uri-reference", "iri-reference", "multi-host-uri"), _url
    ),
    **dict.fromkeys(("path", "regex"), _constant("a")),
}


# --------------------------------------------------------------------------- patterns

#: Python's own regex parser (``re._parser``, 3.11 on): private, so imported by name and
#: read without types; only the opcodes below are followed, anything else gives up.
_SRE: Any = importlib.import_module("re._parser")


#: What a pattern holds that :class:`_Pattern` does not follow, by opcode.
_UNFOLLOWED = {
    "ASSERT": "lookaround",
    "ASSERT_NOT": "lookaround",
    "GROUPREF": "backreference",
    "GROUPREF_EXISTS": "conditional",
}


@dataclass(frozen=True)
class _Repeat:
    lo: int
    hi: int
    body: tuple[_Node, ...]


@dataclass(frozen=True)
class _Branch:
    options: tuple[tuple[_Node, ...], ...]


@dataclass(frozen=True)
class _Fail:
    """What gives up, said when it is reached (an alternative not taken never is)."""

    why: str


#: A pattern read once: a literal character, the characters a class matches (in order), a
#: repeat, an alternative, what gives up, or a group's own sequence.
_Node: TypeAlias = "str | list[str] | _Repeat | _Branch | _Fail | tuple[_Node, ...]"


@functools.lru_cache(maxsize=512)
def _compiled(pattern: str) -> tuple[_Node, ...]:
    """``pattern`` read once (every string drawn for a ``pattern`` reads it again
    otherwise, and works out every class's characters again for each character)."""
    try:
        tree = _SRE.parse(pattern)
    except Exception as e:  # a pattern Python cannot read (Rust regex syntax)
        raise Unsampleable(f"cannot read the pattern {pattern!r}: {e}") from None
    return _sequence_of(tree, pattern)


def _sequence_of(items: Any, pattern: str) -> tuple[_Node, ...]:
    return tuple(_node(op, av, pattern) for op, av in items if str(op.name) != "AT")


def _node(op: Any, av: Any, pattern: str) -> _Node:
    name = str(op.name)
    if name == "LITERAL":
        return chr(av)
    if name in ("NOT_LITERAL", "ANY"):
        return [c for c in _PRINTABLE if name == "ANY" or ord(c) != av]
    if name == "IN":
        try:
            return _class(av)
        except Unsampleable as e:
            return _Fail(str(e))
    if name in ("MAX_REPEAT", "MIN_REPEAT", "POSSESSIVE_REPEAT"):
        lo, hi, sub = av
        return _Repeat(int(lo), int(hi), _sequence_of(sub, pattern))
    if name == "SUBPATTERN":
        return _sequence_of(av[-1], pattern)
    if name == "ATOMIC_GROUP":
        return _sequence_of(av, pattern)
    if name == "BRANCH":
        return _Branch(tuple(_sequence_of(b, pattern) for b in av[1]))
    return _Fail(f"the pattern {pattern!r} has a {_UNFOLLOWED.get(name, name.lower())}")


def _class(items: Any) -> list[str] | _Fail:
    """The characters a class ``[...]`` matches, from printable ASCII and each range's and
    literal's own first character (a class of letters outside ASCII); a class that matches
    none of them (``\\s``), from a space and a tab."""
    extra: list[str] = []
    for op, av in items:
        if str(op.name) == "LITERAL":
            extra.append(chr(av))
        elif str(op.name) == "RANGE":
            extra.append(chr(av[0]))
    candidates = sorted(set(_PRINTABLE) | set(extra))
    choices = [c for c in candidates if _in_class(c, items)]
    choices = choices or [c for c in _SPACES if _in_class(c, items)]
    return choices or _Fail("a character class nothing printable matches")


class _Pattern:
    """A string that a pattern matches, from the pattern's parse tree: literals, classes
    (``[a-z]``, ``[^\\s]``, ``\\d``, ``\\s``), ``.``, groups, alternatives and repeats;
    anchors are nothing. A lookaround, a backreference or a conditional gives up. The tree
    is followed, not matched against: what comes out is validated like any other value.

    ``fewest`` takes every repeat at its minimum; ``extra`` more repetitions are given to
    the repeats drawn first that have room, so a ``minLength`` is met."""

    def __init__(self, rng: random.Random, how: str, *, fewest: bool = False, extra: int = 0):
        self._rng, self._how = rng, how
        self._fewest, self._extra = fewest, extra

    def draw(self, pattern: str, shortest: int = 0, longest: int | None = None) -> str:
        """A string ``pattern`` matches, ``shortest`` to ``longest`` long where the pattern's
        repeats allow it."""
        tree = _compiled(pattern)
        text = self._sequence(tree)
        fewest = False
        if longest is not None and len(text) > longest:
            fewest = True
            text = _Pattern(self._rng, self._how, fewest=True)._sequence(tree)
        extra = 0
        for _ in range(4):  # each round gives what is still missing
            if len(text) >= shortest:
                break
            extra += shortest - len(text)
            text = _Pattern(self._rng, self._how, fewest=fewest, extra=extra)._sequence(tree)
        return text

    def _sequence(self, nodes: tuple[_Node, ...]) -> str:
        return "".join(self._item(node) for node in nodes)

    def _item(self, node: _Node) -> str:
        if isinstance(node, str):
            return node
        if isinstance(node, list):
            return self._pick(node)
        if isinstance(node, tuple):
            return self._sequence(node)
        if isinstance(node, _Repeat):
            return "".join(self._sequence(node.body) for _ in range(self._count(node)))
        if isinstance(node, _Branch):
            return self._sequence(self._pick(node.options))
        raise Unsampleable(node.why)

    def _count(self, node: _Repeat) -> int:
        lo, hi = node.lo, node.hi
        if self._fewest or self._how == MIN:
            n = lo
        elif self._how == RANDOM:
            n = self._rng.randint(lo, min(hi, lo + _RANDOM_ITEMS))
        else:
            n = min(hi, lo + _REPEATS)
        more = max(0, min(self._extra, hi - n))
        self._extra -= more
        return n + more

    def _pick(self, choices: Sequence[_T]) -> _T:
        if self._how == MIN:
            return choices[0]
        if self._how == RANDOM:
            return self._rng.choice(choices)
        return choices[-1]


_CATEGORIES: dict[str, Callable[[str], bool]] = {
    "CATEGORY_DIGIT": str.isdigit,
    "CATEGORY_NOT_DIGIT": lambda c: not c.isdigit(),
    "CATEGORY_SPACE": str.isspace,
    "CATEGORY_NOT_SPACE": lambda c: not c.isspace(),
    "CATEGORY_WORD": lambda c: c.isalnum() or c == "_",
    "CATEGORY_NOT_WORD": lambda c: not (c.isalnum() or c == "_"),
}


def _in_class(c: str, items: Any) -> bool:
    """Whether ``c`` is in the class ``[...]`` whose parsed items are ``items``."""
    negate = False
    found = False
    for op, av in items:
        name = str(op.name)
        if name == "NEGATE":
            negate = True
        elif name == "LITERAL":
            found = found or ord(c) == av
        elif name == "RANGE":
            found = found or av[0] <= ord(c) <= av[1]
        elif name == "CATEGORY":
            test = _CATEGORIES.get(str(av.name))
            found = found or (test is not None and test(c))
        else:
            raise Unsampleable(f"a character class with a {name.lower()}")
    return found != negate
