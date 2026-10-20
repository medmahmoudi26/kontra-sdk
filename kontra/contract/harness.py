"""The published contract against temporal-agent-harness's schema for the same handler
(decisions I33).

Authored once, derived twice, published once. A bound handler's pydantic models
(``serve(ReconAgent.sweep)``) are walked by two emitters: :func:`kontra.contract.derive_document`,
whose document is canonicalised, hashed and published -- what a buyer buys and ``kontra pull``
generates from -- and the harness's ``agent_schema``, which the seller's harness-codegen turns
into TypeScript for their own UI and which is never published. This module compares the two
for one handler, path by path, so that two emitters walking the same models differently is a
warning at ``kontra publish`` and a failing seam ③ test, not a buyer's surprise.

What is compared: the contract's ``execute`` input and output against the handler's ``input``
and ``output`` in the harness's document, each with everything it references. Nothing else of
the harness's document is read -- the init data, the agent's other handlers and its declared
states are the agent's envelope, never the operation. Both emitters describe an input in
pydantic's validation mode and an output in its serialization mode (decisions I38), so the
two documents say the same once both are in one canonical form, which removes every
difference that does not change which JSON values are valid, each by a rule I33 lists and
``tests/contract/test_harness_rules.py`` tests:

* references are followed on both sides, and a definition is matched by where it is used, not
  by its name (``ReportOutput`` against the harness's ``Report-Output``, ``KontraArtifact``
  against ``Artifact``, an enum Kontra writes inline against the harness's ``$ref``); a
  recursive model is compared once. JSON Schema 2020-12 asserts a ``$ref``'s definition and
  the members beside it both, so a use site joins its definition as their conjunction: its
  annotations replace the definition's, an assertion the definition does not make joins it,
  and one both make is the one value that says what the two do (the tighter bound, the common
  ``enum`` values, ``integer`` within ``number``). Where no one value does, the two stay side
  by side, ``allOf``, and differ from the contract, which never writes one;
* annotations are dropped: ``title``, ``description``, ``examples``, ``$comment``,
  ``deprecated``, ``readOnly``, ``writeOnly``; so is the OpenAPI ``discriminator`` (the
  branches' ``const`` tags select the branch);
* ``required`` is a set (sorted; empty is absent), and values are compared as RFC 8785
  canonical JSON (key order, ``1`` and ``1.0``);
* the contract closes models by policy: its ``additionalProperties: false`` against the
  harness's none is no difference. Kontra closes every model (decisions I2, section 11.3),
  and a model the harness's schema leaves without one has pydantic's default
  ``extra="ignore"``, which drops other keys on input and never emits one on output, so no
  value exchanged differs. ``additionalProperties: true`` is its absence, on either side,
  except against the contract's ``false``: there it is ``extra="allow"`` (the model keeps
  other keys and serializes them), kept, and reported ``open``. A schema there (typed
  extras) is no ``true``, and is compared like any other member;
* ``anyOf: [T, {type: null}]`` is ``oneOf: [T, {type: null}]`` when ``T`` cannot be null;
* a ``default`` of ``null``, a list or an object is no default (decisions I2: null is the
  absence of one, and nexgen materialises only scalar defaults), and neither is a default on
  a member its object requires, which never applies: the harness's output schema keeps a
  field's default beside the ``required`` that lists it, and the contract writes none there
  (I38). Any other scalar default is compared: a buyer's generated code fills it in.

What remains is reported, each a :class:`Difference`: ``open`` where the harness's schema
says ``additionalProperties: true`` (``extra="allow"``) and the contract closes the model,
which the seller can act on; anything else is the two emitters describing the same model
differently, a bug in one of them, reported as it is.

The harness's schema comes from its own CLI, ``temporal-agent-harness schema module:Class``,
run by :func:`harness_schema` as a subprocess of this interpreter: the same document
harness-codegen reads, and Kontra never imports the harness (decisions I29). This module holds
the CLI's name and the document's shape as constants, checked against the harness by
``tests/contract/test_harness.py``.
"""

from __future__ import annotations

import json
import math
import os
import selectors
import subprocess
import sys
import tempfile
import time
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from importlib.metadata import entry_points
from pathlib import Path
from typing import IO, Any, Final, Literal, cast

from kontra.contract.canonical import canonical_json
from kontra.contract.errors import ContractError, join_path
from kontra.contract.validate import validate_document

__all__ = [
    "ABSENT",
    "HARNESS_SCRIPT",
    "OPEN_NOTE",
    "OTHER_NOTE",
    "SCHEMA_COMMAND",
    "SCHEMA_DEFS",
    "SCHEMA_HANDLERS",
    "SCHEMA_INPUT",
    "SCHEMA_OUTPUT",
    "SCHEMA_OUTPUT_OPTION",
    "SCHEMA_TIMEOUT",
    "Difference",
    "HarnessSchemaError",
    "agent_target",
    "compare_handler",
    "describe",
    "harness_installed",
    "harness_schema",
]

# --------------------------------------------------------------------------- the harness's shape
#
# temporal-agent-harness's schema CLI and document, as Kontra relies on it (0.6.0 and 0.7.0 are
# identical here). Constants only: Kontra must never import the harness.

#: The harness's console script, and its subcommand that prints an agent's schema.
HARNESS_SCRIPT = "temporal-agent-harness"
SCHEMA_COMMAND = "schema"
#: ``schema``'s option that writes the document to a file instead of standard output (the
#: seller's agent module is imported first, and may print).
SCHEMA_OUTPUT_OPTION = "-o"
#: The document's members Kontra reads: ``handlers.<name>.input`` and ``.output``, each a
#: ``$ref`` into ``$defs``.
SCHEMA_HANDLERS = "handlers"
SCHEMA_INPUT = "input"
SCHEMA_OUTPUT = "output"
SCHEMA_DEFS = "$defs"

#: How long the harness may take to import the agent's module and print its schema.
SCHEMA_TIMEOUT = 60.0

#: Runs a console script with this interpreter, as its own wrapper would: the script's name is
#: the first argument, the rest are its arguments. The entry point is read from the installed
#: distribution's metadata, so nothing here names a module of the harness.
_RUN_SCRIPT = """\
import sys
from importlib.metadata import entry_points

[script] = entry_points(group="console_scripts", name=sys.argv[1])
sys.argv[:2] = [script.name]
sys.exit(script.load()())
"""

#: How much of the harness's error output a refusal quotes.
_ERROR_CHARS = 300
#: How much of the end of that output is read to find the line quoted: the seller's module is
#: imported in the child, and may log without bound.
_STDERR_TAIL = 64 * 1024
#: The largest schema document read (an agent's every handler, init data and states).
_SCHEMA_BYTES = 16 * 1024 * 1024


class HarnessSchemaError(Exception):
    """The harness is installed, but it gave no usable schema for this agent or handler."""


def harness_installed() -> bool:
    """Whether this interpreter's environment has the harness's console script (its
    distribution's metadata; nothing is imported)."""
    return bool(entry_points(group="console_scripts", name=HARNESS_SCRIPT))


def agent_target(agent: type) -> str:
    """``module:Qualname`` of an agent class: what the harness's ``schema`` imports."""
    target = f"{agent.__module__}:{agent.__qualname__}"
    _check_target(target)
    return target


def _check_target(target: str) -> None:
    module, sep, qualname = target.partition(":")
    parts = [*module.split("."), *qualname.split(".")]
    if not sep or not all(p.isidentifier() for p in parts):
        raise HarnessSchemaError(f"{target!r} is not module.path:ClassName")


def harness_schema(
    target: str, *, cwd: Path | None = None, timeout: float = SCHEMA_TIMEOUT
) -> dict[str, Any]:
    """The harness's schema document for the agent class ``target`` (``module:Class``), from
    ``temporal-agent-harness schema``, run by this interpreter with this process's
    ``sys.path`` (so the agent's module is found where Kontra found it) in ``cwd``.

    Raises :class:`HarnessSchemaError` with the harness's own reason when it gives none."""
    _check_target(target)
    with tempfile.TemporaryDirectory(prefix="kontra-harness-") as tmp:
        out = Path(tmp) / "schema.json"
        env = dict(os.environ)
        env["PYTHONPATH"] = os.pathsep.join(p for p in sys.path if p)
        argv = [
            sys.executable,
            "-c",
            _RUN_SCRIPT,
            HARNESS_SCRIPT,
            SCHEMA_COMMAND,
            target,
            SCHEMA_OUTPUT_OPTION,
            str(out),
        ]
        try:
            status, stderr = _run(argv, cwd=cwd, env=env, timeout=timeout)
        except subprocess.TimeoutExpired:
            raise HarnessSchemaError(
                f"`{HARNESS_SCRIPT} {SCHEMA_COMMAND} {target}` did not answer within {timeout:g}s"
            ) from None
        except OSError as exc:
            raise HarnessSchemaError(f"cannot run {HARNESS_SCRIPT}: {exc}") from None
        if status != 0:
            said = _last_line(stderr) or f"exit status {status}"
            raise HarnessSchemaError(f"`{HARNESS_SCRIPT} {SCHEMA_COMMAND} {target}`: {said}")
        try:
            with out.open("rb") as f:
                raw = f.read(_SCHEMA_BYTES + 1)
        except OSError as exc:
            raise HarnessSchemaError(f"{HARNESS_SCRIPT} wrote no schema: {exc}") from None
    if len(raw) > _SCHEMA_BYTES:
        raise HarnessSchemaError(
            f"{HARNESS_SCRIPT}'s schema is larger than {_SCHEMA_BYTES} bytes; not read"
        )
    try:
        data: Any = json.loads(raw.decode("utf-8"))
    except (ValueError, RecursionError) as exc:
        raise HarnessSchemaError(f"{HARNESS_SCRIPT}'s schema is not JSON: {exc}") from None
    if not isinstance(data, dict):
        raise HarnessSchemaError(f"{HARNESS_SCRIPT}'s schema is not a JSON object")
    return cast(dict[str, Any], data)


def _run(
    argv: list[str], *, cwd: Path | None, env: dict[str, str], timeout: float
) -> tuple[int, bytes]:
    """Run ``argv`` to its end: its exit status and the last :data:`_STDERR_TAIL` bytes of
    its standard error, read as they come, so no more than that is ever held, in memory or
    on disk, however much it writes. Raises :class:`subprocess.TimeoutExpired` after
    ``timeout`` seconds (a descendant holding standard error open counts), the child
    killed."""
    proc = subprocess.Popen(  # noqa: S603 -- argv, no shell
        argv,
        cwd=cwd,
        env=env,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )
    stream = cast(IO[bytes], proc.stderr)
    deadline = time.monotonic() + timeout
    tail = bytearray()
    try:
        with stream, selectors.DefaultSelector() as ready:
            ready.register(stream, selectors.EVENT_READ)
            while True:
                left = deadline - time.monotonic()
                if left <= 0:
                    raise subprocess.TimeoutExpired(argv, timeout)
                if not ready.select(left):
                    continue
                chunk = os.read(stream.fileno(), _STDERR_TAIL)
                if not chunk:
                    break
                tail += chunk
                del tail[:-_STDERR_TAIL]
        return proc.wait(max(0.0, deadline - time.monotonic())), bytes(tail)
    except BaseException:
        proc.kill()
        proc.wait()
        raise


def _last_line(stderr: bytes) -> str:
    lines = [ln.strip() for ln in stderr.decode("utf-8", "replace").splitlines() if ln.strip()]
    return lines[-1][:_ERROR_CHARS] if lines else ""


# --------------------------------------------------------------------------- the comparison

Side = Literal["input", "output"]

#: What an ``open`` difference means, and what any other one does.
OPEN_NOTE: Final = (
    "the contract closes every model (no keys but its fields) and the harness's schema "
    'leaves this one open, as extra="allow" does: the model keeps other keys and serializes '
    "them into its reply, which the contract refuses, so a reply that carries one fails the "
    'job; pydantic\'s default (extra="ignore", which drops them) or extra="forbid" on the '
    "model makes the two agree"
)
OTHER_NOTE: Final = (
    "the two emitters describe the same model differently, which neither should (a bug in "
    "one of them): please report it, with the model, at "
    "https://github.com/medmahmoudi26/kontra-sdk/issues"
)


class _Absent:
    """A member one side does not have."""

    def __repr__(self) -> str:
        return "ABSENT"


#: The value of a :class:`Difference` side that has no such member.
ABSENT: Final = _Absent()


@dataclass(frozen=True)
class Difference:
    """One place where the contract and the harness's schema still differ after both are in
    the canonical form. ``path`` is in the contract's terms (``$defs.Report.required``);
    ``contract``/``harness`` are the values there, or :data:`ABSENT`. ``open`` is the
    harness's ``additionalProperties: true`` (``extra="allow"``) against the contract's
    ``false``; any other difference is the emitters disagreeing."""

    side: Side
    path: str
    contract: Any
    harness: Any
    open: bool = False

    def __str__(self) -> str:
        return (
            f"{self.side:<6} {self.path}: contract {_show(self.contract)}, "
            f"harness {_show(self.harness)}"
        )


def describe(differences: Iterable[Difference]) -> list[str]:
    """Lines for a person: one per difference, then what they mean."""
    found = list(differences)
    lines = [str(d) for d in found]
    opened = sum(d.open for d in found)
    if opened:
        lines.append(f"(open) {OPEN_NOTE}")
    if opened < len(found):
        lines.append(("the other lines: " if opened else "") + OTHER_NOTE)
    return lines


def _show(value: Any) -> str:
    if value is ABSENT:
        return "(absent)"
    text = json.dumps(value, sort_keys=True, ensure_ascii=True, separators=(",", ":"))
    return text if len(text) <= 160 else text[:157] + "..."


_ANNOTATIONS = frozenset(
    {"title", "description", "examples", "$comment", "deprecated", "readOnly", "writeOnly"}
)
#: Dropped with the annotations: OpenAPI's, never a JSON Schema 2020-12 assertion.
_DISCRIMINATOR = "discriminator"
#: Members whose value is one schema, a map of names to schemas, or a list of schemas.
_ONE = frozenset(
    {
        "items",
        "contains",
        "propertyNames",
        "not",
        "if",
        "then",
        "else",
        "additionalProperties",
        "unevaluatedProperties",
        "unevaluatedItems",
    }
)
_MAP = frozenset({"properties", "patternProperties", "dependentSchemas"})
_LIST = frozenset({"oneOf", "anyOf", "allOf", "prefixItems"})
_DEFS_REF = "#/$defs/"
#: The member Kontra's contract writes ``false`` on every model (section 11.3).
_EXTRA = "additionalProperties"
_NULL: Mapping[str, Any] = {"type": "null"}
#: What a ``$ref``'s use site writes over its definition's: annotations, which assert nothing
#: (a field's own ``description`` or ``default`` over its type's).
_OVERRIDE = _ANNOTATIONS | {"default", _DISCRIMINATOR}
#: Keywords that read one another in one schema object (``additionalProperties`` refuses the
#: keys ``properties`` and ``patternProperties`` beside it do not name): a use site's joins
#: its definition only where the definition writes none of the group, or the group alike.
_TOGETHER = (
    frozenset({"properties", "patternProperties", "additionalProperties"}),
    frozenset({"prefixItems", "items"}),
    frozenset({"contains", "minContains", "maxContains"}),
    frozenset({"if", "then", "else"}),
)
#: Keywords that read every applicator beside them.
_UNEVALUATED = frozenset({"unevaluatedProperties", "unevaluatedItems"})
#: Bounds two schemas may each write: both hold, so the conjunction is the tighter.
_UPPER = frozenset({"maximum", "exclusiveMaximum", "maxLength", "maxItems", "maxProperties"})
_LOWER = frozenset({"minimum", "exclusiveMinimum", "minLength", "minItems", "minProperties"})
#: How many ``$ref`` aliases are followed in a row (``A: {$ref: B, description}``).
_ALIASES = 32


def compare_handler(
    document: Mapping[str, Any], schema: Mapping[str, Any], handler: str
) -> tuple[Difference, ...]:
    """Where the contract ``document`` (sealed, as published) and the harness's ``schema``
    (``agent_schema``'s document) differ for ``handler``: its input against ``execute``'s
    input, its output against ``execute``'s output. Empty means equal. Sorted by side, then
    path. Raises :class:`HarnessSchemaError` if ``schema`` has no such handler."""
    info = validate_document(document)
    cdefs = cast(Mapping[str, Any], document["$defs"])
    handlers = schema.get(SCHEMA_HANDLERS)
    entry = cast(Mapping[str, Any], handlers).get(handler) if isinstance(handlers, dict) else None
    hdefs = schema.get(SCHEMA_DEFS, {})
    if not isinstance(entry, dict) or not isinstance(hdefs, dict):
        raise HarnessSchemaError(f"the harness's schema has no handler {handler!r}")
    entry = cast(Mapping[str, Any], entry)
    found: list[Difference] = []
    for side, root in (("input", info.input_type), ("output", info.output_type)):
        h = entry.get(SCHEMA_INPUT if side == "input" else SCHEMA_OUTPUT)
        if not isinstance(h, dict):
            raise HarnessSchemaError(f"the harness's schema has no {side} for {handler!r}")
        walk = _Walk(cast(Side, side), cdefs, cast(Mapping[str, Any], hdefs))
        walk.run({"$ref": _DEFS_REF + root}, cast(Mapping[str, Any], h), f"execute.{side}")
        found.extend(walk.found)
    return tuple(sorted(found, key=lambda d: (d.side != "input", d.path)))


def _ref_name(node: Mapping[str, Any], defs: Mapping[str, Any]) -> str | None:
    ref = node.get("$ref")
    if isinstance(ref, str) and ref.startswith(_DEFS_REF) and ref[len(_DEFS_REF) :] in defs:
        return ref[len(_DEFS_REF) :]
    return None


def _resolve(node: Mapping[str, Any], defs: Mapping[str, Any], depth: int = 0) -> Mapping[str, Any]:
    """A ``$ref`` node as one schema: its definition (itself resolved, if it is an alias of
    another) and the use site's members together. JSON Schema 2020-12 asserts the definition
    and the members beside the ``$ref`` both (``{$ref: X, k: v}`` is ``{allOf: [X, {k:
    v}]}``), so they are joined only as their conjunction (:func:`_join`): the use site's
    annotations replace the definition's, and its assertions join it. Where no one schema
    says what the two do, the node is that ``allOf``, the use site's annotations beside it."""
    name = _ref_name(node, defs)
    if name is None or depth > _ALIASES:
        return node
    body = defs[name]
    target = (
        _resolve(cast(Mapping[str, Any], body), defs, depth + 1) if isinstance(body, dict) else {}
    )
    notes = {k: v for k, v in node.items() if k in _OVERRIDE}
    site = {k: v for k, v in node.items() if k != "$ref" and k not in _OVERRIDE}
    joined = _join(target, site)
    if joined is None:
        return {**notes, "allOf": [dict(target), site]}
    return {**joined, **notes}


def _join(body: Mapping[str, Any], site: Mapping[str, Any]) -> dict[str, Any] | None:
    """``body`` and a use site's assertions ``site`` as one schema object that asserts what
    the two do side by side, or None where there is none: an assertion the body does not make
    joins it, one it makes alike is kept, one it makes otherwise is conjoined
    (:func:`_conjoin`); keywords that read each other (:data:`_TOGETHER`) join only a body
    that writes none of their group or the group alike, and nothing joins beside an
    ``unevaluated*``."""
    out = dict(body)
    if not site:
        return out
    if _UNEVALUATED & (set(body) | set(site)):
        return None
    for key, value in site.items():
        group = next((g for g in _TOGETHER if key in g), None)
        if group is not None:
            theirs = {k: v for k, v in body.items() if k in group}
            if theirs and not _same({k: v for k, v in site.items() if k in group}, theirs):
                return None
            out[key] = value
        elif key not in body or _same(body[key], value):
            out[key] = value
        else:
            both = _conjoin(key, body[key], value)
            if both is ABSENT:
                return None
            out[key] = both
    return out


def _number(value: Any) -> bool:
    return isinstance(value, int | float) and not isinstance(value, bool)


def _positive_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value > 0


def _conjoin(key: str, a: Any, b: Any) -> Any:
    """One value of ``key`` that asserts what ``a`` and ``b`` both do, or :data:`ABSENT`."""
    if key in _UPPER and _number(a) and _number(b):
        return min(a, b)
    if key in _LOWER and _number(a) and _number(b):
        return max(a, b)
    if key == "type" and isinstance(a, str) and isinstance(b, str):
        return "integer" if {a, b} == {"integer", "number"} else ABSENT
    if key == "enum" and isinstance(a, list) and isinstance(b, list):
        later = cast(list[Any], b)
        both = [v for v in cast(list[Any], a) if any(_same(v, w) for w in later)]
        return both or ABSENT
    if key == "multipleOf" and _positive_int(a) and _positive_int(b):
        return math.lcm(cast(int, a), cast(int, b))
    if key == "uniqueItems" and isinstance(a, bool) and isinstance(b, bool):
        return a or b
    return ABSENT


def _rejects_null(node: Any, defs: Mapping[str, Any], depth: int = 0) -> bool:
    """Whether no schema-valid value of ``node`` is ``null`` (conservatively: unsure is no)."""
    if not isinstance(node, dict) or depth > 32:
        return False
    schema = cast(Mapping[str, Any], node)
    if _ref_name(schema, defs) is not None:
        return _rejects_null(_resolve(schema, defs), defs, depth + 1)
    conjuncts = schema.get("allOf")
    if isinstance(conjuncts, list) and any(
        _rejects_null(b, defs, depth + 1) for b in cast(list[Any], conjuncts)
    ):
        return True
    kind = schema.get("type")
    if isinstance(kind, str):
        return kind != "null"
    if isinstance(kind, list):
        return "null" not in kind
    if "const" in schema:
        return schema["const"] is not None
    if isinstance(schema.get("enum"), list):
        return None not in cast(list[Any], schema["enum"])
    for key in ("oneOf", "anyOf"):
        branches = schema.get(key)
        if isinstance(branches, list) and branches:
            return all(_rejects_null(b, defs, depth + 1) for b in cast(list[Any], branches))
    return False


def _closed_alike(c: Any, h: Any) -> bool:
    """Whether the contract's ``additionalProperties`` ``c`` and the harness's ``h`` (either
    :data:`ABSENT`) admit the same values exchanged. ``true`` is no assertion, so it is the
    member's absence; and the contract's ``false`` against the harness's absence is Kontra's
    policy, not a difference: the contract closes every model (section 11.3), and a model
    pydantic writes without one (its default ``extra="ignore"``) drops other keys on input
    and never emits one on output. ``false`` against ``true`` (``extra="allow"``: the model
    keeps other keys and serializes them) is not alike, and a schema is compared as any."""
    if c is False and h is ABSENT:
        return True
    return (c is True or c is ABSENT) and (h is True or h is ABSENT)


def _same(c: Any, h: Any) -> bool:
    """Equal as RFC 8785 canonical JSON; a value with no canonical form (an integer past
    2^53, a NaN the harness wrote) equals nothing."""
    try:
        return canonical_json(c) == canonical_json(h)
    except ContractError:
        return False


def _canonical(node: Mapping[str, Any], defs: Mapping[str, Any]) -> dict[str, Any]:
    """One schema node in the canonical form (its own members; sub-schemas are walked)."""
    out = {k: v for k, v in node.items() if k not in _ANNOTATIONS and k != _DISCRIMINATOR}
    default = out.get("default", ABSENT)
    if default is None or isinstance(default, dict | list):
        del out["default"]
    required = out.get("required")
    if isinstance(required, list):
        needed = cast(list[Any], required)
        props = out.get("properties")
        if isinstance(props, dict):
            # A required member is never absent, so its default never applies.
            out["properties"] = {
                name: _without_default(sub) if name in needed else sub
                for name, sub in cast(Mapping[str, Any], props).items()
            }
        if needed:
            out["required"] = sorted(needed, key=str)
        else:
            del out["required"]
    branches = out.get("anyOf")
    if isinstance(branches, list) and len(branches) == 2 and "oneOf" not in out:
        pair = cast(list[Any], branches)
        nulls = [b for b in pair if b == _NULL]
        if len(nulls) == 1 and _rejects_null(next(b for b in pair if b != _NULL), defs):
            out["oneOf"] = out.pop("anyOf")
    return out


def _without_default(node: Any) -> Any:
    if isinstance(node, dict) and "default" in node:
        return {k: v for k, v in cast(Mapping[str, Any], node).items() if k != "default"}
    return node


class _Walk:
    """Both sides of one operation (input or output), walked together; references are
    followed, and each pair of definitions is compared once."""

    def __init__(self, side: Side, cdefs: Mapping[str, Any], hdefs: Mapping[str, Any]) -> None:
        self.side: Side = side
        self.cdefs = cdefs
        self.hdefs = hdefs
        self.found: list[Difference] = []
        self._seen: set[tuple[str, str]] = set()
        self._todo: list[tuple[Mapping[str, Any], Mapping[str, Any], str]] = []

    def run(self, c: Mapping[str, Any], h: Mapping[str, Any], path: str) -> None:
        self._todo.append((c, h, path))
        while self._todo:
            self._node(*self._todo.pop())

    def _node(self, c: Mapping[str, Any], h: Mapping[str, Any], path: str) -> None:
        cname, hname = _ref_name(c, self.cdefs), _ref_name(h, self.hdefs)
        if cname is not None and hname is not None:
            # Two definitions: the use site's own members here, the bodies once per pair.
            self._members(
                _canonical({k: v for k, v in c.items() if k != "$ref"}, self.cdefs),
                _canonical({k: v for k, v in h.items() if k != "$ref"}, self.hdefs),
                path,
            )
            if (cname, hname) not in self._seen:
                self._seen.add((cname, hname))
                cbody, hbody = self.cdefs[cname], self.hdefs[hname]
                where = join_path("$defs", cname)
                if isinstance(cbody, dict) and isinstance(hbody, dict):
                    self._todo.append(
                        (cast(Mapping[str, Any], cbody), cast(Mapping[str, Any], hbody), where)
                    )
                else:
                    self._leaf(where, cbody, hbody)
            return
        # One side wrote inline what the other names (an enum, a constrained scalar, with
        # the use site's own members): compare the definition in place.
        cnode = _resolve(c, self.cdefs) if cname is not None else c
        hnode = _resolve(h, self.hdefs) if hname is not None else h
        self._members(_canonical(cnode, self.cdefs), _canonical(hnode, self.hdefs), path)

    def _members(self, c: Mapping[str, Any], h: Mapping[str, Any], path: str) -> None:
        for key in sorted(set(c) | set(h)):
            cv, hv = c.get(key, ABSENT), h.get(key, ABSENT)
            where = join_path(path, key)
            if key == _EXTRA and _closed_alike(cv, hv):
                continue
            if key == _EXTRA and cv is False and hv is True:
                self.found.append(Difference(self.side, where, cv, hv, open=True))
            elif cv is ABSENT or hv is ABSENT:
                self._differ(where, self._shown(cv, key), self._shown(hv, key, harness=True))
            elif key in _ONE and isinstance(cv, dict) and isinstance(hv, dict):
                self._todo.append((cast(Mapping[str, Any], cv), cast(Mapping[str, Any], hv), where))
            elif key in _MAP and isinstance(cv, dict) and isinstance(hv, dict):
                cmap, hmap = cast(Mapping[str, Any], cv), cast(Mapping[str, Any], hv)
                for name in sorted(set(cmap) | set(hmap)):
                    sub = join_path(where, name)
                    cs, hs = cmap.get(name, ABSENT), hmap.get(name, ABSENT)
                    if isinstance(cs, dict) and isinstance(hs, dict):
                        self._todo.append(
                            (cast(Mapping[str, Any], cs), cast(Mapping[str, Any], hs), sub)
                        )
                    else:
                        self._differ(sub, self._shown(cs), self._shown(hs, harness=True))
            elif (
                key in _LIST
                and isinstance(cv, list)
                and isinstance(hv, list)
                and len(cast(list[Any], cv)) == len(cast(list[Any], hv))
                and all(isinstance(b, dict) for b in [*cast(list[Any], cv), *cast(list[Any], hv)])
            ):
                for i, (cb, hb) in enumerate(
                    zip(cast(list[Any], cv), cast(list[Any], hv), strict=True)
                ):
                    self._todo.append((cb, hb, join_path(where, i)))
            else:
                self._leaf(where, cv, hv)

    def _shown(self, value: Any, key: str = "", *, harness: bool = False) -> Any:
        """A whole member one side lacks, as the canonical form shows it (a schema, or each
        schema of an ``allOf``, ``anyOf``, ``oneOf`` or ``prefixItems``)."""
        if isinstance(value, dict):
            return _canonical(cast(Mapping[str, Any], value), self.hdefs if harness else self.cdefs)
        if key in _LIST and isinstance(value, list):
            return [self._shown(v, harness=harness) for v in cast(list[Any], value)]
        return value

    def _leaf(self, path: str, c: Any, h: Any) -> None:
        if not _same(c, h):
            self._differ(path, c, h)

    def _differ(self, path: str, c: Any, h: Any) -> None:
        self.found.append(Difference(self.side, path, c, h))
