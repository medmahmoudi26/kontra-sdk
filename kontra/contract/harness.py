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
states are the agent's envelope, never the operation. Both sides are put in one canonical form
first, which removes every difference that does not change which JSON values are valid, each
by a rule I33 lists and ``tests/contract/test_harness_rules.py`` tests:

* references are followed on both sides, and a definition is matched by where it is used, not
  by its name (``Report`` against the harness's ``Report-Output``, ``KontraArtifact`` against
  ``Artifact``, an enum Kontra writes inline against the harness's ``$ref``); a recursive
  model is compared once;
* annotations are dropped: ``title``, ``description``, ``examples``, ``$comment``,
  ``deprecated``, ``readOnly``, ``writeOnly``; so is the OpenAPI ``discriminator`` (the
  branches' ``const`` tags select the branch);
* ``required`` is a set (sorted; empty is absent), ``additionalProperties: true`` is its
  absence, and values are compared as RFC 8785 canonical JSON (key order, ``1`` and ``1.0``);
* ``anyOf: [T, {type: null}]`` is ``oneOf: [T, {type: null}]`` when ``T`` cannot be null;
* a ``default`` of ``null``, a list or an object is no default (decisions I2: null is the
  absence of one, and nexgen materialises only scalar defaults). A scalar default is compared:
  a buyer's generated code fills it in.

What remains is reported, each :class:`Difference` with a :data:`Kind`: ``open`` (the contract
closes every model, section 11.3, and the harness leaves this one open), ``defaults`` (on the
output, the harness's serialization-mode schema requires fields that have a default; the reply
always carries them), ``serialization`` (any other output difference: the harness describes the
model as serialized -- computed and excluded fields, serialization aliases, serializers -- and
the contract as validated) and ``drift`` (any other input difference: unexplained).

The harness's schema comes from its own CLI, ``temporal-agent-harness schema module:Class``,
run by :func:`harness_schema` as a subprocess of this interpreter: the same document
harness-codegen reads, and Kontra never imports the harness (decisions I29). This module holds
the CLI's name and the document's shape as constants, checked against the harness by
``tests/contract/test_harness.py``.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from importlib.metadata import entry_points
from pathlib import Path
from typing import Any, Final, Literal, cast

from kontra.contract.canonical import canonical_json
from kontra.contract.errors import ContractError, join_path
from kontra.contract.validate import validate_document

__all__ = [
    "ABSENT",
    "HARNESS_SCRIPT",
    "NOTES",
    "SCHEMA_COMMAND",
    "SCHEMA_DEFS",
    "SCHEMA_HANDLERS",
    "SCHEMA_INPUT",
    "SCHEMA_OUTPUT",
    "SCHEMA_OUTPUT_OPTION",
    "SCHEMA_TIMEOUT",
    "Difference",
    "HarnessSchemaError",
    "Kind",
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
            proc = subprocess.run(  # noqa: S603 -- argv, no shell
                argv,
                cwd=cwd,
                env=env,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                timeout=timeout,
                check=False,
            )
        except subprocess.TimeoutExpired:
            raise HarnessSchemaError(
                f"`{HARNESS_SCRIPT} {SCHEMA_COMMAND} {target}` did not answer within {timeout:g}s"
            ) from None
        except OSError as exc:
            raise HarnessSchemaError(f"cannot run {HARNESS_SCRIPT}: {exc}") from None
        if proc.returncode != 0:
            said = _last_line(proc.stderr) or f"exit status {proc.returncode}"
            raise HarnessSchemaError(f"`{HARNESS_SCRIPT} {SCHEMA_COMMAND} {target}`: {said}")
        try:
            text = out.read_text(encoding="utf-8")
        except OSError as exc:
            raise HarnessSchemaError(f"{HARNESS_SCRIPT} wrote no schema: {exc}") from None
    try:
        data: Any = json.loads(text)
    except (ValueError, RecursionError) as exc:
        raise HarnessSchemaError(f"{HARNESS_SCRIPT}'s schema is not JSON: {exc}") from None
    if not isinstance(data, dict):
        raise HarnessSchemaError(f"{HARNESS_SCRIPT}'s schema is not a JSON object")
    return cast(dict[str, Any], data)


def _last_line(stderr: bytes) -> str:
    lines = [ln.strip() for ln in stderr.decode("utf-8", "replace").splitlines() if ln.strip()]
    return lines[-1][:_ERROR_CHARS] if lines else ""


# --------------------------------------------------------------------------- the comparison

#: What a difference means; :data:`NOTES` says it in a sentence.
Kind = Literal["open", "defaults", "serialization", "drift"]
Side = Literal["input", "output"]

NOTES: Final[Mapping[Kind, str]] = {
    "open": (
        "the contract closes every model (no keys but its fields) and the harness's schema "
        'leaves this one open, as pydantic\'s default extra="ignore" does; '
        'model_config = ConfigDict(extra="forbid") on the model makes both say so'
    ),
    "defaults": (
        "the harness's output schema (serialization mode) requires the fields that have a "
        "default, which the reply always carries; the contract lets them be absent"
    ),
    "serialization": (
        "the harness describes the output as serialized (computed and excluded fields, "
        "serialization aliases, serializers), the contract as validated: the job's result is "
        "the serialization"
    ),
    "drift": "the two emitters describe the same model differently",
}


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
    ``contract``/``harness`` are the values there, or :data:`ABSENT`."""

    side: Side
    path: str
    contract: Any
    harness: Any
    kind: Kind

    def __str__(self) -> str:
        return (
            f"{self.side:<6} {self.path}: contract {_show(self.contract)}, "
            f"harness {_show(self.harness)}"
        )


def describe(differences: Iterable[Difference]) -> list[str]:
    """Lines for a person: one per difference, then what each kind present means."""
    found = list(differences)
    lines = [str(d) for d in found]
    for kind in NOTES:
        if any(d.kind == kind for d in found):
            lines.append(f"({kind}) {NOTES[kind]}")
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
_NULL: Mapping[str, Any] = {"type": "null"}


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


def _resolve(node: Mapping[str, Any], defs: Mapping[str, Any]) -> Mapping[str, Any]:
    """A ``$ref`` node as its definition with the use site's members over it."""
    name = _ref_name(node, defs)
    if name is None:
        return node
    body = defs[name]
    merged = dict(cast(Mapping[str, Any], body)) if isinstance(body, dict) else {}
    merged.update({k: v for k, v in node.items() if k != "$ref"})
    return merged


def _rejects_null(node: Any, defs: Mapping[str, Any], depth: int = 0) -> bool:
    """Whether no schema-valid value of ``node`` is ``null`` (conservatively: unsure is no)."""
    if not isinstance(node, dict) or depth > 32:
        return False
    schema = cast(Mapping[str, Any], node)
    if _ref_name(schema, defs) is not None:
        return _rejects_null(_resolve(schema, defs), defs, depth + 1)
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
    if out.get("additionalProperties") is True:
        del out["additionalProperties"]
    required = out.get("required")
    if isinstance(required, list):
        if required:
            out["required"] = sorted(cast(list[Any], required), key=str)
        else:
            del out["required"]
    branches = out.get("anyOf")
    if isinstance(branches, list) and len(branches) == 2 and "oneOf" not in out:
        pair = cast(list[Any], branches)
        nulls = [b for b in pair if b == _NULL]
        if len(nulls) == 1 and _rejects_null(next(b for b in pair if b != _NULL), defs):
            out["oneOf"] = out.pop("anyOf")
    return out


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
        # One side wrote inline what the other names (an enum): compare the definition in place.
        self._members(
            _canonical(_resolve(c, self.cdefs), self.cdefs),
            _canonical(_resolve(h, self.hdefs), self.hdefs),
            path,
        )

    def _members(self, c: Mapping[str, Any], h: Mapping[str, Any], path: str) -> None:
        for key in sorted(set(c) | set(h)):
            cv, hv = c.get(key, ABSENT), h.get(key, ABSENT)
            where = join_path(path, key)
            if cv is ABSENT or hv is ABSENT:
                self._differ(where, key, self._shown(cv), self._shown(hv, harness=True), c)
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
                        self._differ(sub, name, self._shown(cs), self._shown(hs, harness=True), c)
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
                self._leaf(where, cv, hv, key=key, parent=c)

    def _shown(self, value: Any, *, harness: bool = False) -> Any:
        """A whole member one side lacks, as the canonical form shows it."""
        if isinstance(value, dict):
            return _canonical(cast(Mapping[str, Any], value), self.hdefs if harness else self.cdefs)
        return value

    def _leaf(
        self,
        path: str,
        c: Any,
        h: Any,
        *,
        key: str = "",
        parent: Mapping[str, Any] | None = None,
    ) -> None:
        if not _same(c, h):
            self._differ(path, key, c, h, parent or {})

    def _differ(self, path: str, key: str, c: Any, h: Any, parent: Mapping[str, Any]) -> None:
        self.found.append(Difference(self.side, path, c, h, self._kind(key, c, h, parent)))

    def _kind(self, key: str, c: Any, h: Any, parent: Mapping[str, Any]) -> Kind:
        if key == "additionalProperties" and c is False and h is ABSENT:
            return "open"
        if self.side == "input":
            return "drift"
        if key == "required":
            have = set(cast(list[str], c)) if isinstance(c, list) else set[str]()
            want = set(cast(list[str], h)) if isinstance(h, list) else set[str]()
            props = parent.get("properties")
            declared = (
                set(cast(Mapping[str, Any], props)) if isinstance(props, dict) else set[str]()
            )
            if have < want and want - have <= declared:
                return "defaults"
        return "serialization"
