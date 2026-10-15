"""Deterministic YAML for contract documents and a strict loader for YAML people write.

Rendering: the same document always renders to the same bytes, whatever order its keys
arrived in (a document read back from the catalog's ``jsonb`` comes back reordered), so a
golden file can be compared byte for byte. Keys are sorted, except that a few structural
keys come first so the file reads top-down (``nexusrpc``, ``services`` ... ``x-kontra``).
No anchors or aliases are ever emitted.

Loading follows the YAML 1.2 core schema instead of PyYAML's YAML 1.1 defaults, which
would turn a property named ``on`` into ``True``, ``2026-10-11`` into a date and ``012``
into 10. Aliases (the billion-laughs vector), merge keys, duplicate keys and non-string
keys are refused: none has a JSON meaning.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from pathlib import Path
from typing import Any, cast

import yaml

from kontra.contract.canonical import check_json_value
from kontra.contract.errors import ROOT, ContractError

__all__ = ["dump_ordered", "from_yaml", "load_yaml_file", "to_yaml"]

#: Keys that come first, in this order, wherever they appear. Everything else is sorted.
_FIRST = (
    "nexusrpc",
    "$schema",
    "services",
    "$defs",
    "x-kontra",
    "name",
    "fqn",
    "version",
    "description",
    "pricing_hint",
    "tags",
    "max_duration",
    "review_window",
    "max_concurrent_jobs",
    "max_output_bytes",
    "artifact_retention",
    "uses_hosted_model",
    "contract_hash",
    "operations",
    "quote",
    "execute",
    "input",
    "output",
    "type",
)
_RANK = {k: i for i, k in enumerate(_FIRST)}


def _rank(key: str) -> tuple[int, str]:
    return (_RANK.get(key, len(_FIRST)), key)


def _order(value: Any) -> Any:
    if isinstance(value, Mapping):
        mapping = cast(Mapping[str, Any], value)
        return {k: _order(mapping[k]) for k in sorted(mapping, key=_rank)}
    if isinstance(value, list | tuple):
        return [_order(v) for v in value]
    return value


#: Characters YAML treats as line breaks besides \n and \r (NEL, LS, PS) and the BOM.
_LINE_BREAKS = frozenset("\r\x85\u2028\u2029\ufeff")


def _resolves_to_non_string(data: str) -> bool:
    """Whether a YAML 1.2 core reader would take ``data``, written plain, as a bool,
    null or number."""
    return any(r.match(data) for r in (_BOOL, _NULL, _INT, _FLOAT))


class _Dumper(yaml.SafeDumper):
    def ignore_aliases(self, data: Any) -> bool:
        return True


def _represent_str(dumper: yaml.SafeDumper, data: str) -> yaml.ScalarNode:
    if any(c in _LINE_BREAKS for c in data):
        # PyYAML writes these unescaped, and a reader folds them into spaces; only the
        # double-quoted style escapes them.
        return dumper.represent_scalar("tag:yaml.org,2002:str", data, style='"')
    if "\n" in data:
        return dumper.represent_scalar("tag:yaml.org,2002:str", data, style="|")
    if data[:1].isdigit() or _resolves_to_non_string(data):
        # PyYAML's dumper knows YAML 1.1, so it leaves "-1e5" or "+.5" plain, and a YAML
        # 1.2 reader (ours, nexgen's) reads a number back: the hash would change. Quoting
        # anything that starts with a digit also settles "1.0.0" for every parser.
        return dumper.represent_scalar("tag:yaml.org,2002:str", data, style='"')
    return dumper.represent_scalar("tag:yaml.org,2002:str", data)


_Dumper.add_representer(str, _represent_str)


def dump_ordered(value: Any) -> str:
    """Render any JSON-compatible value in the deterministic style."""
    out: str = yaml.dump(
        _order(value),
        Dumper=_Dumper,
        sort_keys=False,
        default_flow_style=False,
        allow_unicode=True,
        width=1 << 30,
        indent=2,
    )
    return out


def to_yaml(doc: Mapping[str, Any]) -> str:
    """The document as deterministic YAML. ``from_yaml(to_yaml(d)) == d`` for any JSON
    document ``d``, so the contract hash survives the round trip."""
    check_json_value(doc)
    return dump_ordered(doc)


# --------------------------------------------------------------------------- loading


class _Loader(yaml.SafeLoader):
    """SafeLoader with YAML 1.2 core-schema scalars and no aliases or duplicate keys."""

    def compose_node(self, parent: Any, index: Any) -> Any:
        if self.check_event(yaml.AliasEvent):
            event = self.peek_event()
            raise yaml.constructor.ConstructorError(
                None, None, "aliases (*name) are not allowed", event.start_mark
            )
        return super().compose_node(parent, index)

    def construct_mapping(self, node: Any, deep: bool = False) -> dict[Any, Any]:
        if isinstance(node, yaml.MappingNode):
            seen: set[Any] = set()
            for key_node, _ in node.value:
                if key_node.tag == "tag:yaml.org,2002:merge":
                    raise yaml.constructor.ConstructorError(
                        None, None, "merge keys (<<) are not allowed", key_node.start_mark
                    )
                key = self.construct_object(key_node, deep=True)
                if not isinstance(key, str):
                    raise yaml.constructor.ConstructorError(
                        None, None, f"keys must be strings, got {key!r}", key_node.start_mark
                    )
                if key in seen:
                    raise yaml.constructor.ConstructorError(
                        None, None, f"duplicate key {key!r}", key_node.start_mark
                    )
                seen.add(key)
        return super().construct_mapping(node, deep=deep)


# YAML 1.2 core schema (https://yaml.org/spec/1.2.2/#1032-tag-resolution), decimal only.
_BOOL = re.compile(r"^(?:true|True|TRUE|false|False|FALSE)\Z")
_NULL = re.compile(r"^(?:~|null|Null|NULL|)\Z")
_INT = re.compile(r"^[-+]?[0-9]+\Z", re.ASCII)
_FLOAT = re.compile(r"^[-+]?(?:\.[0-9]+|[0-9]+(?:\.[0-9]*)?)(?:[eE][-+]?[0-9]+)?\Z", re.ASCII)
_Loader.yaml_implicit_resolvers = {}
_Loader.add_implicit_resolver("tag:yaml.org,2002:bool", _BOOL, list("tTfF"))
_Loader.add_implicit_resolver("tag:yaml.org,2002:null", _NULL, ["~", "n", "N", ""])
_Loader.add_implicit_resolver("tag:yaml.org,2002:int", _INT, list("-+0123456789"))
_Loader.add_implicit_resolver("tag:yaml.org,2002:float", _FLOAT, list("-+.0123456789"))


def _construct_int(loader: yaml.SafeLoader, node: yaml.Node) -> int:
    # PyYAML's constructor reads a leading 0 as octal (YAML 1.1); 1.2 core is decimal.
    return int(loader.construct_scalar(node), 10)  # type: ignore[arg-type]


def _construct_float(loader: yaml.SafeLoader, node: yaml.Node) -> float:
    return float(loader.construct_scalar(node))  # type: ignore[arg-type]


_Loader.add_constructor("tag:yaml.org,2002:int", _construct_int)
_Loader.add_constructor("tag:yaml.org,2002:float", _construct_float)


def _load(text: str, source: str) -> Any:
    try:
        return yaml.load(text, Loader=_Loader)  # noqa: S506 -- _Loader is a SafeLoader
    except yaml.YAMLError as exc:
        mark = getattr(exc, "problem_mark", None) or getattr(exc, "context_mark", None)
        where = f" (line {mark.line + 1}, column {mark.column + 1})" if mark is not None else ""
        problem = getattr(exc, "problem", None) or str(exc)
        raise ContractError(ROOT, f"{source} is not valid YAML{where}: {problem}") from exc
    except RecursionError:
        # PyYAML composes recursively; a few thousand nested brackets exhaust the stack.
        raise ContractError(ROOT, f"{source} nests too deeply to read") from None
    except (ValueError, OverflowError) as exc:
        # An explicit tag our constructors cannot read (!!float .inf, !!int 0x10).
        raise ContractError(ROOT, f"{source} has a value YAML 1.2 cannot read: {exc}") from None


def from_yaml(text: str, *, source: str = "document") -> dict[str, Any]:
    """Parse a contract document. It must be a mapping of JSON values."""
    data = _load(text, source)
    if not isinstance(data, dict):
        raise ContractError(ROOT, f"{source} must be a mapping, got {type(data).__name__}")
    check_json_value(data)
    return data  # type: ignore[return-value]


def load_yaml_file(path: str | Path) -> Any:
    """Any YAML file with the strict loader (``offering.yaml``)."""
    p = Path(path)
    try:
        text = p.read_text(encoding="utf-8")
    except FileNotFoundError:
        raise ContractError(ROOT, f"{p} does not exist") from None
    data = _load(text, p.name)
    if data is not None:
        check_json_value(data)
    return data
