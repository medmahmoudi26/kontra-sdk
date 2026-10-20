"""Seam ③: the published contract against temporal-agent-harness's schema for the entry
handler (decisions I33) -- the same pattern as the golden contracts, with the harness's own
emitter as the other side.

The harness's schema is what its ``schema`` command prints (the input to the seller's
harness-codegen), read here as ``kontra publish`` reads it: through
:func:`kontra.contract.harness.harness_schema`, a subprocess, so Kontra never imports the
harness. For the test agents (``tests/serve/harness_agents.py``) and the model zoo
(``harness_zoo.py``) the verdict is asserted exactly: equal once both are in the canonical
form, or exactly the documented semantic differences. Then Kontra's copy of the harness's
schema shape against the harness, and the warning ``kontra contract`` prints.

Skipped only when the harness is not installed; kontra-sdk's dev group installs it, so CI
runs all of it."""

from __future__ import annotations

import json
import textwrap
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner, Result

from kontra.cli import cli as kontra_cli
from kontra.contract import OfferingSpec, derive_document, from_yaml, validate_document
from kontra.contract import harness as h
from kontra.contract.harness import Difference
from kontra.serve import offering
from kontra.serve.turn import AGENT_HANDLERS

pytest.importorskip("temporal_agent_harness", reason="the dev group's temporal-agent-harness")

from tests.serve import harness_agents as agents

from . import harness_zoo as zoo

REPO = Path(__file__).resolve().parents[2]
EXAMPLES = REPO / "examples"

#: Every agent class the tests compare: one subprocess each, all at once.
AGENTS: tuple[type, ...] = (
    agents.ReconAgent,
    agents.NeedsData,
    agents.MayHaveData,
    agents.SlowAgent,
    agents.HopAgent,
    zoo.Zoo,
)


@pytest.fixture(scope="module")
def schemas() -> dict[type, dict[str, Any]]:
    """Each agent's schema, from the harness's CLI."""
    with ThreadPoolExecutor(max_workers=len(AGENTS)) as pool:
        docs = pool.map(lambda cls: h.harness_schema(h.agent_target(cls), cwd=REPO), AGENTS)
        return dict(zip(AGENTS, docs, strict=True))


def _found(differences: tuple[Difference, ...]) -> set[tuple[str, str, str]]:
    return {(d.side, d.path, d.kind) for d in differences}


def _open(*models: str) -> set[tuple[str, str, str]]:
    """``open`` on both sides for each model: the contract closes it, and the harness's
    schema says ``additionalProperties: true`` (``extra="allow"``)."""
    return {
        (side, f"$defs.{m}.additionalProperties", "open")
        for m in models
        for side in ("input", "output")
    }


# --------------------------------------------------------------------------- the harness's shape


def test_kontras_copy_of_the_schema_shape_is_the_harnesses(
    schemas: dict[type, dict[str, Any]],
) -> None:
    """Golden: the console script, its ``schema`` command and ``-o``, and the document's
    members Kontra reads (kontra/contract/harness.py) are the harness's own. A harness
    release that changes one fails here, not at a seller's ``kontra publish``."""
    from importlib.metadata import entry_points

    from temporal_agent_harness.harness.agent_schema import agent_schema
    from temporal_agent_harness.web import cli as harness_cli

    [script] = entry_points(group="console_scripts", name=h.HARNESS_SCRIPT)
    assert script.value == f"{harness_cli.__name__}:main"
    source = Path(harness_cli.__file__).read_text(encoding="utf-8")
    assert f'"{h.SCHEMA_COMMAND}",' in source
    assert f'"{h.SCHEMA_OUTPUT_OPTION}", "--output"' in source
    doc = agent_schema(agents.ReconAgent)
    assert {h.SCHEMA_HANDLERS, h.SCHEMA_DEFS} <= set(doc)
    assert {h.SCHEMA_INPUT, h.SCHEMA_OUTPUT} <= set(doc[h.SCHEMA_HANDLERS]["sweep"])
    # What the CLI prints is agent_schema's document, value for value.
    assert schemas[agents.ReconAgent] == json.loads(json.dumps(doc))


def test_the_harness_says_why_it_gives_no_schema() -> None:
    with pytest.raises(h.HarnessSchemaError, match=r"no module named 'tests\.no_such_module'"):
        h.harness_schema("tests.no_such_module:Agent", cwd=REPO)
    with pytest.raises(h.HarnessSchemaError, match=r"is not a @workflow\.defn agent class"):
        h.harness_schema("tests.serve.harness_agents:Sweep", cwd=REPO)


# --------------------------------------------------------------------------- the test agents


@pytest.mark.parametrize(
    ("agent", "handler"),
    [
        (agents.ReconAgent, "sweep"),
        (agents.NeedsData, "sweep"),
        (agents.MayHaveData, "sweep"),
        (agents.SlowAgent, "sweep"),
        (agents.HopAgent, "hop"),
    ],
)
def test_the_test_agents(
    enrich_spec: OfferingSpec, schemas: dict[type, dict[str, Any]], agent: type, handler: str
) -> None:
    """No difference at all. Their models keep pydantic's default ``extra="ignore"``, which
    the contract closes by policy (no ``open``); ``Sweep.fail``'s default is on the input,
    which both describe in validation mode; ``HopAgent``'s aliases are the keys on both
    sides."""
    declared = getattr(agent, AGENT_HANDLERS)[handler]
    doc = derive_document(enrich_spec, declared.input_type, declared.output_type)
    assert h.compare_handler(doc, schemas[agent], handler) == ()


def test_a_bound_handlers_document_is_what_is_compared(
    tmp_path: Path, schemas: dict[type, dict[str, Any]]
) -> None:
    """``offering(entry=Agent.handler)`` derives the document compared, and nothing of the
    agent's envelope is compared: ``MayHaveData``'s init data is in the harness's schema,
    and in no difference."""
    yaml = _offering_yaml(tmp_path, "acme/recon")
    bound = offering(yaml, entry=agents.MayHaveData.sweep, quote=agents.quote)
    schema = schemas[agents.MayHaveData]
    assert schema["init_data"] == {"data": {"$ref": "#/$defs/Data"}, "required": False}
    assert h.compare_handler(bound.document(), schema, "sweep") == ()


# --------------------------------------------------------------------------- the model zoo

#: Per handler of the zoo: every difference. A model with pydantic's default
#: ``extra="ignore"`` is closed by the contract alone, which is Kontra's policy and no
#: difference; ``Allow`` (``extra="allow"``), the harness's ``additionalProperties: true``,
#: is ``open`` on both sides, alone or nested. ``serialization`` is Kontra deriving the
#: output in validation mode while the job's result is its serialization (decisions I33
#: reports it for the owner).
ZOO: dict[str, set[tuple[str, str, str]]] = {
    "defaults": {("output", "$defs.Defaults.required", "defaults")},
    "optionals": {("output", "$defs.Optionals.required", "defaults")},
    "aliases": set(),
    "split_alias": {
        ("output", "$defs.SplitAlias.properties.in_a", "serialization"),
        ("output", "$defs.SplitAlias.properties.out_a", "serialization"),
        ("output", "$defs.SplitAlias.required", "serialization"),
    },
    "camel": {("output", "$defs.Camel.required", "defaults")},
    "choices": set(),
    "nested": {
        ("output", "$defs.Leaf.required", "defaults"),
        ("output", "$defs.Nested.required", "defaults"),
    },
    "enums": {("output", "$defs.Enums.required", "defaults")},
    "literals": {("output", "$defs.Literals.required", "defaults")},
    "constrained": set(),
    "collections": {("output", "$defs.Collections.required", "defaults")},
    "times": set(),
    # KontraArtifact is closed on both sides.
    "files": {("output", "$defs.Files.required", "defaults")},
    "tagged": {("output", "$defs.Dog.required", "defaults")},
    "titled": set(),
    "tree": {("output", "$defs.Tree.required", "defaults")},
    "forbid": set(),
    "allow": _open("Allow"),
    "outer": _open("Allow"),
    "computed": {
        ("output", "$defs.Computed.properties.area", "serialization"),
        ("output", "$defs.Computed.required", "serialization"),
    },
    "excluded": {
        ("output", "$defs.Excluded.properties.secret", "serialization"),
        ("output", "$defs.Excluded.required", "serialization"),
    },
    "serialized": {
        ("output", "$defs.Serialized.properties.at.format", "serialization"),
        ("output", "$defs.Serialized.properties.at.type", "serialization"),
    },
    # The harness writes {$ref: Code, maxLength: 10}, both asserted (at most 5); the contract
    # keeps the use site's 10 (decisions I33, for the owner). Drift on both sides.
    "overlap": {
        (side, f"$defs.Overlap.properties.{field}.{key}", "drift")
        for side in ("input", "output")
        for field in ("code", "maybe.oneOf[0]")
        for key in ("allOf", "maxLength", "type")
    },
}


def test_the_zoo_covers_every_handler(schemas: dict[type, dict[str, Any]]) -> None:
    assert set(ZOO) == set(zoo.MODELS) == set(schemas[zoo.Zoo]["handlers"])


@pytest.mark.parametrize("handler", sorted(ZOO))
def test_the_model_zoo(
    enrich_spec: OfferingSpec, schemas: dict[type, dict[str, Any]], handler: str
) -> None:
    model = zoo.MODELS[handler]
    doc = derive_document(enrich_spec, model, model)
    assert _found(h.compare_handler(doc, schemas[zoo.Zoo], handler)) == ZOO[handler]


def test_the_zoo_s_differences_say_what_differs(
    enrich_spec: OfferingSpec, schemas: dict[type, dict[str, Any]]
) -> None:
    """The values behind a few verdicts: defaults the output requires, a serializer's type,
    a computed field, the closed model that agrees, the harness's ``true`` against the
    contract's ``false``, and a constraint on both a type and its use."""

    def compare(handler: str) -> dict[tuple[str, str], tuple[Any, Any]]:
        model = zoo.MODELS[handler]
        found = h.compare_handler(
            derive_document(enrich_spec, model, model), schemas[zoo.Zoo], handler
        )
        return {(d.side, d.path): (d.contract, d.harness) for d in found}

    assert compare("defaults")[("output", "$defs.Defaults.required")] == (
        ["req"],
        ["b", "f", "n", "req", "s"],
    )
    assert compare("serialized")[("output", "$defs.Serialized.properties.at.type")] == (
        "string",
        "integer",
    )
    assert compare("computed")[("output", "$defs.Computed.properties.area")] == (
        h.ABSENT,
        {"type": "integer"},
    )
    assert compare("forbid") == {}
    assert compare("outer") == {
        (side, "$defs.Allow.additionalProperties"): (False, True) for side in ("input", "output")
    }
    # The contract accepts a 10-character code the model refuses; the harness says both.
    assert compare("overlap")[("input", "$defs.Overlap.properties.code.allOf")] == (
        h.ABSENT,
        [{"maxLength": 5, "type": "string"}, {"maxLength": 10}],
    )
    assert compare("overlap")[("input", "$defs.Overlap.properties.code.maxLength")] == (
        10,
        h.ABSENT,
    )


# --------------------------------------------------------------------------- the warning


def _offering_yaml(directory: Path, name: str) -> Path:
    spec = {
        "name": name,
        "version": "1.0.0",
        "description": "Sweeps a host",
        "max_duration": "10m",
        "review_window": "1h",
        "max_output_bytes": "1MB",
        "artifact_retention": "1d",
    }
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / "offering.yaml"
    path.write_text("".join(f"{k}: {json.dumps(v)}\n" for k, v in spec.items()))
    return path


def _project(
    root: Path, entry: str, quote: str, module: str = "tests.serve.harness_agents"
) -> Path:
    _offering_yaml(root, "acme/recon")
    (root / "worker.py").write_text(
        textwrap.dedent(f"""
        from kontra import serve
        from {module} import {entry.split(".")[0]}, {quote}

        serve({entry}, quote={quote})
        """).lstrip()
    )
    return root


def _contract(root: Path, home: Path) -> Result:
    env = {"KONTRA_HOME": str(home), "KONTRA_GATE": None, "KONTRA_API_KEY": None}
    return CliRunner().invoke(kontra_cli, ["contract", "--dir", str(root)], env=env)


def test_kontra_contract_warns_where_they_differ(tmp_path: Path) -> None:
    """On stderr: one warning naming the offering and the handler, a line per difference and
    what each kind means. The document on stdout is unchanged, and the command succeeds."""
    root = _project(tmp_path / "p", "Zoo.allow", "allow_quote", module="tests.contract.harness_zoo")
    result = _contract(root, tmp_path / "home")
    assert result.exit_code == 0, result.output
    info = validate_document(from_yaml(result.stdout))
    assert info.offering.key == "acme/recon@1.0.0"
    lines = result.stderr.splitlines()
    assert lines[0].startswith("warning: acme/recon@1.0.0 (Zoo.allow): the contract ")
    assert "in 2 places" in lines[0]
    assert lines[1:] == [
        "  input  $defs.Allow.additionalProperties: contract false, harness true",
        "  output $defs.Allow.additionalProperties: contract false, harness true",
        f"  (open) {h.NOTES['open']}",
    ]


@pytest.mark.parametrize(
    ("entry", "quote", "module"),
    [
        # pydantic's default models (extra="ignore"): the contract closes them by policy.
        ("ReconAgent.sweep", "quote", "tests.serve.harness_agents"),
        ("Zoo.forbid", "forbid_quote", "tests.contract.harness_zoo"),
    ],
)
def test_kontra_contract_says_nothing_when_they_agree(
    tmp_path: Path, entry: str, quote: str, module: str
) -> None:
    result = _contract(_project(tmp_path / "p", entry, quote, module=module), tmp_path / "home")
    assert result.exit_code == 0, result.output
    assert result.stderr == ""


def test_no_harness_and_a_workflow_entry_say_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Without the harness installed nothing is run or said; a workflow entry is never
    compared."""

    def run(*_: object, **__: object) -> dict[str, Any]:
        raise AssertionError("the harness was asked")

    monkeypatch.setattr(h, "harness_schema", run)
    enrich = _contract(EXAMPLES / "enrich", tmp_path / "home")
    assert (enrich.exit_code, enrich.stderr) == (0, "")
    monkeypatch.setattr(h, "harness_installed", lambda: False)
    handler = _contract(_project(tmp_path / "p", "ReconAgent.sweep", "quote"), tmp_path / "home")
    assert (handler.exit_code, handler.stderr) == (0, "")


def test_a_harness_that_fails_is_one_warning(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def run(*_: object, **__: object) -> dict[str, Any]:
        raise h.HarnessSchemaError("`temporal-agent-harness schema x:Y`: it broke\x1b[2J")

    monkeypatch.setattr(h, "harness_schema", run)
    result = _contract(_project(tmp_path / "p", "ReconAgent.sweep", "quote"), tmp_path / "home")
    assert result.exit_code == 0, result.output
    assert result.stderr.splitlines() == [
        "warning: acme/recon@1.0.0 (ReconAgent.sweep): cannot compare the contract with "
        "temporal-agent-harness's schema: `temporal-agent-harness schema x:Y`: it broke[2J"
    ]


# --------------------------------------------------------------------------- the examples

#: The seller examples that use the harness (a worker.py, and the harness imported in some
#: module beside it). None yet: kontra-sdk#58 adds examples/recon-harness, and this check
#: then covers its golden contract too.
HARNESS_EXAMPLES = sorted(
    w.parent.name
    for w in EXAMPLES.glob("*/worker.py")
    if any("temporal_agent_harness" in p.read_text(encoding="utf-8") for p in w.parent.glob("*.py"))
)


@pytest.mark.parametrize(
    "name",
    HARNESS_EXAMPLES
    or [
        pytest.param(
            None,
            marks=pytest.mark.skip(
                reason="no example sells a harness handler yet (kontra-sdk#58 adds "
                "examples/recon-harness)"
            ),
        )
    ],
)
def test_an_example_handler_agrees_with_the_harness(name: str) -> None:
    """What a seller copies publishes without a warning: the golden contract of an example
    that sells a handler (``examples/<name>/contract.yaml``, held byte for byte by
    tests/examples/test_golden.py) is what its binding derives, and equals the harness's
    schema for that handler with no difference at all."""
    from kontra.cli._project import load_worker

    root = EXAMPLES / name
    with load_worker(root) as worker:
        sold = [b for b in worker.offerings if b.agent is not None and b.handler is not None]
        if not sold:
            pytest.skip(f"examples/{name} uses the harness but sells no handler")
        golden = from_yaml((root / "contract.yaml").read_text(encoding="utf-8"))
        for bound in sold:
            assert bound.document() == golden
            schema = h.harness_schema(h.agent_target(bound.agent), cwd=root)
            assert h.compare_handler(golden, schema, bound.handler) == ()
