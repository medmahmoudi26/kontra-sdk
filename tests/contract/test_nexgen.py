"""Acceptance 16's basis: nexgen accepts the nexgen view of a derived contract for all four
targets, and the generated bindings speak the wire names the handler serves; and a buyer's
generated Python and TypeScript types read every delivery of the zoo (decisions I38).

nexgen 0.2.5 is a Rust binary that needs glibc 2.39: it runs as it is where it can start,
and inside ``ubuntu:24.04`` where it cannot. Point ``NEXGEN_BIN`` at it (or put ``nexgen``
on PATH); without the binary, or without docker on an older glibc, the tests skip. The
TypeScript reader also needs node and npm (the client's peer dependencies, pinned below).
"""

from __future__ import annotations

import functools
import importlib
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest
from pydantic import BaseModel, create_model
from temporalio.api.common.v1 import Payload
from temporalio.converter import DataConverter
from temporalio.exceptions import ApplicationError

from kontra.contract import OfferingSpec, derive_document, merge_document, nexgen_view, to_yaml

from .test_derive import Rich
from .test_patterns import PORTABLE
from .zoo import (
    CASES,
    Computed,
    Defaults,
    EnumOverlap,
    Excluded,
    Nested,
    Serialized,
    SplitAlias,
    Tagged,
)

pytestmark = pytest.mark.slow

LANGS = ("go", "python", "typescript", "java")
IMAGE = "ubuntu:24.04"


def _binary() -> Path | None:
    env = os.environ.get("NEXGEN_BIN")
    candidate = env or shutil.which("nexgen")
    if candidate and Path(candidate).is_file():
        return Path(candidate).resolve()
    return None


def _docker() -> bool:
    if shutil.which("docker") is None:
        return False
    probe = subprocess.run(
        ["docker", "image", "inspect", IMAGE], capture_output=True, timeout=60, check=False
    )
    if probe.returncode == 0:
        return True
    pull = subprocess.run(["docker", "pull", IMAGE], capture_output=True, timeout=600, check=False)
    return pull.returncode == 0


@functools.cache
def _starts(binary: Path) -> bool:
    """Whether the binary runs on this machine (its glibc is new enough)."""
    try:
        probe = subprocess.run(
            [str(binary), "--version"], capture_output=True, timeout=60, check=False
        )
    except OSError:
        return False
    return probe.returncode == 0


@pytest.fixture(scope="module")
def nexgen() -> Path:
    binary = _binary()
    if binary is None:
        pytest.skip("set NEXGEN_BIN to a nexgen 0.2.5 linux binary")
    if not _starts(binary) and not _docker():
        pytest.skip(f"nexgen does not start here, and docker with {IMAGE} is not available")
    return binary


def _run(
    nexgen: Path, workdir: Path, lang: str, doc: dict[str, Any]
) -> tuple[subprocess.CompletedProcess[str], Path]:
    (workdir / "contract.nexusrpc.yaml").write_text(to_yaml(nexgen_view(doc)), encoding="utf-8")
    # the last directory names the package: Go's must not be the keyword "go"
    out = {"java": "gen/java/enrich", "go": "gen/enrich"}.get(lang, f"gen/{lang}")
    args = [lang, "contract.nexusrpc.yaml", "--output", out]
    if lang == "java":
        args += ["--package-name", "run.kontra.acme.enrich"]  # last segment = output dir
    if _starts(nexgen):
        cmd = [str(nexgen), *args]
    else:
        cmd = [
            "docker", "run", "--rm",
            "-v", f"{workdir}:/w",
            "-v", f"{nexgen}:/usr/local/bin/nexgen:ro",
            "-w", "/w",
            IMAGE,
            "nexgen", *args,
        ]  # fmt: skip
    proc = subprocess.run(
        cmd, cwd=workdir, capture_output=True, text=True, timeout=300, check=False
    )
    return proc, workdir / out


def _generated_text(root: Path) -> str:
    return "\n".join(p.read_text(errors="replace") for p in sorted(root.rglob("*")) if p.is_file())


@pytest.mark.parametrize("lang", LANGS)
def test_nexgen_accepts_the_enrich_contract(
    nexgen: Path, tmp_path: Path, enrich_doc: dict[str, Any], lang: str
) -> None:
    proc, out = _run(nexgen, tmp_path, lang, enrich_doc)
    assert proc.returncode == 0, proc.stderr + proc.stdout
    text = _generated_text(out)
    assert "acme.enrich" in text  # the Nexus service name on the wire
    assert '"quote"' in text
    assert '"execute"' in text
    assert "KontraArtifact" in text
    assert "KontraQuote" in text


@pytest.mark.parametrize("lang", LANGS)
def test_nexgen_accepts_every_lowering(
    nexgen: Path, tmp_path: Path, enrich_spec: OfferingSpec, lang: str
) -> None:
    """Optional, enums, tagged unions, typed maps, datetimes, scalar defaults: everything
    the derivation lowers is something nexgen generates for."""
    doc = derive_document(enrich_spec, Rich, Rich)
    proc, _ = _run(nexgen, tmp_path, lang, doc)
    assert proc.returncode == 0, proc.stderr + proc.stdout


class Zoo(BaseModel):
    """Every way an output serializes otherwise than it validates (decisions I38): the
    contract has ``ZooInput``/``ZooOutput`` and the like, and output members all required."""

    computed: Computed
    excluded: Excluded
    split: SplitAlias
    serialized: Serialized
    defaults: Defaults
    nested: Nested
    tagged: Tagged
    color: EnumOverlap


@pytest.mark.parametrize("lang", LANGS)
def test_nexgen_accepts_outputs_in_serialization_mode(
    nexgen: Path, tmp_path: Path, enrich_spec: OfferingSpec, lang: str
) -> None:
    doc = derive_document(enrich_spec, Zoo, Zoo)
    assert {"ZooInput", "ZooOutput", "DogInput", "DogOutput"} <= set(doc["$defs"])
    proc, _ = _run(nexgen, tmp_path, lang, doc)
    assert proc.returncode == 0, proc.stderr + proc.stdout


# --------------------------------------------------------------------------- a buyer reads it


def _zoo_delivery(spec: OfferingSpec, *, without: tuple[str, ...] = ()) -> tuple[Any, Any]:
    """The contract of one model with a member per zoo case, and a delivery of it: every
    case's ``model_dump(mode="json", by_alias=True)``, as the buyer receives it."""
    cases = {name: case for name, case in CASES.items() if name not in without}
    zoo = create_model("Zoo", **{name: (model, ...) for name, (model, _) in cases.items()})
    value = zoo(**{name: model.model_validate(v) for name, (model, v) in cases.items()})
    return derive_document(spec, zoo, zoo), value.model_dump(mode="json", by_alias=True)


async def test_a_generated_python_type_reads_every_delivery(
    nexgen: Path, tmp_path: Path, enrich_spec: OfferingSpec
) -> None:
    """``kontra pull --lang python``'s types (nexgen's), decoding a delivery as a buyer's
    client does (temporalio's converter, with the result type): every member of every case
    is there -- defaulted, computed, aliased, nullable and required -- and the value
    re-encodes to the same JSON; a delivery without a member is refused. ``aliases`` is
    left out: nexgen 0.2.5 refuses a member named ``from`` for Python (a reserved word;
    Go, Java and TypeScript accept it), a gap of the derivation that is not I38's."""
    doc, delivery = _zoo_delivery(enrich_spec, without=("aliases",))
    proc, out = _run(nexgen, tmp_path, "python", doc)
    assert proc.returncode == 0, proc.stderr + proc.stdout
    assert (out / "__init__.py").is_file()
    sys.path.insert(0, str(out.parent))
    try:
        client = importlib.import_module(out.name)
        converter = DataConverter.default

        def payload(value: Any) -> Payload:
            return Payload(metadata={"encoding": b"json/plain"}, data=json.dumps(value).encode())

        [read] = await converter.decode([payload(delivery)], [client.ZooOutput])
        assert type(read).__name__ == "ZooOutput"
        assert (read.optionals.must, read.optionals.note, read.defaults.n) == (None, None, 3)
        [written] = await converter.encode([read])
        assert json.loads(written.data) == delivery
        short = json.loads(json.dumps(delivery))
        del short["defaults"]["n"]
        with pytest.raises(ApplicationError) as refused:
            await converter.decode([payload(short)], [client.ZooOutput])
        assert refused.value.type == "PayloadValidationError"
        [violations] = refused.value.details
        assert [(v.path, v.reason) for v in violations] == [("defaults.n", "required")]
    finally:
        sys.path.remove(str(out.parent))
        for name in [m for m in sys.modules if m == out.name or m.startswith(f"{out.name}.")]:
            del sys.modules[name]


#: What nexgen 0.2.5's TypeScript output imports, at the versions tests/cli/test_pull_ts.py
#: checks it with.
PEER_DEPS = ("nexus-rpc@0.0.3", "@temporalio/common@1.24.0", "typescript@5.9.3")
_READ_TS = """
declare const console: { log(message: string): void };
import { zooOutputTransferTypeConverter } from "./gen/typescript/index";
import type { ZooOutput } from "./gen/typescript/index";

const wire: unknown = %s;
const read: ZooOutput = zooOutputTransferTypeConverter.fromTransferType(wire);
const written: unknown = zooOutputTransferTypeConverter.toTransferType(read);
let refused = false;
try {
  const short = JSON.parse(JSON.stringify(wire));
  delete short.defaults.n;
  zooOutputTransferTypeConverter.fromTransferType(short);
} catch {
  refused = true;
}
console.log(JSON.stringify({ written, refused }));
"""


def test_a_generated_typescript_type_reads_every_delivery(
    nexgen: Path, tmp_path: Path, enrich_spec: OfferingSpec
) -> None:
    """``kontra pull --lang ts``'s client, compiled under ``tsc --strict`` and run: its
    output converter reads every case's delivery, writes back the same JSON, and refuses a
    delivery without a member."""
    if shutil.which("node") is None or shutil.which("npm") is None:
        pytest.skip("needs node and npm")
    doc, delivery = _zoo_delivery(enrich_spec)
    proc, _ = _run(nexgen, tmp_path, "typescript", doc)
    assert proc.returncode == 0, proc.stderr + proc.stdout
    (tmp_path / "package.json").write_text('{"name": "zoo-read", "private": true}')
    npm = subprocess.run(
        ["npm", "install", "--no-audit", "--no-fund", "--ignore-scripts", "--loglevel=error",
         *PEER_DEPS],
        cwd=tmp_path, capture_output=True, text=True, timeout=600, check=False,
    )  # fmt: skip
    if npm.returncode != 0:
        pytest.skip(f"npm install failed (offline?): {npm.stderr.strip()[-500:]}")
    (tmp_path / "read.ts").write_text(_READ_TS % json.dumps(delivery))
    config = {
        "compilerOptions": {
            "strict": True, "target": "ES2022", "lib": ["ES2022"], "module": "commonjs",
            "moduleResolution": "node10", "skipLibCheck": True, "types": [], "outDir": "out",
        },
        "include": ["read.ts", "gen/typescript/**/*.ts"],
    }  # fmt: skip
    (tmp_path / "tsconfig.json").write_text(json.dumps(config))
    tsc = subprocess.run(
        ["node", "node_modules/typescript/bin/tsc", "-p", "tsconfig.json"],
        cwd=tmp_path, capture_output=True, text=True, timeout=300, check=False,
    )  # fmt: skip
    assert tsc.returncode == 0, tsc.stdout + tsc.stderr
    run = subprocess.run(
        ["node", "out/read.js"], cwd=tmp_path, capture_output=True, text=True, timeout=60,
        check=False,
    )  # fmt: skip
    assert run.returncode == 0, run.stdout + run.stderr
    result = json.loads(run.stdout)
    assert result == {"written": delivery, "refused": True}


def _edge_document(spec: OfferingSpec) -> dict[str, Any]:
    """The edge of what the catalog accepts: every portable pattern, the punctuation a
    property name and an enum value may carry, multi-line prose, scalar defaults."""
    props: dict[str, Any] = {
        f"p{i}": {"type": "string", "pattern": p} for i, p in enumerate(PORTABLE)
    }
    props["a-b"] = {
        "type": "string",
        "description": 'multi\nline\tprose with */ and """ and ` and ${x} and \\u002a/',
    }
    props["_x"] = {"type": "string", "title": "A title", "default": "hello"}
    props["en"] = {
        "type": "string",
        "enum": ["a.b", "c:d", "e/f", "g@h", "i+j", "k=l", "m!", "n?", "o,p", "q;r", "s#t",
                 "u~v", "w^x", "y|z", "(a1)", "[b1]"],
    }  # fmt: skip
    props["co"] = {"type": "string", "const": "v1.2/x"}
    props["num"] = {"type": "integer", "minimum": 1, "maximum": 10, "default": 5}
    nexus = {
        "nexusrpc": "1.0.0",
        "services": {
            "AcmeEnrich": {
                "fqn": "acme.enrich",
                "description": "line one\nline two",
                "operations": {
                    "quote": {
                        "fqn": "quote",
                        "input": {"$ref": "#/$defs/Edge"},
                        "output": {"$ref": "#/$defs/KontraQuote"},
                    },
                    "execute": {
                        "fqn": "execute",
                        "input": {"$ref": "#/$defs/Edge"},
                        "output": {"$ref": "#/$defs/Edge"},
                    },
                },
            }
        },
        "$defs": {"Edge": {"type": "object", "additionalProperties": False, "properties": props}},
    }
    return merge_document(spec, nexus)


@pytest.mark.parametrize("lang", LANGS)
def test_nexgen_accepts_the_edge_of_the_catalog(
    nexgen: Path, tmp_path: Path, enrich_spec: OfferingSpec, lang: str
) -> None:
    """What the catalog's stricter rules (patterns, names, literals) still accept, nexgen
    generates for: tightening them did not trade one failure for another."""
    proc, _ = _run(nexgen, tmp_path, lang, _edge_document(enrich_spec))
    assert proc.returncode == 0, proc.stderr + proc.stdout
