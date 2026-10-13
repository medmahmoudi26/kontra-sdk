"""Acceptance 16's basis: nexgen accepts the nexgen view of a derived contract for all four
targets, and the generated bindings speak the wire names the handler serves.

nexgen 0.2.5 is a Rust binary that needs glibc 2.39, so it runs inside ``ubuntu:24.04``.
Point ``NEXGEN_BIN`` at it (or put ``nexgen`` on PATH); without docker or the binary the
tests skip.
"""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path
from typing import Any

import pytest

from kontra.contract import OfferingSpec, derive_document, merge_document, nexgen_view, to_yaml

from .conftest import PUBLIC_KEY
from .test_derive import Rich
from .test_patterns import PORTABLE

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


@pytest.fixture(scope="module")
def nexgen() -> Path:
    binary = _binary()
    if binary is None:
        pytest.skip("set NEXGEN_BIN to a nexgen 0.2.5 linux binary")
    if not _docker():
        pytest.skip(f"docker with {IMAGE} is not available")
    return binary


def _run(
    nexgen: Path, workdir: Path, lang: str, doc: dict[str, Any]
) -> tuple[subprocess.CompletedProcess[str], Path]:
    (workdir / "contract.nexusrpc.yaml").write_text(to_yaml(nexgen_view(doc)), encoding="utf-8")
    # the last directory names the package: Go's must not be the keyword "go"
    out = {"java": "gen/java/enrich", "go": "gen/enrich"}.get(lang, f"gen/{lang}")
    cmd = [
        "docker", "run", "--rm",
        "-v", f"{workdir}:/w",
        "-v", f"{nexgen}:/usr/local/bin/nexgen:ro",
        "-w", "/w",
        IMAGE,
        "nexgen", lang, "contract.nexusrpc.yaml", "--output", out,
    ]  # fmt: skip
    if lang == "java":
        cmd += ["--package-name", "run.kontra.acme.enrich"]  # last segment = output dir
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=300, check=False)
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
    doc = derive_document(enrich_spec, Rich, Rich, PUBLIC_KEY)
    proc, _ = _run(nexgen, tmp_path, lang, doc)
    assert proc.returncode == 0, proc.stderr + proc.stdout


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
    return merge_document(spec, nexus, PUBLIC_KEY)


@pytest.mark.parametrize("lang", LANGS)
def test_nexgen_accepts_the_edge_of_the_catalog(
    nexgen: Path, tmp_path: Path, enrich_spec: OfferingSpec, lang: str
) -> None:
    """What the catalog's stricter rules (patterns, names, literals) still accept, nexgen
    generates for: tightening them did not trade one failure for another."""
    proc, _ = _run(nexgen, tmp_path, lang, _edge_document(enrich_spec))
    assert proc.returncode == 0, proc.stderr + proc.stdout
