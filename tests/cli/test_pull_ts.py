"""Acceptance criterion 16, second half: `kontra pull --lang ts` on the enrich example's
published contract yields a TypeScript client that type-checks (tsc --strict) against a
value shaped like the example's output.

Slow, and it needs the outside world: nexgen (``$NEXGEN_BIN``, else the pinned release,
downloaded), Docker when this machine's glibc is older than nexgen's, and npm for the
client's peer dependencies (pinned below) and tsc. Any of them missing skips the test."""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest

from kontra.testing import Marketplace

from .support import Cli

pytestmark = [pytest.mark.slow, pytest.mark.timeout(900)]

ENRICH = Path(__file__).resolve().parents[2] / "examples" / "enrich"
#: What nexgen 0.2.5's TypeScript output imports, at versions it was checked with.
PEER_DEPS = ("nexus-rpc@0.0.3", "@temporalio/common@1.24.0", "typescript@5.9.3")
TSCONFIG = {
    "compilerOptions": {
        "strict": True,
        "noEmit": True,
        "target": "ES2022",
        "lib": ["ES2022"],
        "module": "ES2022",
        "moduleResolution": "bundler",
        "skipLibCheck": True,
        "types": [],
    }
}
# A value shaped like the enrich example's output (EnrichOutput), as it arrives on the wire.
CHECK_TS = """
import { acmeEnrich, enrichOutputTransferTypeConverter } from "./enrich/index";
import type { EnrichInput, EnrichOutput, KontraArtifact } from "./enrich/index";

const wire: unknown = {
  records: {
    content_type: "application/x-ndjson",
    key: "0123456789abcdef0123456789abcdef",
    sha256: "%s",
    size: 4096,
  },
  summary: { urls: 3, ok: 2, failed: 1 },
};
const out: EnrichOutput = enrichOutputTransferTypeConverter.fromTransferType(wire);
const records: KontraArtifact = out.records;
const size: number = records.size;
const contentType: string = records.contentType;
const ok: number | undefined = out.summary.additionalProperties["ok"];
const input: EnrichInput = { urls: ["https://example.com"] };
const operation = acmeEnrich.operations.execute;
export const seen = [size, contentType, ok, input, operation] as const;
""" % ("ab" * 32)
# The same client must refuse what the example's output is not.
WRONG_TS = """
import type { EnrichOutput } from "./enrich/index";
export const wrong: EnrichOutput = { records: "not an artifact", summary: {} };
"""


def _tsc(project: Path, files: list[str]) -> subprocess.CompletedProcess[str]:
    config = dict(TSCONFIG, include=[*files, "enrich/**/*.ts"])
    (project / "tsconfig.json").write_text(json.dumps(config))
    return subprocess.run(
        ["node", "node_modules/typescript/bin/tsc", "-p", "tsconfig.json"],
        cwd=project,
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    )


async def test_pull_ts_type_checks_against_the_enrich_output(tmp_path: Path) -> None:
    if shutil.which("node") is None or shutil.which("npm") is None:
        pytest.skip("needs node and npm")
    async with Marketplace(temporal=False) as mp:  # the catalog is all `pull` needs
        acme = await mp.party("acme")
        seller = Cli(acme.home)
        published = await seller.json("publish", "--dir", str(ENRICH))
        assert published[0]["offering"] == "acme/enrich@1.2.0"

        project = tmp_path / "ts"
        project.mkdir()
        env = {"KONTRA_NEXGEN": os.environ["NEXGEN_BIN"]} if os.environ.get("NEXGEN_BIN") else {}
        puller = Cli(tmp_path / "puller", env=env)
        pulled = await puller.run(
            "pull", "acme/enrich", "--lang", "ts", "-o", str(project / "enrich"),
            "--gate", mp.base_url, "--json",
        )  # fmt: skip
        if pulled.exit_code != 0 and any(
            reason in pulled.stderr for reason in ("cannot download", "install Docker")
        ):
            pytest.skip(pulled.stderr.strip())
        assert pulled.exit_code == 0, pulled.output
        assert json.loads(pulled.stdout)["contract_hash"] == published[0]["contract_hash"]

    await asyncio.to_thread(_typecheck, project)


def _typecheck(project: Path) -> None:
    (project / "package.json").write_text('{"name": "pull-check", "private": true}')
    npm = subprocess.run(
        [
            "npm",
            "install",
            "--no-audit",
            "--no-fund",
            "--ignore-scripts",
            "--loglevel=error",
            *PEER_DEPS,
        ],
        cwd=project,
        capture_output=True,
        text=True,
        timeout=600,
        check=False,
    )
    if npm.returncode != 0:
        pytest.skip(f"npm install failed (offline?): {npm.stderr.strip()[-500:]}")

    (project / "check.ts").write_text(CHECK_TS)
    checked = _tsc(project, ["check.ts"])
    assert checked.returncode == 0, checked.stdout + checked.stderr

    (project / "wrong.ts").write_text(WRONG_TS)
    refused = _tsc(project, ["wrong.ts"])
    assert refused.returncode != 0
    assert "wrong.ts" in refused.stdout
