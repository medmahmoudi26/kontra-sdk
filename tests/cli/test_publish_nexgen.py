"""Acceptance criterion 16, first half, as written: the document ``kontra publish`` emits for
the ``enrich`` example -- published to a catalog, then read back by ``kontra pull`` -- is
one nexgen accepts for all four targets.

``tests/contract/test_nexgen.py`` covers what derivation can produce (every lowering, the
edge of the catalog) on sample models; this runs nexgen on the example's own contract, from
the command a seller runs, so a change to ``examples/enrich`` that nexgen refuses fails here.

Slow, and it needs nexgen (``$NEXGEN_BIN``, else the pinned release, downloaded) and Docker
when this machine's glibc is older than nexgen's; either missing skips the test."""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from kontra.testing import Marketplace

from .support import Cli

pytestmark = [pytest.mark.slow, pytest.mark.timeout(900)]

ENRICH = Path(__file__).resolve().parents[2] / "examples" / "enrich"
#: ``kontra pull --lang`` value, the extra options it needs, the output directory's name.
TARGETS = (
    ("go", (), "enrich"),
    ("python", (), "enrich"),
    ("ts", (), "enrich"),
    ("java", ("--package", "run.kontra.acme.enrich"), "enrich"),
)


async def test_nexgen_accepts_what_kontra_publish_emits_for_enrich_in_all_four_languages(
    tmp_path: Path,
) -> None:
    async with Marketplace(temporal=False) as mp:  # the catalog is all publish and pull need
        acme = await mp.party("acme")
        published = await Cli(acme.home).json("publish", "--dir", str(ENRICH))
        assert [p["offering"] for p in published] == ["acme/enrich@1.2.0"]
        env = {"KONTRA_NEXGEN": os.environ["NEXGEN_BIN"]} if os.environ.get("NEXGEN_BIN") else {}
        puller = Cli(tmp_path / "puller", env=env)
        languages: list[str] = []
        for lang, extra, name in TARGETS:
            out = tmp_path / lang / name
            pulled = await puller.run(
                "pull", "acme/enrich", "--lang", lang, "-o", str(out),
                "--gate", mp.base_url, "--json", *extra,
            )  # fmt: skip
            if pulled.exit_code != 0 and any(
                reason in pulled.stderr for reason in ("cannot download", "install Docker")
            ):
                pytest.skip(pulled.stderr.strip())
            assert pulled.exit_code == 0, (lang, pulled.output)
            summary = json.loads(pulled.stdout)
            # The published document itself (verified against its hash by pull), not a copy.
            assert summary["contract_hash"] == published[0]["contract_hash"], lang
            generated = [
                p for p in out.rglob("*") if p.is_file() and p.name != "contract.nexusrpc.yaml"
            ]
            assert generated, f"nexgen wrote nothing for {lang}"
            text = "\n".join(p.read_text(errors="replace") for p in generated)
            assert "acme.enrich" in text, lang  # the Nexus service name on the wire
            assert "execute" in text, lang
            languages.append(summary["language"])
    assert languages == ["go", "python", "typescript", "java"]
