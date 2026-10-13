"""``kontra init``'s project passes its own test right after it is made: the whole
lifecycle -- quote, hire, delivery, accept, and a seller that never delivers refunding the
buyer -- with ``kontra.testing.Marketplace`` and no containers (PRD §19.3).

The scaffold's test runs as a seller would run it: ``pytest`` in the new directory, in its
own process (which starts its own Temporal dev server from the same cached CLI)."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

from kontra.testing.devserver import DevServer

from .support import Cli


@pytest.mark.temporal
@pytest.mark.timeout(600)
def test_the_scaffold_passes_its_own_test(dev_server: DevServer, tmp_path: Path) -> None:
    del dev_server  # only for its skip: no Temporal CLI, no lifecycle test
    result = Cli(tmp_path / "home", cwd=tmp_path)("init", "word-count", "--party", "acme")
    assert result.exit_code == 0, result.output
    env = {
        k: v
        for k, v in os.environ.items()
        if not k.startswith(("KONTRA_HOME", "KONTRA_GATE", "PYTEST_", "PYTHONPATH"))
    }
    proc = subprocess.run(
        [sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider"],
        cwd=tmp_path / "word-count",
        env=env,
        capture_output=True,
        text=True,
        timeout=540,
        check=False,
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "2 passed" in proc.stdout
