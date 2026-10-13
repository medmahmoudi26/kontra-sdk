"""``kontra mcp`` (``kontra.mcp.main``) before any agent connects."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


def test_kontra_mcp_refuses_to_start_without_a_profile(tmp_path: Path) -> None:
    done = subprocess.run(
        [sys.executable, "-m", "kontra.mcp"],
        env={**os.environ, "KONTRA_HOME": str(tmp_path / "nobody")},
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert done.returncode == 1
    assert done.stdout == ""  # stdout is the protocol channel: nothing else goes there
    assert "kontra login" in done.stderr
