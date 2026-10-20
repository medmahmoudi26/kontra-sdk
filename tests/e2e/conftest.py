"""The ``kontra dev`` stack for ``tests/e2e`` (``make e2e``).

One session-scoped fixture brings the stack up with ``kontra dev up`` -- the command a seller
runs, with the environment's settings: ``KONTRA_HOME``, the four ``KONTRA_DEV_*_PORT``
host ports and the two images (``KONTRA_GATE_IMAGE``, ``KONTRA_TEMPORAL_IMAGE``) -- and takes
it down with ``kontra dev down --volumes`` at the end, so every session starts from an empty
catalog and ledger. ``KONTRA_E2E_KEEP=1`` leaves it up (and its data) for a look afterwards.

Every test module here marks its tests ``e2e`` (deselected by default: ``addopts = -m 'not e2e'``). They are
skipped, saying why, when Docker or either image is missing; ``make e2e`` checks the Temporal
image before it starts, and CI pulls the gate image first.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import time
from collections.abc import Iterator

import pytest

from . import stack as e2e_stack
from .support import Stack, read_env_file

_GATE_IMAGE = "ghcr.io/medmahmoudi26/kontra-gate:dev"
_TEMPORAL_IMAGE = "ghcr.io/medmahmoudi26/kontra-temporal:dev"


def _missing() -> str | None:
    """Why the stack cannot come up here, or None."""
    if shutil.which("docker") is None:
        return "tests/e2e need Docker with the compose plugin (make stack-up runs compose.yaml)"
    for var, default, how in (
        ("KONTRA_TEMPORAL_IMAGE", _TEMPORAL_IMAGE, "`make temporal` builds kontra-temporal:dev"),
        ("KONTRA_GATE_IMAGE", _GATE_IMAGE, "kontra-cloud's `make image` builds kontra-gate:dev"),
    ):
        image = os.environ.get(var) or default
        found = subprocess.run(
            ["docker", "image", "inspect", image], capture_output=True, check=False, timeout=60
        )
        if found.returncode != 0:
            return f"the image {image} ({var}) is not on this machine: {how}, or pull it"
    return None


@pytest.fixture(scope="session")
def stack(tmp_path_factory: pytest.TempPathFactory) -> Iterator[Stack]:
    reason = _missing()
    if reason is not None:
        pytest.skip(reason)
    from kontra.profile import kontra_home

    home = kontra_home()
    try:
        started = time.monotonic()
        e2e_stack.up()  # idempotent: `make e2e` already started it
        # The stack's start, for acceptance 1: `make e2e` brings it up before the session
        # and says how long that took; otherwise this `up` started it (or found it up).
        up_seconds = float(os.environ.get("KONTRA_E2E_UP_SECONDS") or (time.monotonic() - started))
        settings = read_env_file(e2e_stack.stack_env())
        gate = e2e_stack.gate_env(e2e_stack.config())
        yield Stack(
            home=home,
            gate_url=gate["KONTRA_PUBLIC_URL"].rstrip("/"),
            temporal=gate["KONTRA_TEMPORAL_PUBLIC"],
            s3_endpoint=gate["KONTRA_S3_PUBLIC_ENDPOINT"].rstrip("/"),
            s3_bucket=gate["KONTRA_S3_BUCKET"],
            s3_region=gate["KONTRA_S3_REGION"],
            s3_access_key=settings["KONTRA_S3_ACCESS_KEY"],
            s3_secret_key=settings["KONTRA_S3_SECRET_KEY"],
            pg_user=settings.get("KONTRA_PG_USER") or os.environ.get("KONTRA_PG_USER") or "kontra",
            parties=tmp_path_factory.mktemp("parties"),
            up_seconds=up_seconds,
            ledger=os.environ.get("KONTRA_LEDGER") or "postgres",
        )
    finally:
        if os.environ.get("KONTRA_E2E_KEEP") != "1":
            e2e_stack.down()
