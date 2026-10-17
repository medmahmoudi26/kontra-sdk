"""Settings every test shares."""

from __future__ import annotations

import os
import sys
from collections.abc import Iterator

import pytest

# A developer's own key names a real party on the real gate (https://api.kontra.run, the
# default): no test may pick it up, in this process or in a subprocess it starts. Tests that
# use a key set their own.
os.environ.pop("KONTRA_API_KEY", None)


def _forget_agents() -> None:
    """OpenAI's ``agents`` not imported, as in a fresh worker process. Importing it is how a
    workflow says it runs an agent (decisions I25): a worker built while it is in
    ``sys.modules`` gets the agents plugin and its converter. Only the top-level entry goes,
    so a test that imports it again gets the same classes (its submodules stay)."""
    sys.modules.pop("agents", None)


def pytest_collection_finish(session: pytest.Session) -> None:
    _forget_agents()  # what collection imported, before any fixture builds a worker


@pytest.fixture(autouse=True)
def _no_agents_imported() -> Iterator[None]:
    """Each test starts, and leaves, with ``agents`` not imported: a test that imports it
    (an agent workflow's project, say) puts it in this process's ``sys.modules``, and every
    worker built after it would get the plugin."""
    _forget_agents()
    yield
    _forget_agents()
