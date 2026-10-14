from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from kontra.contract import OfferingSpec, derive_document, load_offering

from .sample_models import EnrichInput, EnrichOutput

HERE = Path(__file__).parent
#: A fixed, valid age X25519 recipient, so golden output does not depend on a fresh key.
PUBLIC_KEY = "age1e0q4lyexsefmh6hurumvgl3cmmsfnz4zyfhkd7lfrjcpqc2e250qs6j4el"


@pytest.fixture
def enrich_spec() -> OfferingSpec:
    return load_offering(HERE / "fixtures" / "enrich.offering.yaml")


@pytest.fixture
def enrich_doc(enrich_spec: OfferingSpec) -> dict[str, Any]:
    return derive_document(enrich_spec, EnrichInput, EnrichOutput)
