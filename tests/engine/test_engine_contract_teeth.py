"""The engine contract has teeth: every deliberately broken engine in ``broken_engines`` fails
it, each on the rule its bug breaks (seam ⑥)."""

from __future__ import annotations

import pytest
from broken_engines import BROKEN
from engine_harness import FakeHarness

from kontra.testing.engine import FakeEngine
from kontra.testing.engine_contract import EngineContractViolation, run_engine_contract

#: The rule each broken engine breaks.
RULE = {
    "DoubleStart": "E4",
    "SoftEnd": "E6",
    "RevivableEnd": "E6",
    "TrustsAnyRun": "E4",
    "Leaky": "E12",
    "NoFence": "E10",
    "FenceStops": "E10",
    "DriftingPin": "E11",
    "StrangerReaches": "E18",
    "GuessesOnError": "E17",
    "LaxHandles": "E3",
    "IOBeforeParse": "E3",
    "UnlistedTopics": "E14",
    "RewindsOnBuyerWord": "E14",
    "PlaintextItems": "E14",
}


def test_every_broken_engine_names_its_rule() -> None:
    assert set(RULE) == {cls.__name__ for cls in BROKEN}


@pytest.mark.parametrize("broken", BROKEN, ids=lambda c: c.__name__)
async def test_the_contract_rejects(broken: type[FakeEngine]) -> None:
    with pytest.raises(EngineContractViolation) as e:
        await run_engine_contract(lambda: FakeHarness(broken()).setup(), settle=0.05, wait=3.0)
    assert str(e.value).startswith(RULE[broken.__name__] + ":"), str(e.value)
