"""A broken profile's error names the field and the problem, never a value.

``Kontra()`` (a script, ``kontra mcp`` for an agent) and ``python worker.py`` start by
reading the profile. It holds a refresh token, and people paste things into it -- an age
secret key, say. pydantic's own rendering of a validation error quotes the offending input,
so a profile with a value in the wrong place would put that secret on stderr, in a log, or
in an agent's context. No Temporal here: the profile is read before anything connects."""

from __future__ import annotations

import json
import traceback
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from kontra.codec.age import generate_identity
from kontra.hire import Kontra
from kontra.profile import PROFILE_FILE, Profile, ProfileError

SECRET, _ = generate_identity()
TOKEN = "rt_" + "q7" * 20  # a refresh token's shape: lowercase, digits, underscore


def _profile(**changes: Any) -> dict[str, Any]:
    good = Profile(
        gate_url="http://gate.test",
        temporal_address="127.0.0.1:7233",
        party="alice",
        namespace="alice",
        refresh_token=TOKEN,
        auth_time=datetime.now(UTC),
    )
    doc = json.loads(good.model_dump_json())
    doc.update(changes)
    return doc


CASES = {
    "secret-as-a-boolean": (_profile(tls=SECRET), "tls: Input should be a valid boolean"),
    "secret-as-a-key": ({**_profile(), SECRET: True}, "an unknown field: Extra inputs"),
    "token-as-a-key": ({**_profile(), TOKEN: "x"}, "an unknown field: Extra inputs"),
    "token-as-the-party": (_profile(party=TOKEN.upper()), "party: Value error, not a party"),
    "secret-as-the-gate": (_profile(gate_url=SECRET), "gate_url: Value error"),
    "token-not-a-string": (_profile(refresh_token=[TOKEN]), "refresh_token: Input should be"),
    "secret-as-the-time": (_profile(auth_time=SECRET), "auth_time: Input should be"),
}


@pytest.mark.parametrize("case", sorted(CASES))
def test_a_broken_profile_never_shows_its_values(tmp_path: Path, case: str) -> None:
    doc, says = CASES[case]
    (tmp_path / PROFILE_FILE).write_text(json.dumps(doc))
    for load in (lambda: Profile.load(tmp_path), lambda: Kontra(home=tmp_path)):
        with pytest.raises(ProfileError) as err:
            load()
        shown = "".join(traceback.format_exception(err.value))  # what a crash would print
        assert says in str(err.value), str(err.value)
        assert "kontra login" in str(err.value)
        for secret in (SECRET, SECRET.lower(), TOKEN, TOKEN.upper()):
            assert secret not in shown
        assert err.value.__cause__ is None
        assert err.value.__context__ is None  # the ValidationError is not chained either


def test_malformed_json_says_where_and_not_what(tmp_path: Path) -> None:
    (tmp_path / PROFILE_FILE).write_text('{"refresh_token": "' + TOKEN + '", oops}')
    with pytest.raises(ProfileError) as err:
        Profile.load(tmp_path)
    assert "the profile: Invalid JSON" in str(err.value)
    assert TOKEN not in "".join(traceback.format_exception(err.value))
