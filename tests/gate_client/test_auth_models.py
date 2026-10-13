"""Seam ②, the sign-in and API-key additions: the route rows exactly as the production
contract states them, the email and user-code rules of ``MagicLinkRequest``, each token
grant taking exactly its own fields, and credentials kept out of every ``repr``."""

from __future__ import annotations

import pytest
from pydantic import BaseModel, ValidationError

from kontra.gate_client import models as m
from kontra.gate_client.routes import BY_NAME

KEY = m.API_KEY_PREFIX + "Ab3_-" * 8 + "xyz"
CODE = "BCDF-GHJK"


@pytest.mark.parametrize(
    ("name", "method", "path", "auth", "request_model", "response_model", "status"),
    [
        ("magic_link", "POST", "/v1/auth/magic", "none", m.MagicLinkRequest, m.MagicLinkSent, 202),
        ("token", "POST", "/v1/auth/token", "none", m.TokenRequest, m.TokenResponse, 200),
        ("create_api_key", "POST", "/v1/keys", "fresh", m.ApiKeyCreate, m.ApiKeyCreated, 201),
        ("api_keys", "GET", "/v1/keys", "fresh", None, m.ApiKeyList, 200),
        ("revoke_api_key", "DELETE", "/v1/keys/{key_id}", "fresh", None, None, 204),
        # ``kontra serve`` with a key mints worker tokens: this one stays ``party``.
        (
            "temporal_token",
            "POST",
            "/v1/auth/temporal-token",
            "party",
            m.TemporalTokenRequest,
            m.TemporalToken,
            200,
        ),
    ],
)
def test_the_rows_are_the_contracts(
    name: str,
    method: str,
    path: str,
    auth: str,
    request_model: type[BaseModel] | None,
    response_model: type[BaseModel] | None,
    status: int,
) -> None:
    row = BY_NAME[name]
    assert (row.method, row.path, row.auth, row.request, row.response, row.status) == (
        method,
        path,
        auth,
        request_model,
        response_model,
        status,
    )


def test_every_bodiless_answer_is_a_204_and_only_those() -> None:
    for row in BY_NAME.values():
        if row.response is None and not row.stream and row.method != "GET":
            assert row.status == 204, row.name
        elif row.status == 204:
            pytest.fail(f"{row.name} answers 204 with a body")


# --------------------------------------------------------------------------- magic link


@pytest.mark.parametrize(
    "email",
    [
        "a@b.co",
        "first.last+tag@example.com",
        "under_score%percent-dash@sub.example.org",
        "UPPER@EXAMPLE.COM",
        "x@xn--80ak6aa92e.com",
        "x@example.xn--p1ai",
        "e" * 64 + "@example.com",
    ],
)
def test_plain_addresses_are_accepted(email: str) -> None:
    assert m.is_email(email)
    assert m.MagicLinkRequest(email=email, device_user_code=CODE).email == email


def test_the_address_is_stripped_but_not_lowercased() -> None:
    req = m.MagicLinkRequest(email="  Bob@Example.COM\t\n", device_user_code=CODE)
    assert req.email == "Bob@Example.COM"  # the gate lowercases; the wire keeps what was typed


@pytest.mark.parametrize(
    "email",
    [
        "",
        "plain",
        "@example.com",
        "a@",
        "a@localhost",  # one label
        "a@1.2.3.4",  # an IP, bracketed or not
        "a@[1.2.3.4]",
        "a@[IPv6:::1]",
        "Bob <bob@example.com>",  # display names
        '"bob"@example.com',  # quoted local parts
        "bob(comment)@example.com",  # comments
        "a..b@example.com",
        ".a@example.com",
        "a.@example.com",
        "a@-example.com",
        "a@example-.com",
        "a@example.c",
        "a@example.123",
        "a b@example.com",
        "a@b.co\nBcc: x@y.co",  # header injection
        "ünï@example.com",
        "a@exämple.com",
        "a/b@example.com",
        "a|b@example.com",
        "a`b@example.com",
        "a@b@example.com",
        "e" * 65 + "@example.com",  # the local part's 64
        "e" * 64 + "@" + "d" * 63 + "." + "d" * 63 + "." + "d" * 63 + ".com",  # 254 in all
    ],
)
def test_anything_but_a_plain_address_is_refused(email: str) -> None:
    assert not m.is_email(email.strip())
    with pytest.raises(ValidationError) as e:
        m.MagicLinkRequest(email=email, device_user_code=CODE)
    if "@" in email:
        for err in e.value.errors(include_input=False):
            assert email.strip() not in str(err.get("msg", ""))  # never echoed


def test_a_long_address_is_refused_before_its_pattern_runs() -> None:
    with pytest.raises(ValidationError) as e:
        m.MagicLinkRequest(email="a" * (m.EMAIL_MAX + 1), device_user_code=CODE)
    assert [err["type"] for err in e.value.errors()] == ["string_too_long"]


@pytest.mark.parametrize(
    "code", ["bcdf-ghjk", "BCDFGHJK", "BCDF-GHJ", "BCDF-GHJKL", "ABCD-EFGH", "BCDF-GHJK\n", ""]
)
def test_the_user_code_is_the_gates_own_spelling(code: str) -> None:
    assert not m.is_user_code(code)
    with pytest.raises(ValidationError):
        m.MagicLinkRequest(email="a@b.co", device_user_code=code)


def test_magic_link_sent_says_one_thing() -> None:
    assert m.MagicLinkSent().model_dump() == {"status": "sent"}
    with pytest.raises(ValidationError):
        m.MagicLinkSent.model_validate({"status": "not invited"})


# --------------------------------------------------------------------------- token grants


@pytest.mark.parametrize(
    ("fields", "problem"),
    [
        ({"grant_type": m.DEVICE_GRANT, "device_code": "d"}, None),
        ({"grant_type": m.DEVICE_GRANT, "device_code": "d", "handle": "bob"}, None),
        ({"grant_type": m.DEVICE_GRANT}, "device_code"),
        ({"grant_type": m.DEVICE_GRANT, "device_code": "d", "refresh_token": "r"}, "device_code"),
        ({"grant_type": m.DEVICE_GRANT, "device_code": "d", "api_key": KEY}, "device_code"),
        ({"grant_type": "refresh_token", "refresh_token": "r"}, None),
        ({"grant_type": "refresh_token"}, "refresh_token"),
        ({"grant_type": "refresh_token", "refresh_token": "r", "handle": "bob"}, "refresh_token"),
        ({"grant_type": "refresh_token", "refresh_token": "r", "api_key": KEY}, "refresh_token"),
        ({"grant_type": "api_key", "api_key": KEY}, None),
        ({"grant_type": "api_key"}, "api_key"),
        ({"grant_type": "api_key", "api_key": KEY, "refresh_token": "r"}, "api_key"),
        ({"grant_type": "api_key", "api_key": KEY, "device_code": "d"}, "api_key"),
        ({"grant_type": "api_key", "api_key": KEY, "handle": "bob"}, "api_key"),
    ],
)
def test_each_grant_takes_exactly_its_own_fields(
    fields: dict[str, str], problem: str | None
) -> None:
    found = m.TokenRequest.model_validate(fields).grant_error()
    if problem is None:
        assert found is None
    else:
        assert found == f"the {problem} grant takes {problem} only"


@pytest.mark.parametrize(
    "key",
    [
        KEY[:-1],
        KEY + "x",
        KEY + "\n",
        " " + KEY,
        "kontra_pk_" + KEY[len(m.API_KEY_PREFIX) :],
        KEY[:-1] + "=",  # padded base64
        KEY[:-1] + "+",  # the standard alphabet
        KEY[:-1] + "é",
    ],
)
def test_only_a_key_of_the_minted_shape_is_an_api_key(key: str) -> None:
    assert m.is_api_key(KEY)
    assert not m.is_api_key(key)
    with pytest.raises(ValidationError):
        m.TokenRequest(grant_type="api_key", api_key=key)


def test_secrets_stay_out_of_every_repr() -> None:
    req = m.TokenRequest(grant_type="api_key", api_key=KEY)
    tok = m.TokenResponse.model_validate(
        {
            "access_token": "acc",
            "expires_in": 900,
            "refresh_token": "rt-secret",
            "party": "bob",
            "auth_time": "2026-10-13T00:00:00Z",
        }
    )
    made = m.ApiKeyCreated.model_validate(
        {
            "id": "key_0123456789abcdef",
            "label": "ci",
            "created_at": "2026-10-13T00:00:00Z",
            "key": KEY,
        }
    )
    for model, secret in ((req, KEY), (tok, "rt-secret"), (made, KEY)):
        assert secret not in repr(model)
        assert secret not in str(model)
    assert made.model_dump()["key"] == KEY  # ... and still on the wire, where it belongs


def test_an_api_key_exchange_has_no_refresh_token() -> None:
    tok = m.TokenResponse.model_validate(
        {
            "access_token": "a",
            "expires_in": 900,
            "party": "bob",
            "auth_time": "2026-10-13T00:00:00Z",
        }
    )
    assert tok.refresh_token is None


# --------------------------------------------------------------------------- API keys


@pytest.mark.parametrize("label", ["x", "ci-prod", "Mohamed's laptop (old)", "~" * 64, " "])
def test_labels_are_printable_ascii(label: str) -> None:
    assert m.ApiKeyCreate(label=label).label == label


@pytest.mark.parametrize("label", ["", "x" * 65, "tab\there", "new\nline", "\x1b[31mred", "é"])
def test_other_labels_are_refused(label: str) -> None:
    with pytest.raises(ValidationError):
        m.ApiKeyCreate(label=label)


@pytest.mark.parametrize(
    "key_id",
    [
        "key_0123456789ABCDEF",
        "key_0123456789abcde",
        "key_0123456789abcdef0",
        "kex_0123456789abcdef",
    ],
)
def test_key_ids_are_key_and_16_lowercase_hex(key_id: str) -> None:
    assert m.is_api_key_id("key_0123456789abcdef")
    assert not m.is_api_key_id(key_id)
    with pytest.raises(ValidationError):
        m.ApiKeyView.model_validate(
            {"id": key_id, "label": "ci", "created_at": "2026-10-13T00:00:00Z"}
        )
