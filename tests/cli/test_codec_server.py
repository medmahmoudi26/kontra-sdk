"""The codec server ``kontra dev up --ui`` runs for Temporal's UI: Temporal's codec-server
protocol (``POST /decode`` and ``/encode`` with ``{"payloads": [...]}``) with this machine's
key, on 127.0.0.1 only, for the UI's origin only; and its life as a background process that
``kontra dev down`` stops, which ``kontra dev`` tells from another home's server on the
same port by the token it handed it."""

from __future__ import annotations

import json
import os
import socket
import stat
import subprocess
import sys
import threading
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import httpx
import pytest
from google.protobuf import json_format
from temporalio.api.common.v1 import Payload, Payloads

import kontra.cli._dev as dev_module
from kontra.cli._codec_server import MAX_BODY, CodecServer
from kontra.codec import CLAIM_CHECK_ENCODING, ENCODING, KontraCodec
from kontra.codec.age import decrypt, encrypt, generate_identity
from kontra.profile import KEY_FILE, KEY_FILE_ENV

SECRET, PUBLIC = generate_identity()
JOB_SECRET, JOB_PUBLIC = generate_identity()  # a key `kontra hire` made for one job
UI = "http://localhost:8233"
TOKEN = "stop-token-for-tests"
MARKER = "kontra-codec-server-marker-61d2"


def _codec() -> KontraCodec:
    return KontraCodec(SECRET, [PUBLIC], role="seller", extra_identities=lambda: (JOB_SECRET,))


@pytest.fixture
def server() -> Iterator[CodecServer]:
    srv = CodecServer(0, codec=_codec, origins=[UI, "http://127.0.0.1:8233"], token=TOKEN)
    thread = threading.Thread(target=srv.serve_forever, daemon=True)
    thread.start()
    try:
        yield srv
    finally:
        srv.shutdown()
        srv.server_close()
        thread.join(5)


def _plain(text: str) -> Payload:
    return Payload(metadata={"encoding": b"json/plain"}, data=json.dumps(text).encode())


def _sealed(p: Payload, *recipients: str) -> Payload:
    """``p`` as KontraCodec encodes it, to ``recipients``."""
    return Payload(
        metadata={"encoding": ENCODING, "kontra-v": b"1"},
        data=encrypt(p.SerializeToString(), recipients),
    )


def _body(*payloads: Payload) -> bytes:
    return json_format.MessageToJson(Payloads(payloads=payloads)).encode()


def _post(
    srv: CodecServer, path: str, body: bytes, *, origin: str | None = UI, **headers: str
) -> httpx.Response:
    sent = {"Content-Type": "application/json", "X-Namespace": "acme", **headers}
    if origin is not None:
        sent["Origin"] = origin
    with httpx.Client(trust_env=False) as http:
        return http.post(f"http://127.0.0.1:{srv.port}{path}", content=body, headers=sent)


def _payloads(r: httpx.Response) -> list[Payload]:
    out = Payloads()
    json_format.Parse(r.content, out)
    return list(out.payloads)


def test_decode_opens_what_is_ours_and_leaves_the_rest(server: CodecServer) -> None:
    ours = _sealed(_plain(f"in {MARKER}"), PUBLIC)
    job = _sealed(_plain(f"delivery {MARKER}"), JOB_PUBLIC)  # a job key under KONTRA_HOME
    theirs = _sealed(_plain("another party's"), generate_identity()[1])
    claim = Payload(
        metadata={"encoding": CLAIM_CHECK_ENCODING, "kontra-v": b"1"},
        data=b'{"sha256":"' + b"0" * 64 + b'","size":10}',
    )
    plain = _plain("a price")
    r = _post(server, "/decode", _body(ours, job, theirs, claim, plain))
    assert r.status_code == 200, r.text
    assert r.headers["Access-Control-Allow-Origin"] == UI
    assert r.headers["Vary"] == "Origin"
    out = _payloads(r)
    assert out[:2] == [_plain(f"in {MARKER}"), _plain(f"delivery {MARKER}")]
    # Not ours, a reference it cannot fetch, plaintext: answered unchanged.
    assert out[2:] == [theirs, claim, plain]


def test_the_ui_s_query_string_is_answered_alike(server: CodecServer) -> None:
    """Temporal's UI 2.55 posts to ``/decode?preserveStorageRefs=true`` (and preflights
    that); this server fetches no claim-check reference in any case."""
    sealed = _sealed(_plain(MARKER), PUBLIC)
    path = "/decode?preserveStorageRefs=true"
    with httpx.Client(trust_env=False) as http:
        pre = http.options(
            f"http://127.0.0.1:{server.port}{path}",
            headers={
                "Origin": UI,
                "Access-Control-Request-Method": "POST",
                "Access-Control-Request-Headers": "content-type,x-namespace",
            },
        )
    assert pre.status_code == 204
    assert pre.headers["Access-Control-Allow-Origin"] == UI
    r = _post(server, path, _body(sealed))
    assert r.status_code == 200, r.text
    assert _payloads(r) == [_plain(MARKER)]
    assert _post(server, "/decode/x?y", _body(sealed)).status_code == 404


def test_encode_seals_to_this_machine_alone(server: CodecServer) -> None:
    r = _post(server, "/encode", _body(_plain(MARKER)))
    assert r.status_code == 200, r.text
    [sealed] = _payloads(r)
    assert sealed.metadata["encoding"] == ENCODING
    assert MARKER.encode() not in sealed.data
    assert Payload.FromString(decrypt(sealed.data, SECRET)) == _plain(MARKER)
    back = _post(server, "/decode", _body(sealed))
    assert _payloads(back) == [_plain(MARKER)]


def test_the_ui_origin_alone_and_its_preflight(server: CodecServer) -> None:
    with httpx.Client(trust_env=False) as http:
        pre = http.options(
            f"http://127.0.0.1:{server.port}/decode",
            headers={
                "Origin": UI,
                "Access-Control-Request-Method": "POST",
                "Access-Control-Request-Headers": "content-type,x-namespace",
            },
        )
        assert pre.status_code == 204
        assert pre.headers["Access-Control-Allow-Origin"] == UI
        assert pre.headers["Access-Control-Allow-Methods"] == "POST, OPTIONS"
        assert set(pre.headers["Access-Control-Allow-Headers"].split(", ")) == {
            "Content-Type",
            "X-Namespace",
        }
        evil = http.options(
            f"http://127.0.0.1:{server.port}/decode", headers={"Origin": "https://evil.example"}
        )
        assert evil.status_code == 403
        assert "Access-Control-Allow-Origin" not in evil.headers
    sealed = _sealed(_plain(MARKER), PUBLIC)
    for origin in ("https://evil.example", "http://localhost:8234", "null"):
        r = _post(server, "/decode", _body(sealed), origin=origin)
        assert r.status_code == 403, origin
        assert MARKER not in r.text
    assert (
        _post(server, "/decode", _body(sealed), origin="http://127.0.0.1:8233").status_code == 200
    )
    # No Origin: not a browser's cross-site request (curl, the CLI's own probe).
    assert _post(server, "/decode", _body(sealed), origin=None).status_code == 200


def test_a_rebound_name_a_form_and_oversized_bodies_are_refused(server: CodecServer) -> None:
    body = _body(_sealed(_plain(MARKER), PUBLIC))
    for host in ("evil.example", f"evil.example:{server.port}", "127.0.0.1:1", "[::1]"):
        r = _post(server, "/decode", body, Host=host)
        assert r.status_code == 403, host
    assert _post(server, "/decode", body, Host=f"localhost:{server.port}").status_code == 200
    form = _post(server, "/decode", body, **{"Content-Type": "text/plain"})
    assert form.status_code == 415
    assert _post(server, "/decode", b"{not json").status_code == 400
    assert _post(server, "/nope", body).status_code == 404
    # A length above the limit is refused before a byte of the body is read.
    with socket.create_connection(("127.0.0.1", server.port), timeout=10) as raw:
        raw.sendall(
            f"POST /decode HTTP/1.1\r\nHost: 127.0.0.1:{server.port}\r\n"
            f"Content-Type: application/json\r\nContent-Length: {MAX_BODY + 1}\r\n\r\n".encode()
        )
        assert raw.recv(64).startswith(b"HTTP/1.0 413")


def test_stop_needs_the_token_and_no_origin(server: CodecServer) -> None:
    assert _post(server, "/stop", b"", origin=None).status_code == 403
    wrong = _post(server, "/stop", b"", origin=None, Authorization="Bearer nope")
    assert wrong.status_code == 403
    page = _post(server, "/stop", b"", Authorization=f"Bearer {TOKEN}")  # from a page: never
    assert page.status_code == 403
    assert _post(server, "/decode", _body()).status_code == 200  # still serving
    ok = _post(server, "/stop", b"", origin=None, Authorization=f"Bearer {TOKEN}")
    assert ok.status_code == 204


def test_ready_answers_its_token_holder_alone(server: CodecServer) -> None:
    def ready(*, origin: str | None = None, **headers: str | bytes) -> int:
        sent = {**headers, **({"Origin": origin} if origin is not None else {})}
        with httpx.Client(trust_env=False) as http:
            return http.get(f"http://127.0.0.1:{server.port}/ready", headers=sent).status_code

    assert ready(Authorization=f"Bearer {TOKEN}") == 204
    assert ready() == 403
    assert ready(Authorization="Bearer nope") == 403
    assert ready(Authorization="Bearer été".encode()) == 403  # not ASCII: refused, not a 500
    assert ready(origin=UI, Authorization=f"Bearer {TOKEN}") == 403  # never from a page
    assert ready(Authorization=f"Bearer {TOKEN}", Host="evil.example") == 403


# --------------------------------------------------------------------------- the process


def _ui_config(codec_port: int, ui_port: int, endpoint: str | None = None) -> dict[str, Any]:
    return {
        "services": {
            "ui": {
                "environment": {
                    "TEMPORAL_CODEC_ENDPOINT": endpoint or f"http://127.0.0.1:{codec_port}"
                },
                "ports": [{"host_ip": "127.0.0.1", "published": str(ui_port), "target": 8080}],
            }
        }
    }


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def test_kontra_dev_starts_and_stops_it_in_the_background(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    home = tmp_path / "home"
    home.mkdir(mode=0o700)
    key = home / KEY_FILE
    key.write_text(SECRET + "\n")
    key.chmod(0o600)
    monkeypatch.setenv("KONTRA_HOME", str(home))
    monkeypatch.delenv(KEY_FILE_ENV, raising=False)
    port = _free_port()
    config = _ui_config(port, 8233)
    url = dev_module._start_codec(home, config)
    try:
        assert url == f"http://127.0.0.1:{port}"
        state = json.loads((home / dev_module.CODEC_STATE).read_text())
        assert stat.S_IMODE((home / dev_module.CODEC_STATE).stat().st_mode) == 0o600
        assert state["port"] == port
        assert len(state["token"]) >= 32
        sealed = _sealed(_plain(MARKER), PUBLIC)
        with httpx.Client(trust_env=False) as http:
            r = http.post(
                f"{url}/decode",
                content=_body(sealed),
                headers={"Content-Type": "application/json", "Origin": UI},
            )
        assert _payloads(r) == [_plain(MARKER)]
        # The graph page, from the same process; Temporal only at the first request that
        # needs it -- this home holds no profile, which that request says.
        with httpx.Client(trust_env=False, timeout=30) as http:
            page = http.get(f"{url}/graph")
            runs = http.get(f"{url}/api/workflows")
        assert page.status_code == 200
        assert "nonce-" in page.headers["Content-Security-Policy"]
        assert runs.status_code == 502
        assert runs.json()["error"].startswith("NotLoggedIn: not logged in")
        # Started again (a second `up --ui`): the first is stopped, one runs.
        assert dev_module._start_codec(home, config) == url
    finally:
        stopped = dev_module._stop_codec(home)
    assert stopped
    assert not (home / dev_module.CODEC_STATE).exists()
    assert not dev_module._codec_running(port, state["token"])
    assert dev_module._stop_codec(home) is False  # nothing left to stop
    log = (home / dev_module.CODEC_LOG).read_text()
    assert MARKER not in log
    assert state["token"] not in log


def _home(path: Path) -> Path:
    path.mkdir(mode=0o700)
    key = path / KEY_FILE
    key.write_text(generate_identity()[0] + "\n")
    key.chmod(0o600)
    return path


def test_another_home_s_server_on_the_port_is_not_taken_for_ours(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Home ``a`` runs a codec server; ``up --ui`` from home ``b``, on the same port, cannot
    bind it. It must say so, not report ``a``'s server (``a``'s key) as its own: the probe
    is ``a``'s token or ``b``'s, not whether a decode answers."""
    from click.exceptions import ClickException

    monkeypatch.delenv(KEY_FILE_ENV, raising=False)
    a, b = _home(tmp_path / "a"), _home(tmp_path / "b")
    port = _free_port()
    config = _ui_config(port, 8233)
    monkeypatch.setenv("KONTRA_HOME", str(a))
    dev_module._start_codec(a, config)
    try:
        theirs = json.loads((a / dev_module.CODEC_STATE).read_text())["token"]
        monkeypatch.setenv("KONTRA_HOME", str(b))
        with pytest.raises(ClickException, match="did not start"):
            dev_module._start_codec(b, config)
        assert not (b / dev_module.CODEC_STATE).exists()
        assert dev_module._stop_codec(b) is False
        assert dev_module._codec_running(port, theirs)  # a's still serves, untouched
    finally:
        assert dev_module._stop_codec(a)


def test_without_a_key_it_says_why(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from click.exceptions import ClickException

    home = tmp_path / "home"
    home.mkdir(mode=0o700)
    monkeypatch.setenv("KONTRA_HOME", str(home))
    monkeypatch.delenv(KEY_FILE_ENV, raising=False)
    with pytest.raises(ClickException, match="no encryption key"):
        dev_module._start_codec(home, _ui_config(_free_port(), 8233))
    assert not (home / dev_module.CODEC_STATE).exists()


def test_only_a_loopback_endpoint_is_served(tmp_path: Path) -> None:
    from click.exceptions import ClickException

    for endpoint in ("http://0.0.0.0:8081", "http://localhost:8081", "https://127.0.0.1:8081"):
        with pytest.raises(ClickException, match=r"127\.0\.0\.1"):
            dev_module._codec_ports(_ui_config(8081, 8233, endpoint))
    assert dev_module._codec_ports(_ui_config(8081, 8234)) == (8081, 8234)


def test_the_page_s_links_are_where_this_up_published_them() -> None:
    config = _ui_config(8081, 8234)
    config["services"]["grafana"] = {
        "ports": [{"host_ip": "127.0.0.1", "published": "3300", "target": 3000}]
    }
    assert dev_module._link_bases(config, observability=True) == [
        *("--temporal-ui", "http://localhost:8234"),
        *("--grafana", "http://localhost:3300"),
    ]
    # Without the observability profile no Grafana runs: the page links none.
    assert dev_module._link_bases(config, observability=False) == [
        "--temporal-ui",
        "http://localhost:8234",
    ]


def test_a_link_base_that_is_not_an_http_url_is_refused(tmp_path: Path) -> None:
    home = _home(tmp_path / "home")
    env = {**os.environ, "KONTRA_HOME": str(home)}
    env.pop(KEY_FILE_ENV, None)
    proc = subprocess.run(
        [
            *(sys.executable, "-m", "kontra.cli._codec_server"),
            *("--port", "0", "--ui-port", "1", "--grafana", "javascript:alert(1)"),
        ],
        input=b"token\n",
        capture_output=True,
        env=env,
        timeout=60,
        check=False,
    )
    assert proc.returncode != 0
    assert b"grafana must be an http(s) base URL" in proc.stderr


def test_the_module_runs_as_kontra_dev_starts_it(tmp_path: Path) -> None:
    """``python -m kontra.cli._codec_server`` with no token on stdin refuses to start."""
    env = {**os.environ, "KONTRA_HOME": str(tmp_path)}
    proc = subprocess.run(
        [sys.executable, "-m", "kontra.cli._codec_server", "--port", "0", "--ui-port", "1"],
        input=b"",
        capture_output=True,
        env=env,
        timeout=60,
        check=False,
    )
    assert proc.returncode != 0
    assert b"stop token" in proc.stderr
