"""The codec server of ``kontra dev up --ui``: Temporal's codec-server protocol on this
machine's loopback, so Temporal's UI shows a seller their own inputs and results.

The UI's browser posts ``{"payloads": [...]}`` (Temporal's ``Payloads`` in proto3 JSON) to
``/decode`` and ``/encode`` and reads the same shape back. Both are
:class:`kontra.codec.KontraCodec`'s, seller role, with this machine's key
(``$KONTRA_KEY_FILE``, else ``$KONTRA_HOME/key``) and the job keys under ``$KONTRA_HOME``:

* decode opens what is encrypted to one of those keys and answers everything else unchanged,
  as Temporal's own codec servers do: another party's payloads (the UI sees every
  namespace), plaintext, and claim-check references, which it does not fetch (it holds no
  gate session);
* encode -- a workflow started or signalled from the UI -- encrypts to this machine's key
  alone, which a worker holding it reads.

Loopback only, and never in production: whoever reaches the port can have this machine's key
decrypt what was encrypted to it. So it listens on 127.0.0.1 and nothing else, and refuses a
request whose ``Host`` is not 127.0.0.1 or ``localhost`` at its port (a web page that
rebinds a name of its own to 127.0.0.1), whose ``Origin`` is not the UI's (the only origin
its CORS headers name), or whose body is not JSON (a cross-site form). It is for a machine
whose loopback is its user's alone.

It also serves the graph page of ``kontra dev up --ui`` (:mod:`kontra.cli._console`, decisions
I28): ``GET /graph`` and its ``/api/...`` routes, for the console's own origin
(``http://127.0.0.1:P``, ``http://localhost:P``) and no other, behind the same ``Host`` check;
``/decode`` and ``/encode`` keep the UI's origin alone, and a preflight is answered for those
two only.

``kontra dev`` runs it as ``python -m kontra.cli._codec_server --port P --ui-port U
[--temporal-ui URL] [--grafana URL]`` in the background (the two URLs are where the page's
links point), handing it a token on standard input. Two requests take that token (as
``Authorization: Bearer``, with no ``Origin``): ``GET /ready`` answers 204, which is how
``kontra dev`` knows the server answering on the port is the one it started and not another
(one started from another ``KONTRA_HOME``, decrypting with that home's key); ``POST /stop``
ends it, which is how ``kontra dev down`` stops it.
"""

from __future__ import annotations

import argparse
import asyncio
import hmac
import sys
import threading
from collections.abc import Callable, Sequence
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import urlsplit

from google.protobuf import json_format
from temporalio.api.common.v1 import Payload, Payloads

from kontra.cli import _console
from kontra.codec import CodecError, KontraCodec
from kontra.codec.age import public_key_of
from kontra.profile import Profile, kontra_home

__all__ = ["HOST", "MAX_BODY", "CodecServer", "main"]

#: The one address it listens on.
HOST = "127.0.0.1"
#: The largest request body read: Temporal's own limit for one payload is 2 MiB, and the UI
#: sends an event's payloads together.
MAX_BODY = 32 * 1024 * 1024
_ALLOWED_HEADERS = "Content-Type, X-Namespace"
_CODEC_ROUTES = ("/decode", "/encode")


class CodecServer(ThreadingHTTPServer):
    """The HTTP server: ``codec`` made per request (job keys come and go), the UI's
    ``origins``, the ``token`` that stops it, and the graph page's ``console`` (None: no
    page)."""

    daemon_threads = True

    def __init__(
        self,
        port: int,
        *,
        codec: Callable[[], KontraCodec],
        origins: Sequence[str],
        token: str,
        console: _console.Console | None = None,
    ) -> None:
        super().__init__((HOST, port), _Handler)
        self.codec = codec
        self.origins = frozenset(origins)
        self.token = token
        self.console = console
        self.port = int(self.server_address[1])
        self.hosts = frozenset({f"{HOST}:{self.port}", f"localhost:{self.port}"})
        #: The page's own origins: the console routes' only ones.
        self.console_origins = frozenset(f"http://{h}" for h in self.hosts)


def _decode_each(codec: KontraCodec, payloads: Sequence[Payload]) -> list[Payload]:
    async def one(p: Payload) -> Payload:
        try:
            [out] = await codec.decode([p])
        except CodecError:  # not encrypted to us, or a claim-check: shown as it is
            return p
        return out

    async def all_of() -> list[Payload]:
        return [await one(p) for p in payloads]

    return asyncio.run(all_of())


class _Handler(BaseHTTPRequestHandler):
    server: CodecServer  # pyright: ignore[reportIncompatibleVariableOverride]

    def log_message(self, format: str, *args: Any) -> None:
        pass  # no request log: a path is all it would say

    # ------------------------------------------------------------------ the checks

    def _origin(self) -> str | None:
        return self.headers.get("Origin")

    def _refuse(self, status: HTTPStatus, why: str) -> None:
        body = why.encode()
        self.send_response(status)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _allowed(self, origins: frozenset[str] | None = None) -> bool:
        """The Host is this server's and the Origin, if any, one of ``origins`` (default:
        the UI's)."""
        if self.headers.get("Host") not in self.server.hosts:
            self._refuse(HTTPStatus.FORBIDDEN, "unexpected Host")
            return False
        origin = self._origin()
        if origin is not None and origin not in (origins or self.server.origins):
            self._refuse(HTTPStatus.FORBIDDEN, "unexpected Origin")
            return False
        return True

    def _cors(self) -> None:
        origin = self._origin()
        if origin is not None:  # checked by _allowed: the UI's
            self.send_header("Access-Control-Allow-Origin", origin)
            self.send_header("Vary", "Origin")

    def _body(self) -> bytes | None:
        try:
            length = int(self.headers.get("Content-Length", ""))
        except ValueError:
            self._refuse(HTTPStatus.LENGTH_REQUIRED, "Content-Length required")
            return None
        if length < 0 or length > MAX_BODY:
            self._refuse(HTTPStatus.REQUEST_ENTITY_TOO_LARGE, f"at most {MAX_BODY} bytes")
            return None
        return self.rfile.read(length)

    # ------------------------------------------------------------------ the methods

    def do_OPTIONS(self) -> None:
        if not self._allowed():
            return
        if urlsplit(self.path).path not in _CODEC_ROUTES:
            # The console's routes are same-origin: a preflight for one is another origin's,
            # which may not send X-Kontra-Console.
            self._refuse(HTTPStatus.FORBIDDEN, "no cross-origin requests here")
            return
        self.send_response(HTTPStatus.NO_CONTENT)
        self._cors()
        self.send_header("Access-Control-Allow-Methods", "POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", _ALLOWED_HEADERS)
        self.send_header("Access-Control-Max-Age", "600")
        self.send_header("Content-Length", "0")
        self.end_headers()

    def _from_kontra_dev(self) -> bool:
        """The request carries the token ``kontra dev`` handed this server, and no Origin."""
        given = self.headers.get("Authorization", "").encode()
        expected = f"Bearer {self.server.token}".encode()
        return self._origin() is None and hmac.compare_digest(given, expected)

    def do_GET(self) -> None:
        url = urlsplit(self.path)
        console = self.server.console
        if console is not None and url.path in _console.ROUTES:
            if self._allowed(self.server.console_origins):
                _console.handle(self, console, url.path, url.query)
            return
        if not self._allowed():
            return
        if self.path != "/ready":
            self._refuse(HTTPStatus.NOT_FOUND, "GET /ready" + (" or /graph" if console else ""))
            return
        if not self._from_kontra_dev():
            self._refuse(HTTPStatus.FORBIDDEN, "not yours")
            return
        self.send_response(HTTPStatus.NO_CONTENT)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_POST(self) -> None:
        if not self._allowed():
            return
        if self.path == "/stop":
            self._stop()
            return
        # Temporal's UI (2.55) asks for /decode?preserveStorageRefs=true: this server never
        # fetches a claim-check reference, so every query is answered alike.
        route = urlsplit(self.path).path
        if route not in _CODEC_ROUTES:
            self._refuse(HTTPStatus.NOT_FOUND, "POST /decode or /encode")
            return
        kind = self.headers.get("Content-Type", "").split(";", 1)[0].strip().lower()
        if kind != "application/json":
            self._refuse(HTTPStatus.UNSUPPORTED_MEDIA_TYPE, "application/json only")
            return
        body = self._body()
        if body is None:
            return
        given = Payloads()
        try:
            json_format.Parse(body, given)
        except (json_format.ParseError, UnicodeDecodeError, ValueError):
            self._refuse(HTTPStatus.BAD_REQUEST, 'expected {"payloads": [...]}')
            return
        codec = self.server.codec()
        try:
            if route == "/decode":
                out = _decode_each(codec, given.payloads)
            else:
                out = asyncio.run(codec.encode(list(given.payloads)))
        except CodecError as e:
            self._refuse(HTTPStatus.UNPROCESSABLE_ENTITY, str(e))
            return
        answer = json_format.MessageToJson(Payloads(payloads=out), indent=None).encode()
        self.send_response(HTTPStatus.OK)
        self._cors()
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(answer)))
        self.end_headers()
        self.wfile.write(answer)

    def _stop(self) -> None:
        if not self._from_kontra_dev():
            self._refuse(HTTPStatus.FORBIDDEN, "not yours to stop")
            return
        self.send_response(HTTPStatus.NO_CONTENT)
        self.send_header("Content-Length", "0")
        self.end_headers()
        threading.Thread(target=self.server.shutdown, daemon=True).start()


def _codec_factory() -> Callable[[], KontraCodec]:
    """This machine's codec, its job keys read again at each use. Raises if there is no
    key here."""
    home = kontra_home()
    identity = Profile.identity(home)
    ours = public_key_of(identity)

    def make() -> KontraCodec:
        return KontraCodec(
            identity,
            [ours],
            role="seller",
            extra_identities=lambda: tuple(Profile.job_identities(home)),
        )

    make()  # a key that does not load fails here, not at the first request
    return make


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="python -m kontra.cli._codec_server")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--ui-port", type=int, required=True)
    parser.add_argument("--temporal-ui", metavar="URL", help="Where the graph page links runs.")
    parser.add_argument("--grafana", metavar="URL", help="Where it links logs and traces.")
    args = parser.parse_args(argv)
    token = sys.stdin.readline().strip()
    if not token:
        sys.exit("a stop token is expected on standard input")
    try:
        codec = _codec_factory()
    except Exception as e:  # no key, an unreadable one: said in the log `kontra dev` shows
        sys.exit(f"cannot load this machine's key: {e}")
    try:
        console = _console.Console(
            kontra_home(), codec, temporal_ui=args.temporal_ui, grafana=args.grafana
        )
    except ValueError as e:  # a link base that is not an http(s) URL
        sys.exit(str(e))
    ui = int(args.ui_port)
    server = CodecServer(
        int(args.port),
        codec=codec,
        origins=[f"http://localhost:{ui}", f"http://{HOST}:{ui}"],
        token=token,
        console=console,
    )
    print(
        f"codec server on http://{HOST}:{server.port} for the UI at :{ui}; "
        f"the graph page at http://{HOST}:{server.port}{_console.PAGE_ROUTE}",
        flush=True,
    )
    try:
        with server:
            server.serve_forever()
    finally:
        console.close()


if __name__ == "__main__":
    main()
