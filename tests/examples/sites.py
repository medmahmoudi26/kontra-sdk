"""Web sites on 127.0.0.1 for the example sellers to fetch: the URLs of the end-to-end tests.

Three servers share one set of pages:

* ``plain``: HTTP.
* ``secure``: HTTPS with a certificate from a throwaway CA, for ``https://`` URLs (the recon
  example only builds those). A seller process trusts the CA through ``SSL_CERT_FILE``,
  which both ``urllib`` and ``httpx`` honour, so nothing in the examples changes for a test.
* ``stall``: HTTPS that answers every request one header line per second until
  :meth:`Sites.release` (or two minutes): no client timeout that applies per network
  operation fires, so a job can be caught while it runs (the enrich seller still gives up
  on a URL after its own overall deadline).

Pages: ``/`` ("Home"), ``/page/<n>`` ("Page <n>"), ``/moved`` (301 to ``/page/moved``),
``/missing`` (404, "Not found"), ``/garbage`` (not HTTP at all) and ``/endless`` (a title,
then bytes until the client hangs up -- or ``ENDLESS_CAP``, which only a client that reads
everything reaches; :attr:`Sites.endless_capped` says whether one did).

Hostile ones: ``/redirect-endless`` (to a GET, a 301 to ``/page/landed`` whose body is
endless like ``/endless``'s; to a HEAD, the bare 301), ``/redirect-twice`` (301 to
``/redirect-endless``: a client that turns a redirected HEAD into a GET meets that body),
``/loop`` (301 to itself) and ``/bomb`` (``Content-Encoding: gzip, gzip``: a few hundred
bytes that inflate to ``BOMB_SIZE``, the title "Bomb" first -- a client that finds the title
inflated it).
"""

from __future__ import annotations

import datetime
import gzip
import html
import ipaddress
import re
import socket
import ssl
import threading
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

from kontra.testing.devserver import free_port

__all__ = ["BOMB_SIZE", "ENDLESS_CAP", "SERVER", "Sites"]

#: The ``Server`` header every page carries.
SERVER = "kontra-test-site"
#: Where ``/endless`` gives up: far beyond what a client that stops reading ever receives.
ENDLESS_CAP = 64 * 1024 * 1024
#: What ``/bomb`` inflates to.
BOMB_SIZE = 16 * 1024 * 1024
_BOMB = gzip.compress(
    gzip.compress(b"<html><head><title>Bomb</title></head>" + bytes(BOMB_SIZE), 9), 9
)
_STALL_MAX = 120.0
_PAGE = re.compile(r"/page/([A-Za-z0-9_-]{1,40})")


@dataclass
class _State:
    release: threading.Event = field(default_factory=threading.Event)
    endless_capped: bool = False
    stalled: int = 0
    lock: threading.Lock = field(default_factory=threading.Lock)


def _handler(state: _State, *, stall: bool) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def version_string(self) -> str:
            return SERVER

        def log_message(self, format: str, *args: Any) -> None:
            pass

        def setup(self) -> None:
            if isinstance(self.request, ssl.SSLSocket):
                self.request.settimeout(10)
                self.request.do_handshake()  # here, on this connection's thread
            super().setup()

        def do_GET(self) -> None:
            self._answer(body=True)

        def do_HEAD(self) -> None:
            self._answer(body=False)

        def _answer(self, *, body: bool) -> None:
            if stall:
                return self._stall(body)
            path = self.path.split("?", 1)[0]
            page = _PAGE.fullmatch(path)
            if path == "/":
                return self._page(200, "Home", body)
            if page is not None:
                return self._page(200, f"Page {page.group(1)}", body)
            if path == "/moved":
                return self._moved("/page/moved")
            if path == "/garbage":
                self.wfile.write(b"NONSENSE\r\n\r\n")
                self.close_connection = True
                return None
            if path == "/redirect-endless":
                landed = "/page/landed"
                return self._endless(redirect=landed) if body else self._moved(landed)
            if path in ("/redirect-twice", "/loop"):
                return self._moved("/redirect-endless" if path == "/redirect-twice" else path)
            if path == "/bomb":
                return self._bomb(body)
            if path == "/endless" and body:
                return self._endless()
            if path == "/endless":
                return self._page(200, "Endless", body=False)
            return self._page(404, "Not found", body)

        def _stall(self, body: bool) -> None:
            with state.lock:
                state.stalled += 1
            data = b"<html><head><title>Late</title></head></html>\n"
            try:
                self.wfile.write(b"HTTP/1.1 200 OK\r\n")
                waited = 0.0
                while not state.release.wait(1.0) and waited < _STALL_MAX:
                    waited += 1.0
                    self.wfile.write(f"X-Stall: {waited:.0f}\r\n".encode())
                head = f"Content-Type: text/html\r\nContent-Length: {len(data)}\r\n\r\n"
                self.wfile.write(head.encode() + (data if body else b""))
            except OSError:  # the client gave up
                self.close_connection = True

        def _page(self, status: int, title: str, body: bool) -> None:
            data = f"<html><head><title>{html.escape(title)}</title></head></html>\n".encode()
            self.send_response(status)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            if body:
                self.wfile.write(data)

        def _moved(self, location: str) -> None:
            self.send_response(301)
            self.send_header("Location", location)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def _bomb(self, body: bool) -> None:
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.send_header("Content-Encoding", "gzip, gzip")
            self.send_header("Content-Length", str(len(_BOMB)))
            self.end_headers()
            if body:
                self.wfile.write(_BOMB)

        def _endless(self, redirect: str | None = None) -> None:
            if redirect is None:
                self.send_response(200)
            else:
                self.send_response(301)
                self.send_header("Location", redirect)
            self.send_header("Content-Type", "text/html")
            self.send_header("Connection", "close")
            self.end_headers()
            self.close_connection = True
            sent = 0
            chunk = b"x" * 65536
            try:
                self.wfile.write(b"<html><head><title>Endless</title></head><body>")
                while sent < ENDLESS_CAP:
                    self.wfile.write(chunk)
                    sent += len(chunk)
            except OSError:  # the client hung up: what a careful client does
                return
            state.endless_capped = True

    return Handler


class _Server(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, handler: type[BaseHTTPRequestHandler], tls: ssl.SSLContext | None) -> None:
        self.tls = tls
        super().__init__(("127.0.0.1", free_port()), handler)

    def get_request(self) -> tuple[socket.socket, Any]:
        sock, addr = super().get_request()
        if self.tls is not None:  # the handshake runs on the connection's thread, not here
            sock = self.tls.wrap_socket(sock, server_side=True, do_handshake_on_connect=False)
        return sock, addr

    def handle_error(self, request: Any, client_address: Any) -> None:
        pass  # clients that give up, time out or refuse the certificate: not test failures


def _write_ca_and_leaf(directory: Path) -> tuple[Path, Path, Path]:
    """A throwaway CA and a server certificate for 127.0.0.1 and localhost, with the
    extensions a strict verifier wants. Returns (ca.pem, server.pem, server.key)."""
    now = datetime.datetime.now(datetime.UTC)
    ca_key = ec.generate_private_key(ec.SECP256R1())
    ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "kontra test CA")])
    ca = (
        x509.CertificateBuilder()
        .subject_name(ca_name)
        .issuer_name(ca_name)
        .public_key(ca_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(minutes=5))
        .not_valid_after(now + datetime.timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
        .add_extension(
            x509.KeyUsage(
                digital_signature=True,
                content_commitment=False,
                key_encipherment=False,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=True,
                crl_sign=True,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .add_extension(x509.SubjectKeyIdentifier.from_public_key(ca_key.public_key()), False)
        .sign(ca_key, hashes.SHA256())
    )
    key = ec.generate_private_key(ec.SECP256R1())
    leaf = (
        x509.CertificateBuilder()
        .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "127.0.0.1")]))
        .issuer_name(ca_name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(minutes=5))
        .not_valid_after(now + datetime.timedelta(days=1))
        .add_extension(
            x509.SubjectAlternativeName(
                [x509.IPAddress(ipaddress.ip_address("127.0.0.1")), x509.DNSName("localhost")]
            ),
            critical=False,
        )
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]), critical=False)
        .add_extension(
            x509.AuthorityKeyIdentifier.from_issuer_public_key(ca_key.public_key()), False
        )
        .sign(ca_key, hashes.SHA256())
    )
    ca_pem, leaf_pem, key_pem = (
        directory / "ca.pem",
        directory / "server.pem",
        directory / "server.key",
    )
    ca_pem.write_bytes(ca.public_bytes(serialization.Encoding.PEM))
    leaf_pem.write_bytes(leaf.public_bytes(serialization.Encoding.PEM))
    key_pem.write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    return ca_pem, leaf_pem, key_pem


class Sites:
    """The three servers, running on daemon threads. Build with :meth:`start`."""

    def __init__(self, directory: Path) -> None:
        self.ca_file, cert, key = _write_ca_and_leaf(directory)
        tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        tls.load_cert_chain(cert, key)
        self._state = _State()
        self._servers = {
            "plain": _Server(_handler(self._state, stall=False), None),
            "secure": _Server(_handler(self._state, stall=False), tls),
            "stall": _Server(_handler(self._state, stall=True), tls),
        }
        self._threads: list[threading.Thread] = []

    @classmethod
    def start(cls, directory: Path) -> Sites:
        sites = cls(directory)
        for name, server in sites._servers.items():
            thread = threading.Thread(target=server.serve_forever, name=f"site-{name}", daemon=True)
            thread.start()
            sites._threads.append(thread)
        return sites

    def stop(self) -> None:
        self.release()
        for server in self._servers.values():
            server.shutdown()
            server.server_close()
        for thread in self._threads:
            thread.join(timeout=10)

    # ------------------------------------------------------------------ addresses

    def _port(self, name: str) -> int:
        return int(self._servers[name].server_address[1])

    def host(self, name: str = "plain") -> str:
        """``127.0.0.1:<port>`` of a server: a probe target's ``host``, a recon domain."""
        return f"127.0.0.1:{self._port(name)}"

    def plain(self, path: str = "/") -> str:
        return f"http://{self.host('plain')}{path}"

    def secure(self, path: str = "/") -> str:
        return f"https://{self.host('secure')}{path}"

    def stall(self, path: str = "/") -> str:
        return f"https://{self.host('stall')}{path}"

    @staticmethod
    def refused() -> str:
        """A URL nothing listens on: connection refused."""
        return f"http://127.0.0.1:{free_port()}/"

    # ------------------------------------------------------------------ state

    def release(self) -> None:
        """Let every stalled request (and every later one) answer."""
        self._state.release.set()

    def rearm(self) -> None:
        """Stall again from now on."""
        self._state.release.clear()

    @property
    def stalled(self) -> int:
        """Requests the stall server has received."""
        return self._state.stalled

    @property
    def endless_capped(self) -> bool:
        """Whether some client read ``/endless`` all the way to the cap."""
        return self._state.endless_capped
