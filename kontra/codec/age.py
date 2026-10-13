"""age v1 (https://age-encryption.org/v1) for X25519 recipients, streaming, in constant memory.

Why not just ``pyrage``: its ``encrypt_io``/``decrypt_io`` are constant-memory (measured: +2 MiB
RSS over 512 MiB), but rage adds a random-length *grease* stanza to every header (measured
251-335 header bytes for one recipient), so the ciphertext size of a file cannot be known
before it is encrypted. The gate must know that size up front to refuse an output above
``max_output_bytes`` before a byte is stored (acceptance 15), and the multipart plan needs it.
rage's API is also a blocking reader/writer pair, while an upload wants ciphertext pushed
into async HTTP part bodies. So this module writes the format itself (with ``cryptography``),
deterministic in size, and reads anything a conforming implementation writes, grease
included; ``tests/codec/test_age.py`` proves interoperability with pyrage both ways.

Format summary (all of it is here, nothing else is accepted):

* header: ``age-encryption.org/v1\\n``, one stanza per recipient
  (``-> X25519 <ephemeral share>\\n<wrapped file key>\\n``), then ``--- <HMAC-SHA256>\\n``
  over everything up to and including ``---``;
* a 16-byte nonce; payload key = HKDF-SHA256(file key, salt=nonce, info="payload");
* ChaCha20-Poly1305 STREAM: 64 KiB plaintext chunks, nonce = 11-byte big-endian counter
  || last-chunk flag, so truncation and reordering are detected.

Keys are Bech32: identities ``AGE-SECRET-KEY-1...`` and recipients ``age1...``, the same
strings ``age``/``rage``/``pyrage`` use, so a party can decrypt a delivery with the ``age``
CLI during a dispute (decisions I7).
"""

from __future__ import annotations

import base64
import hmac
import os
import re
from collections.abc import AsyncIterable, AsyncIterator, Iterable, Sequence
from dataclasses import dataclass
from hashlib import sha256
from typing import BinaryIO

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric.x25519 import X25519PrivateKey, X25519PublicKey
from cryptography.hazmat.primitives.ciphers.aead import ChaCha20Poly1305
from cryptography.hazmat.primitives.kdf.hkdf import HKDF

__all__ = [
    "CHUNK_SIZE",
    "MAX_HEADER_SIZE",
    "AgeError",
    "ChunkSealer",
    "Decryptor",
    "Encryptor",
    "ciphertext_size",
    "decrypt",
    "decrypt_iter",
    "decrypt_stream",
    "encrypt",
    "encrypt_iter",
    "encrypt_stream",
    "generate_identity",
    "header_size",
    "is_identity",
    "is_recipient",
    "new_header",
    "public_key_of",
    "recipient_count",
]

#: Plaintext bytes per STREAM chunk (fixed by the format).
CHUNK_SIZE = 64 * 1024
_TAG = 16
#: Ciphertext bytes per full chunk.
_CT_CHUNK = CHUNK_SIZE + _TAG
_NONCE = 16
_FILE_KEY = 16
#: A header larger than this is refused; it bounds what a decryptor buffers before the payload.
MAX_HEADER_SIZE = 64 * 1024
#: Read size for the stream helpers: big enough to amortise call overhead, small enough to
#: keep memory flat.
_IO_BLOCK = 1024 * 1024

_VERSION_LINE = b"age-encryption.org/v1"
_X25519_TAG = b"X25519"
_X25519_INFO = b"age-encryption.org/v1/X25519"
_ZERO_NONCE = bytes(12)
# Our own stanzas and MAC line have a fixed size, which is what makes the ciphertext size
# computable before encryption: "-> X25519 " + 43 + "\n" + 43 + "\n", and "--- " + 43 + "\n".
_STANZA_SIZE = 10 + 43 + 1 + 43 + 1
_HEADER_FIXED = len(_VERSION_LINE) + 1 + 4 + 43 + 1 + _NONCE

_BECH32_CHARSET = "qpzry9x8gf2tvdw0s3jn54khce6mua7l"
_RECIPIENT_HRP = "age"
_IDENTITY_HRP = "age-secret-key-"
# 32 bytes -> 52 data characters + 6 checksum characters.
_RECIPIENT_RE = re.compile(r"age1[02-9ac-hj-np-z]{58}", re.ASCII)
_IDENTITY_RE = re.compile(r"AGE-SECRET-KEY-1[02-9AC-HJ-NP-Z]{58}", re.ASCII)
_B64_RE = re.compile(rb"[A-Za-z0-9+/]*", re.ASCII)


class AgeError(ValueError):
    """Malformed input, a key that does not parse, no matching identity, or a failed
    authentication check (header MAC, a chunk's tag, truncation, trailing data)."""


# --------------------------------------------------------------------------- bech32 keys


def _polymod(values: Iterable[int]) -> int:
    gen = (0x3B6A57B2, 0x26508E6D, 0x1EA119FA, 0x3D4233DD, 0x2A1462B3)
    chk = 1
    for v in values:
        top = chk >> 25
        chk = ((chk & 0x1FFFFFF) << 5) ^ v
        for i in range(5):
            if (top >> i) & 1:
                chk ^= gen[i]
    return chk


def _hrp_expand(hrp: str) -> list[int]:
    return [ord(c) >> 5 for c in hrp] + [0] + [ord(c) & 31 for c in hrp]


def _convert_bits(data: Iterable[int], frombits: int, tobits: int, *, pad: bool) -> list[int]:
    acc = bits = 0
    out: list[int] = []
    maxv = (1 << tobits) - 1
    for value in data:
        acc = (acc << frombits) | value
        bits += frombits
        while bits >= tobits:
            bits -= tobits
            out.append((acc >> bits) & maxv)
    if pad:
        if bits:
            out.append((acc << (tobits - bits)) & maxv)
    elif bits >= frombits or (acc << (tobits - bits)) & maxv:
        raise AgeError("invalid key encoding (non-zero padding)")
    return out


def _bech32_encode(hrp: str, data: bytes) -> str:
    five = _convert_bits(data, 8, 5, pad=True)
    poly = _polymod([*_hrp_expand(hrp), *five, 0, 0, 0, 0, 0, 0]) ^ 1
    checksum = [(poly >> 5 * (5 - i)) & 31 for i in range(6)]
    return hrp + "1" + "".join(_BECH32_CHARSET[d] for d in five + checksum)


def _bech32_decode(value: str, hrp: str) -> bytes:
    # The caller has already full-matched the shape (hrp, charset, length), so this only
    # checks the checksum and the padding.
    data = [_BECH32_CHARSET.index(c) for c in value[len(hrp) + 1 :]]
    if _polymod([*_hrp_expand(hrp), *data]) != 1:
        raise AgeError("invalid key checksum")
    raw = bytes(_convert_bits(data[:-6], 5, 8, pad=False))
    if len(raw) != 32:
        raise AgeError("invalid key length")
    return raw


def is_recipient(value: object) -> bool:
    """True for a well-formed ``age1...`` X25519 recipient (checksum included)."""
    try:
        _parse_recipient(value)
    except AgeError:
        return False
    return True


def is_identity(value: object) -> bool:
    """True for a well-formed ``AGE-SECRET-KEY-1...`` X25519 identity."""
    try:
        _parse_identity(value)
    except AgeError:
        return False
    return True


def _parse_recipient(value: object) -> bytes:
    if not isinstance(value, str) or len(value) != 62 or not _RECIPIENT_RE.fullmatch(value):
        raise AgeError("not an age X25519 recipient (age1...)")
    return _bech32_decode(value, _RECIPIENT_HRP)


def _parse_identity(value: object) -> X25519PrivateKey:
    if not isinstance(value, str) or len(value) != 74 or not _IDENTITY_RE.fullmatch(value):
        raise AgeError("not an age X25519 identity (AGE-SECRET-KEY-1...)")
    return X25519PrivateKey.from_private_bytes(_bech32_decode(value.lower(), _IDENTITY_HRP))


def _recipient_str(public: bytes) -> str:
    return _bech32_encode(_RECIPIENT_HRP, public)


def generate_identity() -> tuple[str, str]:
    """A fresh X25519 keypair: ``("AGE-SECRET-KEY-1...", "age1...")``."""
    key = X25519PrivateKey.generate()
    secret = _bech32_encode(_IDENTITY_HRP, key.private_bytes_raw()).upper()
    return secret, _recipient_str(key.public_key().public_bytes_raw())


def public_key_of(identity: str) -> str:
    """The ``age1...`` recipient of an ``AGE-SECRET-KEY-1...`` identity."""
    return _recipient_str(_parse_identity(identity).public_key().public_bytes_raw())


# --------------------------------------------------------------------------- primitives


def _b64(data: bytes) -> bytes:
    return base64.b64encode(data).rstrip(b"=")


def _unb64(data: bytes) -> bytes:
    """Canonical unpadded standard base64, as the spec requires (no padding, no stray bits)."""
    if len(data) % 4 == 1 or not _B64_RE.fullmatch(data):
        raise AgeError("malformed header: bad base64")
    out = base64.b64decode(data + b"=" * (-len(data) % 4), validate=True)
    if _b64(out) != data:
        raise AgeError("malformed header: non-canonical base64")
    return out


def _hkdf(ikm: bytes, salt: bytes, info: bytes) -> bytes:
    return HKDF(algorithm=hashes.SHA256(), length=32, salt=salt, info=info).derive(ikm)


def _exchange(private: X25519PrivateKey, public: bytes) -> bytes:
    try:
        shared = private.exchange(X25519PublicKey.from_public_bytes(public))
    except ValueError as e:  # low-order point
        raise AgeError("X25519 exchange produced an all-zero secret") from e
    if not any(shared):
        raise AgeError("X25519 exchange produced an all-zero secret")
    return shared


def _header_mac(file_key: bytes, header_through_dashes: bytes) -> bytes:
    key = _hkdf(file_key, b"", b"header")
    return hmac.new(key, header_through_dashes, sha256).digest()


def header_size(recipients: int) -> int:
    """Bytes before the first payload chunk (nonce included) in what this module writes."""
    if recipients < 1:
        raise AgeError("at least one recipient is required")
    return _HEADER_FIXED + _STANZA_SIZE * recipients


def ciphertext_size(plaintext_size: int, recipients: int) -> int:
    """Exact size of :func:`encrypt` output for ``plaintext_size`` bytes and ``recipients``
    distinct recipients. Lets the gate enforce a size limit before anything is uploaded."""
    if plaintext_size < 0:
        raise AgeError("size must be >= 0")
    chunks = max(1, -(-plaintext_size // CHUNK_SIZE))
    return header_size(recipients) + plaintext_size + _TAG * chunks


class ChunkSealer:
    """The payload half of a file: seals or opens STREAM chunk ``index`` independently.

    Chunks are independent given the payload key, so a multipart upload can regenerate the
    ciphertext of any byte range (a retried part) without re-encrypting from the start.
    """

    __slots__ = ("_aead",)

    def __init__(self, file_key: bytes, nonce: bytes) -> None:
        self._aead = ChaCha20Poly1305(_hkdf(file_key, nonce, b"payload"))

    @staticmethod
    def _nonce(index: int, last: bool) -> bytes:
        return index.to_bytes(11, "big") + (b"\x01" if last else b"\x00")

    def seal(self, index: int, plaintext: bytes | memoryview, *, last: bool) -> bytes:
        if len(plaintext) > CHUNK_SIZE or (not last and len(plaintext) != CHUNK_SIZE):
            raise AgeError("a chunk is 64 KiB, except the last")
        if last and not plaintext and index:
            raise AgeError("only an empty file has an empty last chunk")
        return self._aead.encrypt(self._nonce(index, last), plaintext, None)

    def open(self, index: int, ciphertext: bytes | memoryview, *, last: bool) -> bytes:
        try:
            out = self._aead.decrypt(self._nonce(index, last), ciphertext, None)
        except InvalidTag:
            raise AgeError(
                "payload authentication failed (tampered, truncated or wrong length)"
            ) from None
        if last and not out and index:
            raise AgeError("payload authentication failed (empty last chunk)")
        return out


def _distinct(recipients: Iterable[str]) -> list[bytes]:
    out: list[bytes] = []
    for r in recipients:
        raw = _parse_recipient(r)
        if raw not in out:
            out.append(raw)
    if not out:
        raise AgeError("at least one recipient is required")
    return out


def recipient_count(recipients: Iterable[str]) -> int:
    """How many stanzas :func:`new_header` writes for ``recipients`` (duplicates collapse)."""
    return len(_distinct(recipients))


def new_header(recipients: Iterable[str]) -> tuple[bytes, ChunkSealer]:
    """Start a file: the header (nonce included, exactly :func:`header_size` bytes) and the
    sealer for its payload. Duplicate recipients collapse into one stanza."""
    publics = _distinct(recipients)
    file_key = os.urandom(_FILE_KEY)
    parts = [_VERSION_LINE, b"\n"]
    for public in publics:
        eph = X25519PrivateKey.generate()
        share = eph.public_key().public_bytes_raw()
        wrap_key = _hkdf(_exchange(eph, public), share + public, _X25519_INFO)
        body = ChaCha20Poly1305(wrap_key).encrypt(_ZERO_NONCE, file_key, None)
        parts += [b"-> ", _X25519_TAG, b" ", _b64(share), b"\n", _b64(body), b"\n"]
    parts.append(b"---")
    through_dashes = b"".join(parts)
    nonce = os.urandom(_NONCE)
    header = through_dashes + b" " + _b64(_header_mac(file_key, through_dashes)) + b"\n" + nonce
    return header, ChunkSealer(file_key, nonce)


# --------------------------------------------------------------------------- header parsing


@dataclass(frozen=True, slots=True)
class _Stanza:
    args: list[bytes]
    body: bytes


def _vchar(arg: bytes) -> bool:
    return bool(arg) and all(0x21 <= b <= 0x7E for b in arg)


def _parse_header(buf: bytes | bytearray) -> tuple[list[_Stanza], bytes, bytes, int] | None:
    """``(stanzas, header through '---', mac, payload offset)``, or None if ``buf`` does not
    hold the whole header and nonce yet."""
    pos = 0

    def line() -> bytes | None:
        nonlocal pos
        end = buf.find(b"\n", pos)
        if end < 0:
            if len(buf) > MAX_HEADER_SIZE:
                raise AgeError("header too large")
            return None
        if end > MAX_HEADER_SIZE:
            raise AgeError("header too large")
        out = bytes(buf[pos:end])
        pos = end + 1
        return out

    head = bytes(buf[: len(_VERSION_LINE) + 1])
    if not (_VERSION_LINE + b"\n").startswith(head):
        raise AgeError("not an age v1 file (armored or another format)")
    first = line()
    if first is None:
        return None
    if first != _VERSION_LINE:
        raise AgeError("not an age v1 file (armored or another format)")
    stanzas: list[_Stanza] = []
    while True:
        start = pos
        cur = line()
        if cur is None:
            return None
        if cur.startswith(b"---"):
            fields = cur.split(b" ")
            if len(fields) != 2 or fields[0] != b"---":
                raise AgeError("malformed header MAC line")
            mac = _unb64(fields[1])
            if len(mac) != 32:
                raise AgeError("malformed header MAC line")
            if not stanzas:
                raise AgeError("header has no recipient stanza")
            if len(buf) < pos + _NONCE:
                return None
            return stanzas, bytes(buf[: start + 3]), mac, pos
        if not cur.startswith(b"-> "):
            raise AgeError("malformed header: expected a stanza")
        args = cur[3:].split(b" ")
        if not all(_vchar(a) for a in args):
            raise AgeError("malformed stanza arguments")
        body_lines: list[bytes] = []
        while True:
            body_line = line()
            if body_line is None:
                return None
            if len(body_line) > 64:
                raise AgeError("malformed stanza body")
            body_lines.append(body_line)
            if len(body_line) < 64:
                break
        stanzas.append(_Stanza(args, _unb64(b"".join(body_lines))))


def _identities(identity: str | Sequence[str]) -> list[X25519PrivateKey]:
    items = [identity] if isinstance(identity, str) else list(identity)
    if not items:
        raise AgeError("at least one identity is required")
    return [_parse_identity(i) for i in items]


def _unwrap(stanzas: list[_Stanza], identities: list[X25519PrivateKey]) -> bytes:
    for st in stanzas:
        if st.args[0] != _X25519_TAG:
            continue  # another recipient type, or grease
        if len(st.args) != 2:
            raise AgeError("malformed X25519 stanza")
        share = _unb64(st.args[1])
        if len(share) != 32 or len(st.body) != _FILE_KEY + _TAG:
            raise AgeError("malformed X25519 stanza")
        for ident in identities:
            ours = ident.public_key().public_bytes_raw()
            wrap_key = _hkdf(_exchange(ident, share), share + ours, _X25519_INFO)
            try:
                return ChaCha20Poly1305(wrap_key).decrypt(_ZERO_NONCE, st.body, None)
            except InvalidTag:
                continue
    raise AgeError("no identity matched any recipient (not encrypted to this key)")


# --------------------------------------------------------------------------- incremental


class Encryptor:
    """Incremental encryption: ``update(plaintext) -> ciphertext`` then ``finalize()``.

    Holds at most one chunk of plaintext: a full chunk is only released once more input
    proves it is not the last one (the last chunk carries a flag in its nonce).
    """

    def __init__(self, recipients: Iterable[str]) -> None:
        self.header, self._sealer = new_header(recipients)
        self._buf = bytearray()
        self._index = 0
        self._emitted_header = False
        self._done = False

    def _start(self) -> bytearray:
        if self._done:
            raise AgeError("encryptor already finalized")
        out = bytearray()
        if not self._emitted_header:
            out += self.header
            self._emitted_header = True
        return out

    def update(self, data: bytes | bytearray | memoryview) -> bytes:
        out = self._start()
        self._buf += data
        pos = 0
        with memoryview(self._buf) as view:
            while len(self._buf) - pos > CHUNK_SIZE:
                out += self._sealer.seal(self._index, view[pos : pos + CHUNK_SIZE], last=False)
                self._index += 1
                pos += CHUNK_SIZE
        del self._buf[:pos]
        return bytes(out)

    def finalize(self) -> bytes:
        out = self._start()
        out += self._sealer.seal(self._index, bytes(self._buf), last=True)
        self._buf.clear()
        self._done = True
        return bytes(out)


class Decryptor:
    """Incremental decryption: ``update(ciphertext) -> plaintext`` then ``finalize()``.

    Plaintext of a chunk is released once that chunk authenticates, before the end of the
    stream does: whoever consumes it must discard everything if ``finalize`` raises
    (``kontra.artifacts`` writes to a temporary file and renames only after it succeeds).
    """

    def __init__(self, identity: str | Sequence[str]) -> None:
        self._identities = _identities(identity)
        self._buf = bytearray()
        self._sealer: ChunkSealer | None = None
        self._index = 0
        self._done = False

    def _header(self) -> bool:
        parsed = _parse_header(self._buf)
        if parsed is None:
            return False
        stanzas, through_dashes, mac, end = parsed
        file_key = _unwrap(stanzas, self._identities)
        if not hmac.compare_digest(_header_mac(file_key, through_dashes), mac):
            raise AgeError("header MAC mismatch (tampered header)")
        self._sealer = ChunkSealer(file_key, bytes(self._buf[end : end + _NONCE]))
        del self._buf[: end + _NONCE]
        return True

    def update(self, data: bytes | bytearray | memoryview) -> bytes:
        if self._done:
            raise AgeError("decryptor already finalized")
        self._buf += data
        if self._sealer is None and not self._header():
            return b""
        sealer = self._sealer
        assert sealer is not None
        out = bytearray()
        pos = 0
        with memoryview(self._buf) as view:
            # A full chunk followed by more bytes is not the last one.
            while len(self._buf) - pos > _CT_CHUNK:
                out += sealer.open(self._index, view[pos : pos + _CT_CHUNK], last=False)
                self._index += 1
                pos += _CT_CHUNK
        del self._buf[:pos]
        return bytes(out)

    def finalize(self) -> bytes:
        if self._done:
            raise AgeError("decryptor already finalized")
        self._done = True
        if self._sealer is None:
            raise AgeError("truncated: the header is incomplete")
        if len(self._buf) < _TAG:
            raise AgeError("truncated: the last chunk is missing")
        out = self._sealer.open(self._index, bytes(self._buf), last=True)
        self._buf.clear()
        return out


# --------------------------------------------------------------------------- one-shot & streams


def encrypt(plaintext: bytes, recipients: Iterable[str]) -> bytes:
    enc = Encryptor(recipients)
    return enc.update(plaintext) + enc.finalize()


def decrypt(ciphertext: bytes, identity: str | Sequence[str]) -> bytes:
    dec = Decryptor(identity)
    return dec.update(ciphertext) + dec.finalize()


def encrypt_stream(src: BinaryIO, dst: BinaryIO, recipients: Iterable[str]) -> int:
    """Encrypt ``src`` into ``dst`` in constant memory. Returns ciphertext bytes written."""
    enc = Encryptor(recipients)
    written = 0
    while block := src.read(_IO_BLOCK):
        written += dst.write(enc.update(block))
    return written + dst.write(enc.finalize())


def decrypt_stream(src: BinaryIO, dst: BinaryIO, identity: str | Sequence[str]) -> int:
    """Decrypt ``src`` into ``dst`` in constant memory. Returns plaintext bytes written.
    If it raises, ``dst`` holds unauthenticated partial output and must be discarded."""
    dec = Decryptor(identity)
    written = 0
    while block := src.read(_IO_BLOCK):
        written += dst.write(dec.update(block))
    return written + dst.write(dec.finalize())


async def encrypt_iter(
    source: AsyncIterable[bytes], recipients: Iterable[str]
) -> AsyncIterator[bytes]:
    """Ciphertext as plaintext arrives: the header first, then sealed chunks."""
    enc = Encryptor(recipients)
    async for block in source:
        if out := enc.update(block):
            yield out
    yield enc.finalize()


async def decrypt_iter(
    source: AsyncIterable[bytes], identity: str | Sequence[str]
) -> AsyncIterator[bytes]:
    """Plaintext as ciphertext arrives. Raises :class:`AgeError` at the end if the stream
    is truncated; everything yielded before that must then be discarded."""
    dec = Decryptor(identity)
    async for block in source:
        if out := dec.update(block):
            yield out
    if out := dec.finalize():
        yield out
