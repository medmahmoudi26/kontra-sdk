"""kontra.codec.age: keys, exact sizes, streaming, interoperability with rage (pyrage) in both
directions, and refusal of every malformed, tampered or truncated input."""

from __future__ import annotations

import io
import os
import random
from collections.abc import AsyncIterator

import pyrage
import pytest
from pyrage import x25519

from kontra.codec import age
from kontra.codec.age import (
    CHUNK_SIZE,
    AgeError,
    Decryptor,
    Encryptor,
    ciphertext_size,
    decrypt,
    decrypt_iter,
    decrypt_stream,
    encrypt,
    encrypt_iter,
    encrypt_stream,
    generate_identity,
    header_size,
    public_key_of,
)

SIZES = [0, 1, 15, 16, 17, CHUNK_SIZE - 1, CHUNK_SIZE, CHUNK_SIZE + 1, 2 * CHUNK_SIZE, 300_001]


@pytest.fixture(scope="module")
def alice() -> tuple[str, str]:
    return generate_identity()


@pytest.fixture(scope="module")
def bob() -> tuple[str, str]:
    return generate_identity()


# --------------------------------------------------------------------------- keys


def test_keys_match_rage() -> None:
    secret, public = generate_identity()
    assert secret.startswith("AGE-SECRET-KEY-1")
    assert public.startswith("age1")
    assert len(secret) == 74
    assert len(public) == 62
    assert public_key_of(secret) == public
    # rage parses ours and derives the same recipient...
    assert str(x25519.Identity.from_str(secret).to_public()) == public
    # ...and ours parses rage's.
    theirs = x25519.Identity.generate()
    assert public_key_of(str(theirs)) == str(theirs.to_public())
    assert age.is_identity(secret)
    assert age.is_recipient(public)


@pytest.mark.parametrize(
    "mutate",
    [
        lambda s: s + "\n",
        lambda s: " " + s,
        lambda s: s.lower(),
        lambda s: s[:-1] + ("Q" if s[-1] != "Q" else "P"),  # checksum
        lambda s: s[:20] + s[21:],  # length
        lambda s: "",
    ],
)
def test_bad_identity_strings(mutate: object) -> None:
    secret, _ = generate_identity()
    bad = mutate(secret)  # type: ignore[operator]
    assert not age.is_identity(bad)
    with pytest.raises(AgeError):
        public_key_of(bad)


@pytest.mark.parametrize(
    "mutate",
    [
        lambda s: s + "\n",
        lambda s: s.upper(),
        lambda s: s[:-1] + ("q" if s[-1] != "q" else "p"),
        lambda s: "agf1" + s[4:],
        lambda s: s[:-2],
    ],
)
def test_bad_recipient_strings(mutate: object) -> None:
    _, public = generate_identity()
    bad = mutate(public)  # type: ignore[operator]
    assert not age.is_recipient(bad)
    with pytest.raises(AgeError):
        encrypt(b"x", [bad])


def test_non_string_keys_are_refused() -> None:
    assert not age.is_recipient(None)
    assert not age.is_recipient(b"age1")
    assert not age.is_identity(42)


def test_low_order_recipient_is_refused() -> None:
    # The all-zero point is a valid encoding but yields an all-zero shared secret.
    zero = age._bech32_encode("age", bytes(32))
    with pytest.raises(AgeError, match="all-zero"):
        encrypt(b"x", [zero])


# --------------------------------------------------------------------------- sizes & roundtrip


@pytest.mark.parametrize("size", SIZES)
@pytest.mark.parametrize("recipients", [1, 2, 3])
def test_exact_ciphertext_size(size: int, recipients: int) -> None:
    keys = [generate_identity() for _ in range(recipients)]
    data = os.urandom(size)
    ct = encrypt(data, [k[1] for k in keys])
    assert len(ct) == ciphertext_size(size, recipients)
    assert ct[: header_size(recipients)].count(b"\n-> X25519 ") == recipients
    for secret, _ in keys:
        assert decrypt(ct, secret) == data


def test_duplicate_recipients_collapse(alice: tuple[str, str]) -> None:
    ct = encrypt(b"hello", [alice[1], alice[1]])
    assert len(ct) == ciphertext_size(5, 1)
    assert age.recipient_count([alice[1], alice[1]]) == 1


def test_multiple_identities(alice: tuple[str, str], bob: tuple[str, str]) -> None:
    ct = encrypt(b"rotate", [bob[1]])
    assert decrypt(ct, [alice[0], bob[0]]) == b"rotate"
    with pytest.raises(AgeError):
        decrypt(ct, [])


@pytest.mark.parametrize("size", SIZES)
def test_incremental_with_random_feeds(size: int, alice: tuple[str, str]) -> None:
    rng = random.Random(size)
    data = os.urandom(size)
    enc = Encryptor([alice[1]])
    ct = bytearray()
    pos = 0
    while pos < size:
        n = rng.choice([1, 7, 1000, CHUNK_SIZE, CHUNK_SIZE + 3, 200_000])
        ct += enc.update(data[pos : pos + n])
        pos += n
    ct += enc.finalize()
    assert len(ct) == ciphertext_size(size, 1)
    dec = Decryptor(alice[0])
    out = bytearray()
    pos = 0
    while pos < len(ct):
        n = rng.choice([1, 13, 100, CHUNK_SIZE + 16, 100_000])
        out += dec.update(ct[pos : pos + n])
        pos += n
    out += dec.finalize()
    assert out == data


def test_encryptor_holds_at_most_one_chunk(alice: tuple[str, str]) -> None:
    """Constant memory: output keeps up with input, lagging by at most one chunk."""
    enc = Encryptor([alice[1]])
    fed = emitted = 0
    for _ in range(64):
        out = enc.update(os.urandom(100_000))
        fed += 100_000
        emitted += len(out)
        assert emitted >= header_size(1) + fed - CHUNK_SIZE
    dec = Decryptor(alice[0])
    ct = encrypt(os.urandom(20 * CHUNK_SIZE), [alice[1]])
    got = sum(len(dec.update(ct[i : i + 50_000])) for i in range(0, len(ct), 50_000))
    assert got >= 20 * CHUNK_SIZE - CHUNK_SIZE


def test_finalized_objects_refuse_reuse(alice: tuple[str, str]) -> None:
    enc = Encryptor([alice[1]])
    ct = enc.finalize()
    with pytest.raises(AgeError):
        enc.update(b"x")
    dec = Decryptor(alice[0])
    dec.update(ct)
    dec.finalize()
    with pytest.raises(AgeError):
        dec.finalize()


def test_streams(alice: tuple[str, str]) -> None:
    data = os.urandom(3 * 1024 * 1024 + 5)
    ct = io.BytesIO()
    n = encrypt_stream(io.BytesIO(data), ct, [alice[1]])
    assert n == len(ct.getvalue()) == ciphertext_size(len(data), 1)
    out = io.BytesIO()
    assert decrypt_stream(io.BytesIO(ct.getvalue()), out, alice[0]) == len(data)
    assert out.getvalue() == data


async def test_async_iterators(alice: tuple[str, str]) -> None:
    data = os.urandom(500_000)

    async def feed(buf: bytes, step: int) -> AsyncIterator[bytes]:
        for i in range(0, len(buf), step):
            yield buf[i : i + step]

    ct = b"".join([c async for c in encrypt_iter(feed(data, 33_333), [alice[1]])])
    assert len(ct) == ciphertext_size(len(data), 1)
    out = b"".join([p async for p in decrypt_iter(feed(ct, 77_777), alice[0])])
    assert out == data
    with pytest.raises(AgeError, match="truncated"):
        async for _ in decrypt_iter(feed(ct[:-100], 10_000), alice[0]):
            pass


def test_chunk_sealer_random_access(alice: tuple[str, str]) -> None:
    """Any chunk can be sealed on its own (what lets a multipart upload retry a part)."""
    data = os.urandom(3 * CHUNK_SIZE + 10)
    header, sealer = age.new_header([alice[1]])
    chunks = [data[i : i + CHUNK_SIZE] for i in range(0, len(data), CHUNK_SIZE)]
    sealed = [sealer.seal(i, c, last=i == len(chunks) - 1) for i, c in enumerate(chunks)]
    assert sealer.seal(1, chunks[1], last=False) == sealed[1]  # deterministic
    assert decrypt(header + b"".join(sealed), alice[0]) == data
    with pytest.raises(AgeError):
        sealer.seal(0, b"short", last=False)
    with pytest.raises(AgeError):
        sealer.seal(2, b"", last=True)


# --------------------------------------------------------------------------- interop


@pytest.mark.parametrize("size", SIZES)
def test_pyrage_decrypts_ours(size: int, alice: tuple[str, str], bob: tuple[str, str]) -> None:
    data = os.urandom(size)
    ct = encrypt(data, [alice[1], bob[1]])
    assert pyrage.decrypt(ct, [x25519.Identity.from_str(alice[0])]) == data
    assert pyrage.decrypt(ct, [x25519.Identity.from_str(bob[0])]) == data


@pytest.mark.parametrize("size", SIZES)
def test_we_decrypt_pyrage(size: int, alice: tuple[str, str], bob: tuple[str, str]) -> None:
    data = os.urandom(size)
    recipients = [x25519.Recipient.from_str(alice[1]), x25519.Recipient.from_str(bob[1])]
    # rage adds a random grease stanza: run a few times to meet several shapes of it.
    for _ in range(5):
        ct = pyrage.encrypt(data, recipients)
        assert decrypt(ct, alice[0]) == data
        assert decrypt(ct, bob[0]) == data


def test_we_decrypt_pyrage_streaming(alice: tuple[str, str]) -> None:
    data = os.urandom(2 * 1024 * 1024 + 99)
    ct = io.BytesIO()
    pyrage.encrypt_io(io.BytesIO(data), ct, [x25519.Recipient.from_str(alice[1])])
    out = io.BytesIO()
    decrypt_stream(io.BytesIO(ct.getvalue()), out, alice[0])
    assert out.getvalue() == data


def test_pyrage_decrypts_our_stream(alice: tuple[str, str]) -> None:
    data = os.urandom(2 * 1024 * 1024 + 99)
    ct = io.BytesIO()
    encrypt_stream(io.BytesIO(data), ct, [alice[1]])
    out = io.BytesIO()
    pyrage.decrypt_io(io.BytesIO(ct.getvalue()), out, [x25519.Identity.from_str(alice[0])])
    assert out.getvalue() == data


def test_armored_is_refused(alice: tuple[str, str]) -> None:
    ct = pyrage.encrypt(b"x", [x25519.Recipient.from_str(alice[1])], armored=True)
    with pytest.raises(AgeError, match="not an age v1 file"):
        decrypt(ct, alice[0])


# --------------------------------------------------------------------------- tamper & truncation


def _ct(alice: tuple[str, str], bob: tuple[str, str], size: int = 3 * CHUNK_SIZE + 5) -> bytes:
    return encrypt(os.urandom(size), [alice[1], bob[1]])


def test_wrong_identity(alice: tuple[str, str], bob: tuple[str, str]) -> None:
    ct = encrypt(b"secret", [alice[1]])
    with pytest.raises(AgeError, match="no identity matched"):
        decrypt(ct, bob[0])


def test_every_header_byte_is_authenticated(alice: tuple[str, str], bob: tuple[str, str]) -> None:
    ct = _ct(alice, bob, 100)
    head = header_size(2)
    for i in range(head):
        tampered = bytearray(ct)
        tampered[i] ^= 0x01
        with pytest.raises(AgeError):
            decrypt(bytes(tampered), alice[0])


@pytest.mark.parametrize("offset", [0, 100, CHUNK_SIZE + 16, 2 * (CHUNK_SIZE + 16) + 7, -1])
def test_payload_tamper(offset: int, alice: tuple[str, str], bob: tuple[str, str]) -> None:
    ct = bytearray(_ct(alice, bob))
    ct[header_size(2) + offset if offset >= 0 else offset] ^= 0x80
    with pytest.raises(AgeError, match="authentication failed"):
        decrypt(bytes(ct), alice[0])


@pytest.mark.parametrize("cut", [1, 15, 16, 17, 100, CHUNK_SIZE + 16, CHUNK_SIZE + 21])
def test_truncation(cut: int, alice: tuple[str, str], bob: tuple[str, str]) -> None:
    ct = _ct(alice, bob)
    with pytest.raises(AgeError):
        decrypt(ct[:-cut], alice[0])


def test_truncation_at_every_chunk_boundary(alice: tuple[str, str]) -> None:
    data = os.urandom(4 * CHUNK_SIZE)
    ct = encrypt(data, [alice[1]])
    head = header_size(1)
    for chunks in range(4):
        with pytest.raises(AgeError):
            decrypt(ct[: head + chunks * (CHUNK_SIZE + 16)], alice[0])


def test_header_only_and_partial_header(alice: tuple[str, str]) -> None:
    ct = encrypt(b"", [alice[1]])
    for cut in (0, 5, 30, header_size(1) - 1, header_size(1)):
        with pytest.raises(AgeError, match="truncated"):
            decrypt(ct[:cut], alice[0])


def test_trailing_data(alice: tuple[str, str]) -> None:
    ct = encrypt(os.urandom(1000), [alice[1]])
    with pytest.raises(AgeError):
        decrypt(ct + b"x", alice[0])
    full = encrypt(os.urandom(CHUNK_SIZE), [alice[1]])
    with pytest.raises(AgeError):
        decrypt(full + bytes(16), alice[0])


def test_chunk_reordering(alice: tuple[str, str]) -> None:
    ct = encrypt(os.urandom(3 * CHUNK_SIZE + 1), [alice[1]])
    head, size = header_size(1), CHUNK_SIZE + 16
    c = [ct[head + i * size : head + (i + 1) * size] for i in range(3)]
    swapped = ct[:head] + c[1] + c[0] + c[2] + ct[head + 3 * size :]
    with pytest.raises(AgeError, match="authentication failed"):
        decrypt(swapped, alice[0])


def test_empty_last_chunk_after_full_chunk_is_refused(alice: tuple[str, str]) -> None:
    """The spec forbids an empty final chunk unless the whole payload is empty; a writer
    that emits one must be refused, or two encodings of the same plaintext would exist."""
    header, sealer = age.new_header([alice[1]])
    data = os.urandom(CHUNK_SIZE)
    first = sealer._aead.encrypt(sealer._nonce(0, False), data, None)
    empty_last = sealer._aead.encrypt(sealer._nonce(1, True), b"", None)
    with pytest.raises(AgeError, match="empty last chunk"):
        decrypt(header + first + empty_last, alice[0])


def test_unknown_and_grease_stanzas_are_skipped(alice: tuple[str, str]) -> None:
    ct = pyrage.encrypt(b"grease", [x25519.Recipient.from_str(alice[1])])
    assert b"-grease" in ct
    assert decrypt(ct, alice[0]) == b"grease"


@pytest.mark.parametrize(
    "stanza",
    [
        b"-> X25519\nAAAA\n",  # missing share
        b"-> X25519 a b\nAAAA\n",  # too many args
        b"-> X25519 AAAA\n" + b"A" * 43 + b"\n",  # share not 32 bytes
        b"->  X25519 x\n\n",  # empty argument
        b"-> X25519 \x7f\n\n",  # non-VCHAR argument
        b"-> foo\n" + b"A" * 65 + b"\n",  # body line too long
        b"-> foo\nAB=\n",  # padding is not allowed
        b"-> foo\nAB\n",  # non-canonical base64 (stray bits)
        b"junk\n",
    ],
)
def test_malformed_stanzas(stanza: bytes, alice: tuple[str, str]) -> None:
    header, _ = age.new_header([alice[1]])
    first_stanza = header.index(b"-> ")
    # Put the bad stanza first so it is parsed (or unwrapped) before ours.
    bad = header[:first_stanza] + stanza + header[first_stanza:]
    with pytest.raises(AgeError) as err:
        decrypt(bad + bytes(32), alice[0])
    # Any edit to a header breaks its MAC, so a MAC failure here would prove nothing about
    # the parser: the stanza itself must be what is refused.
    assert "MAC" not in str(err.value)


def test_header_size_is_bounded(alice: tuple[str, str]) -> None:
    dec = Decryptor(alice[0])
    dec.update(b"age-encryption.org/v1\n-> grease\n")

    def endless_body() -> None:
        for _ in range(2000):  # 130 KB of 64-column body lines: never a short last line
            dec.update(b"A" * 64 + b"\n")

    with pytest.raises(AgeError, match="too large"):
        endless_body()
    with pytest.raises(AgeError, match="no recipient stanza"):
        decrypt(b"age-encryption.org/v1\n--- " + b"A" * 43 + b"\n" + bytes(32), alice[0])
