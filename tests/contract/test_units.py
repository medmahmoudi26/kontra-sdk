from __future__ import annotations

from datetime import timedelta

import pytest
from hypothesis import given
from hypothesis import strategies as st

from kontra.contract.units import format_duration, format_size, parse_duration, parse_size


@pytest.mark.parametrize(
    ("text", "seconds"),
    [
        ("90s", 90),
        ("30m", 1800),
        ("6h", 21600),
        ("7d", 604800),
        ("0s", 0),
        ("1h30m", 5400),
        ("1d2h3m4s", 93784),
    ],
)
def test_parse_duration(text: str, seconds: int) -> None:
    assert parse_duration(text) == timedelta(seconds=seconds)


@pytest.mark.parametrize(
    "text",
    ["6", "6H", "1.5h", " 6h", "6h ", "6h\n", "30m1h", "1h1h", "", "-1h", "06h", "1w", "6 h"],
)
def test_parse_duration_refuses(text: str) -> None:
    with pytest.raises(ValueError, match=r"duration|largest first"):
        parse_duration(text)


@pytest.mark.parametrize(
    ("seconds", "canonical"),
    [
        (0, "0s"),
        (90, "90s"),
        (120, "2m"),
        (5400, "90m"),
        (21600, "6h"),
        (86400, "1d"),
        (90000, "25h"),
    ],
)
def test_format_duration_is_the_largest_exact_unit(seconds: int, canonical: str) -> None:
    assert format_duration(timedelta(seconds=seconds)) == canonical


def test_format_duration_refuses_fractions() -> None:
    with pytest.raises(ValueError, match="whole number"):
        format_duration(timedelta(milliseconds=1500))


@pytest.mark.parametrize(
    ("text", "size"),
    [
        ("0B", 0),
        ("512MB", 512_000_000),
        ("5GB", 5_000_000_000),
        ("1GiB", 1_073_741_824),
        ("10KiB", 10_240),
        ("2TB", 2 * 10**12),
    ],
)
def test_parse_size(text: str, size: int) -> None:
    assert parse_size(text) == size


@pytest.mark.parametrize("text", ["5gb", "5 GB", "5.5GB", "5GB\n", "GB", "-1B", "05GB", "5G"])
def test_parse_size_refuses(text: str) -> None:
    with pytest.raises(ValueError, match="not a size"):
        parse_size(text)


@pytest.mark.parametrize(
    ("size", "canonical"),
    [
        (0, "0B"),
        (5_000_000_000, "5GB"),
        (1_073_741_824, "1GiB"),
        (1_024_000_000, "1024MB"),
        (1000, "1KB"),
        (1024, "1KiB"),
        (1536, "1536B"),
        (2_048_000, "2000KiB"),
    ],
)
def test_format_size_picks_the_smallest_number(size: int, canonical: str) -> None:
    assert format_size(size) == canonical


@given(st.integers(min_value=0, max_value=30 * 86400))
def test_duration_round_trip(seconds: int) -> None:
    text = format_duration(timedelta(seconds=seconds))
    assert parse_duration(text) == timedelta(seconds=seconds)
    assert format_duration(parse_duration(text)) == text


@given(st.integers(min_value=0, max_value=5 * 1024**4))
def test_size_round_trip(size: int) -> None:
    text = format_size(size)
    assert parse_size(text) == size
    assert format_size(parse_size(text)) == text
