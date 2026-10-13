"""Durations (``90s``, ``30m``, ``6h``, ``7d``) and sizes (``512MB``, ``5GB``, ``1GiB``).

``offering.yaml`` is written by people, so parsing is forgiving (``1h30m``, ``1024MiB``).
The contract document is hashed, so it carries exactly one spelling per value: the
*canonical* form, the largest unit that divides the value exactly. Two offerings with the
same limits therefore produce byte-identical ``x-kontra`` blocks, whatever their authors
typed.
"""

from __future__ import annotations

import re
from datetime import timedelta

__all__ = [
    "DURATION_UNITS",
    "SIZE_UNITS",
    "format_duration",
    "format_size",
    "parse_duration",
    "parse_size",
]

#: Unit -> seconds, largest first (the canonical search order).
DURATION_UNITS: tuple[tuple[str, int], ...] = (("d", 86_400), ("h", 3_600), ("m", 60), ("s", 1))

#: Unit -> bytes. Decimal (SI) and binary (IEC) units are both accepted, because people
#: write both and mean different numbers by them.
SIZE_UNITS: dict[str, int] = {
    "B": 1,
    "KB": 1000,
    "MB": 1000**2,
    "GB": 1000**3,
    "TB": 1000**4,
    "KiB": 1024,
    "MiB": 1024**2,
    "GiB": 1024**3,
    "TiB": 1024**4,
}

_DURATION_PART = re.compile(r"(0|[1-9][0-9]{0,9})([dhms])", re.ASCII)
_DURATION = re.compile(r"(?:(?:0|[1-9][0-9]{0,9})[dhms])+", re.ASCII)
_SIZE = re.compile(r"(0|[1-9][0-9]{0,15})(B|KB|MB|GB|TB|KiB|MiB|GiB|TiB)", re.ASCII)
_SECONDS = dict(DURATION_UNITS)
_RANK = {unit: i for i, (unit, _) in enumerate(DURATION_UNITS)}
#: 10,000 years: far past every bound, far below what ``timedelta`` overflows at.
_MAX_SECONDS = 10_000 * 365 * 86_400


def parse_duration(text: str) -> timedelta:
    """``"6h"`` -> 6 hours. Compound forms (``"1h30m"``) are accepted, each unit at most
    once and largest first. Whole seconds only: a contract limit finer than a second is
    noise."""
    s = text
    if len(s) > 64 or not _DURATION.fullmatch(s):
        raise ValueError(
            f"{text!r} is not a duration; write a whole number and a unit: 90s, 30m, 6h or 7d"
        )
    total = 0
    last = -1
    for num, unit in _DURATION_PART.findall(s):
        rank = _RANK[unit]
        if rank <= last:
            raise ValueError(f"{text!r}: write each unit once, largest first (e.g. 1h30m)")
        last = rank
        total += int(num) * _SECONDS[unit]
    if total > _MAX_SECONDS:
        raise ValueError(f"{text!r} is longer than any limit an offering may set")
    return timedelta(seconds=total)


def format_duration(value: timedelta) -> str:
    """The canonical spelling: the largest unit that divides exactly. ``0`` is ``"0s"``."""
    seconds = value.total_seconds()
    if seconds != int(seconds) or seconds < 0:
        raise ValueError(f"{value} must be a non-negative whole number of seconds")
    whole = int(seconds)
    if whole == 0:
        return "0s"
    for unit, size in DURATION_UNITS:
        if whole % size == 0:
            return f"{whole // size}{unit}"
    raise AssertionError("unreachable: seconds divide everything")  # pragma: no cover


def parse_size(text: str) -> int:
    """``"5GB"`` -> 5_000_000_000; ``"1GiB"`` -> 1_073_741_824. Units are case-sensitive
    (``mb`` would be millibits)."""
    m = _SIZE.fullmatch(text) if len(text) <= 64 else None
    if m is None:
        raise ValueError(
            f"{text!r} is not a size; write a whole number and a unit: "
            "512MB, 5GB or 1GiB (B, KB, MB, GB, TB, KiB, MiB, GiB, TiB)"
        )
    return int(m.group(1)) * SIZE_UNITS[m.group(2)]


def format_size(value: int) -> str:
    """The canonical spelling: the unit giving the smallest whole number; on a tie the
    decimal unit. ``0`` is ``"0B"``."""
    if value < 0:
        raise ValueError(f"{value} must not be negative")
    if value == 0:
        return "0B"
    best_unit, best_num = "B", value
    for unit, size in SIZE_UNITS.items():
        if value % size == 0 and value // size < best_num:
            best_unit, best_num = unit, value // size
    return f"{best_num}{best_unit}"
