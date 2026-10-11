"""Value checks that must agree with nexgen-generated validators in other languages.

``jsonschema``'s own format checker silently skips ``uri``, ``date-time``, ``time``,
``duration`` and ``hostname`` unless optional packages are installed, and Python's ``re``
differs from the RE2/ECMA dialect nexgen's targets use (``$`` matches before a trailing
newline; ``\\d`` matches any Unicode digit). Either gap would let a value through the
seller's handler that a Go or TypeScript buyer rejects, or worse, let ``"key\\n"`` pass
an Artifact key pattern. So the contract owns both: the formats nexgen asserts
(its curated, ASCII-only subset) and an ECMA-style ``pattern``.
"""

from __future__ import annotations

import ipaddress
import re
from collections.abc import Callable, Iterator, Mapping
from datetime import date
from functools import lru_cache
from typing import Any

import re2  # pyright: ignore[reportMissingTypeStubs]
from jsonschema import Draft202012Validator, FormatChecker, ValidationError
from jsonschema.protocols import Validator
from jsonschema.validators import extend

from kontra.contract.patterns import PATTERN_MAX, PatternProblem, check_pattern

__all__ = [
    "FORMATS",
    "PATTERN_MAX",
    "ContractValidator",
    "check_format",
    "compile_pattern",
    "format_checker",
    "pattern_problem",
]

# --------------------------------------------------------------------------- RFC 3986

_UNRESERVED = r"A-Za-z0-9\-._~"
_SUB_DELIMS = r"!$&'()*+,;="
_PCT = r"%[0-9A-Fa-f]{2}"
_PCHAR = rf"(?:[{_UNRESERVED}{_SUB_DELIMS}:@]|{_PCT})"
_SCHEME = r"[A-Za-z][A-Za-z0-9+\-.]*"
_USERINFO = rf"(?:[{_UNRESERVED}{_SUB_DELIMS}:]|{_PCT})*"
_REG_NAME = rf"(?:[{_UNRESERVED}{_SUB_DELIMS}]|{_PCT})*"
_IP_LITERAL = r"\[(?P<ip>[^\]/?#@\[]*)\]"
_HOST = rf"(?:{_IP_LITERAL}|{_REG_NAME})"
_AUTHORITY = rf"(?:{_USERINFO}@)?{_HOST}(?::[0-9]*)?"
_SEGMENT = rf"{_PCHAR}*"
_SEGMENT_NZ = rf"{_PCHAR}+"
_SEGMENT_NZ_NC = rf"(?:[{_UNRESERVED}{_SUB_DELIMS}@]|{_PCT})+"
_PATH_ABEMPTY = rf"(?:/{_SEGMENT})*"
_PATH_ABSOLUTE = rf"/(?:{_SEGMENT_NZ}(?:/{_SEGMENT})*)?"
_PATH_ROOTLESS = rf"{_SEGMENT_NZ}(?:/{_SEGMENT})*"
_PATH_NOSCHEME = rf"{_SEGMENT_NZ_NC}(?:/{_SEGMENT})*"
_QUERY = rf"(?:{_PCHAR}|[/?])*"
_HIER = rf"(?://{_AUTHORITY}{_PATH_ABEMPTY}|{_PATH_ABSOLUTE}|{_PATH_ROOTLESS}|)"
_RELATIVE = rf"(?://{_AUTHORITY}{_PATH_ABEMPTY}|{_PATH_ABSOLUTE}|{_PATH_NOSCHEME}|)"
_URI = re.compile(rf"{_SCHEME}:{_HIER}(?:\?{_QUERY})?(?:#{_QUERY})?", re.ASCII)
_RELATIVE_REF = re.compile(rf"{_RELATIVE}(?:\?{_QUERY})?(?:#{_QUERY})?", re.ASCII)
_IPVFUTURE = re.compile(rf"[vV][0-9A-Fa-f]+\.[{_UNRESERVED}{_SUB_DELIMS}:]+", re.ASCII)


def _ip_literal_ok(match: re.Match[str]) -> bool:
    ip = match.group("ip")
    if ip is None:
        return True
    return _is_ipv6(ip) or _IPVFUTURE.fullmatch(ip) is not None


def _is_uri(value: str) -> bool:
    m = _URI.fullmatch(value)
    return m is not None and _ip_literal_ok(m)


def _is_uri_reference(value: str) -> bool:
    if _is_uri(value):
        return True
    m = _RELATIVE_REF.fullmatch(value)
    return m is not None and _ip_literal_ok(m)


# --------------------------------------------------------------------------- the others

_UUID = re.compile(
    r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}", re.ASCII
)
_OCTET = r"(?:25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])"
_IPV4 = re.compile(rf"{_OCTET}(?:\.{_OCTET}){{3}}", re.ASCII)
_LABEL = r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?"
_HOSTNAME = re.compile(rf"{_LABEL}(?:\.{_LABEL})*", re.ASCII)
_ATEXT = r"[A-Za-z0-9!#$%&'*+/=?^_`{|}~-]"
_EMAIL = re.compile(rf"{_ATEXT}+(?:\.{_ATEXT}+)*@{_LABEL}(?:\.{_LABEL})*", re.ASCII)
_DATE = re.compile(r"([0-9]{4})-([0-9]{2})-([0-9]{2})", re.ASCII)
_TIME = re.compile(
    r"([0-9]{2}):([0-9]{2}):([0-9]{2})(?:\.[0-9]+)?(?:[Zz]|([+-])([0-9]{2}):([0-9]{2}))", re.ASCII
)
_DURATION = re.compile(
    r"P(?:[0-9]+W|(?=[0-9T])(?:[0-9]+Y)?(?:[0-9]+M)?(?:[0-9]+D)?"
    r"(?:T(?=[0-9])(?:[0-9]+H)?(?:[0-9]+M)?(?:[0-9]+S)?)?)",
    re.ASCII,
)


def _is_ipv6(value: str) -> bool:
    if "%" in value or not value.isascii():
        return False  # zone ids are not part of RFC 4291 text form
    try:
        ipaddress.IPv6Address(value)
    except ValueError:
        return False
    return True


def _is_date(value: str) -> bool:
    m = _DATE.fullmatch(value)
    if m is None:
        return False
    try:
        date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    except ValueError:
        return False
    return True


def _is_time(value: str) -> bool:
    m = _TIME.fullmatch(value)
    if m is None:
        return False
    hh, mm, ss = int(m.group(1)), int(m.group(2)), int(m.group(3))
    if hh > 23 or mm > 59 or ss > 59:
        return False
    if m.group(4):
        oh, om = int(m.group(5)), int(m.group(6))
        if om > 59 or oh * 60 + om > 18 * 60:
            return False
    return True


def _is_date_time(value: str) -> bool:
    head, sep, tail = value.partition("T") if "T" in value else value.partition("t")
    return bool(sep) and _is_date(head) and _is_time(tail)


def _is_hostname(value: str) -> bool:
    return len(value) <= 253 and _HOSTNAME.fullmatch(value) is not None


def _is_email(value: str) -> bool:
    return len(value) <= 254 and _EMAIL.fullmatch(value) is not None


#: The formats nexgen 0.2.5 asserts. Any other ``format`` is refused in a contract.
FORMATS: dict[str, Callable[[str], bool]] = {
    "uuid": lambda v: _UUID.fullmatch(v) is not None,
    "ipv4": lambda v: _IPV4.fullmatch(v) is not None,
    "ipv6": _is_ipv6,
    "uri": _is_uri,
    "uri-reference": _is_uri_reference,
    "hostname": _is_hostname,
    "email": _is_email,
    "date": _is_date,
    "date-time": _is_date_time,
    "time": _is_time,
    "duration": lambda v: _DURATION.fullmatch(v) is not None,
}


def check_format(name: str, value: str) -> bool:
    return FORMATS[name](value)


def format_checker() -> FormatChecker:
    """A checker asserting exactly :data:`FORMATS` (ASCII-only, as nexgen does)."""
    checker = FormatChecker(formats=())
    for name, fn in FORMATS.items():

        def check(instance: object, fn: Callable[[str], bool] = fn) -> bool:
            return not isinstance(instance, str) or (instance.isascii() and fn(instance))

        checker.checks(name)(check)
    return checker


# --------------------------------------------------------------------------- patterns


@lru_cache(maxsize=4096)
def pattern_problem(pattern: str) -> str | None:
    """Why ``pattern`` may not appear in a contract (see :mod:`kontra.contract.patterns`),
    or None. Never raises: the catalog reads patterns from strangers."""
    try:
        check_pattern(pattern)
        compile_pattern(pattern)
    except PatternProblem as exc:
        return str(exc)
    except (re.error, re2.error, ValueError, OverflowError, RecursionError) as exc:
        return f"is not a valid regular expression: {exc}"
    return None


@lru_cache(maxsize=1024)
def compile_pattern(pattern: str) -> Any:
    """``pattern`` compiled by RE2, which matches in linear time whatever the pattern: a
    seller's contract runs on buyers' machines and a buyer's input runs against a seller's
    handler, so neither may be able to stall the other with backtracking. RE2's own
    semantics are ECMA's for the portable subset :mod:`kontra.contract.patterns` admits: ``$``
    is the end of input and ``\\d \\w \\s`` are ASCII. Matching is a search, as JSON Schema
    requires (anchors are explicit)."""
    return re2.compile(pattern)


def _pattern_keyword(
    validator: Validator, pattern: Any, instance: Any, schema: Mapping[str, Any]
) -> Iterator[ValidationError]:
    if (
        isinstance(instance, str)
        and isinstance(pattern, str)
        and not compile_pattern(pattern).search(instance)
    ):
        yield ValidationError(f"{instance!r} does not match {pattern!r}")


#: JSON Schema 2020-12 with ECMA/RE2 ``pattern`` semantics. Pass ``format_checker()`` to
#: assert formats. Used for values (InputValidator) and for a contract's own literals.
ContractValidator: Any = extend(Draft202012Validator, {"pattern": _pattern_keyword})
