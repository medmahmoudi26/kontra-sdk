"""The temporalio range Kontra is tested on (decisions I29).

Seller code imports ``temporalio`` directly, so compatibility is a dependency problem, not an
import path: kontra-sdk declares ``temporalio>=1.34,<1.38`` (the tested range) and says so at
the three moments it matters. :class:`~kontra.plugin.KontraPlugin` refuses to start a worker
outside it (:func:`check_temporalio`: the codec relies on temporalio's serialization context
and Nexus operation info, experimental APIs whose shape a minor release may change); ``kontra
serve`` and ``kontra publish`` warn before they load ``worker.py`` (:func:`temporalio_warning`),
so a seller whose resolver picked another temporalio learns why before anything else fails.

The range is one constant pair here, never read back from the installed distribution's
metadata (an editable install's can be stale); ``tests/serve/test_compat.py`` checks it equals
``pyproject.toml``'s specifier.
"""

from __future__ import annotations

import re

__all__ = [
    "TEMPORALIO_MAX",
    "TEMPORALIO_MIN",
    "check_temporalio",
    "temporalio_range",
    "temporalio_version",
    "temporalio_warning",
]

#: The oldest temporalio minor Kontra runs on, inclusive.
TEMPORALIO_MIN = (1, 34)
#: The first temporalio minor outside the tested range, exclusive.
TEMPORALIO_MAX = (1, 38)

#: A release version as temporalio publishes them (``1.34.0``, ``1.35.0rc1``): numbers only
#: are read, anchored at both ends.
_NUM = r"(0|[1-9][0-9]{0,5})"
_VERSION = re.compile(rf"{_NUM}\.{_NUM}(?:\.{_NUM})?([A-Za-z0-9.+-]{{0,32}})", re.ASCII)


def temporalio_range() -> str:
    """The range as a requirement specifier: ``>=1.34,<1.38``."""
    lo, hi = TEMPORALIO_MIN, TEMPORALIO_MAX
    return f">={lo[0]}.{lo[1]},<{hi[0]}.{hi[1]}"


def temporalio_version(version: str | None = None) -> tuple[int, int, int] | None:
    """``(major, minor, patch)`` of ``version`` (default: the installed
    ``temporalio.__version__``), or None when it is not a version string."""
    if version is None:
        import temporalio

        version = str(getattr(temporalio, "__version__", ""))
    m = _VERSION.fullmatch(version)
    if m is None:
        return None
    return int(m.group(1)), int(m.group(2)), int(m.group(3) or 0)


def _installed() -> str:
    import temporalio

    return str(getattr(temporalio, "__version__", "unknown"))


def temporalio_warning() -> str | None:
    """One line saying the installed temporalio is outside the tested range, or None when it
    is inside."""
    parsed = temporalio_version()
    if parsed is not None and TEMPORALIO_MIN <= parsed[:2] < TEMPORALIO_MAX:
        return None
    return (
        f"temporalio {_installed()} is outside the range kontra-sdk is tested on "
        f"(temporalio{temporalio_range()}); install one in it, e.g. "
        f"`pip install 'temporalio{temporalio_range()}'`"
    )


def check_temporalio() -> None:
    """Refuse to start (:class:`~kontra.serve.ServeError`) outside the tested range."""
    line = temporalio_warning()
    if line is not None:
        from kontra.serve.errors import ServeError

        raise ServeError(f"refusing to start: {line}")
