"""The one error type of the contract seam.

Every refusal names *where*: a dotted JSON path into the document
(``$defs.EnrichInput.properties.urls``), the offering file (``max_duration``) or the
value being validated (``input.urls[3]``). A message without a location sends a seller
grepping through generated JSON; with one, the fix is obvious.
"""

from __future__ import annotations

import re
from collections.abc import Sequence

__all__ = ["ROOT", "ContractError", "OfferingError", "join_path"]

#: The path of the document (or value) itself.
ROOT = "$"

_PLAIN_KEY = re.compile(r"[A-Za-z_$][A-Za-z0-9_$-]*", re.ASCII)


def join_path(base: str, *parts: str | int) -> str:
    """``join_path("$defs.A", "properties", "x")`` -> ``"$defs.A.properties.x"``.

    Integers are array indices (``oneOf[1]``); keys that are not plain identifiers are
    bracket-quoted (``properties["a.b"]``) so a path is never ambiguous.
    """
    out = "" if base == ROOT else base
    for part in parts:
        if isinstance(part, int):
            out += f"[{part}]"
        elif _PLAIN_KEY.fullmatch(part):
            out = f"{out}.{part}" if out else part
        else:
            quoted = part.replace("\\", "\\\\").replace('"', '\\"')
            out += f'["{quoted}"]'
    return out or ROOT


class ContractError(ValueError):
    """A contract document, offering or value that breaks a rule.

    ``path`` locates the offence; ``message`` says what is wrong and, where there is
    one, how to fix it. ``errors`` carries every violation when more than one was found
    (input validation reports them all); ``path``/``message`` are the first.
    """

    def __init__(
        self,
        path: str,
        message: str,
        *,
        errors: Sequence[tuple[str, str]] = (),
    ) -> None:
        self.path = path or ROOT
        self.message = message
        self.errors: tuple[tuple[str, str], ...] = tuple(errors) or ((self.path, message),)
        super().__init__(f"{self.path}: {message}")


class OfferingError(ContractError):
    """``offering.yaml`` is wrong. ``path`` is the key (``max_duration``); ``source`` the
    file, when the offering came from one."""

    def __init__(
        self,
        path: str,
        message: str,
        *,
        source: str | None = None,
        errors: Sequence[tuple[str, str]] = (),
    ) -> None:
        self.source = source
        super().__init__(path, message, errors=errors)
        if source:
            self.args = (f"{source}: {self.path}: {message}",)
