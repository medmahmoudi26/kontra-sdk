"""What every command shares: running a coroutine, turning a known failure into one clear
line, printing for people or as JSON, reading an input file, and the default handle.

The CLI holds no business logic (PRD §12): each command calls :mod:`kontra.auth`,
:mod:`kontra.hire`, :mod:`kontra.serve` or :mod:`kontra.contract` and prints the answer.
What *is* here is the translation of their exceptions into ``error: ...`` and exit code 1,
so a user reads the gate's ``code``/``message`` or the contract path at fault instead of a
traceback (set ``KONTRA_DEBUG=1`` for the traceback too).
"""

from __future__ import annotations

import asyncio
import contextlib
import getpass
import json
import os
import re
import sys
from collections.abc import Callable, Coroutine, Generator
from decimal import Decimal
from pathlib import Path
from typing import IO, TYPE_CHECKING, Any, NoReturn, TypeVar

import click
import httpx
from pydantic import BaseModel

from kontra.contract.names import HANDLE_MAX, RESERVED_HANDLES, is_handle

if TYPE_CHECKING:
    from kontra.auth import Session

__all__ = [
    "DEBUG_ENV",
    "CliError",
    "KontraGroup",
    "cli_session",
    "default_handle",
    "describe",
    "emit_json",
    "fail",
    "interruptible",
    "printable",
    "read_input",
    "run",
    "to_jsonable",
]

_T = TypeVar("_T")

#: ``KONTRA_DEBUG=1`` shows the traceback of a handled error as well as its one line.
DEBUG_ENV = "KONTRA_DEBUG"
#: An input file larger than this is refused before it is parsed: inputs travel as one
#: Temporal payload; anything bigger belongs in an ``Artifact``.
MAX_INPUT_BYTES = 4 * 1024 * 1024
_HINTS = {
    # Decisions I23: a change needs the login confirmed within the hour; a command asks in
    # place when a person is at the terminal, so this is what everyone else reads.
    "confirmation_required": "this change needs you to confirm it's you: run `kontra "
    "confirm` in a terminal, then this command again",
    "confirm_unavailable": "your account cannot confirm it's you by email on this gate: run "
    "`kontra login`, then the command again",
    # A gate older than kontra-sdk 0.2.2 still answers this.
    "stale_session": "your login is too old for this (it needs a recent one): run "
    "`kontra login` and try again",
    "fresh_login_required": "an API key hires and serves, but publishing, changing "
    "recipients or devices and seeing or managing keys need a person's login: unset "
    "KONTRA_API_KEY",
    "invalid_token": "the gate no longer accepts this session: run `kontra login`",
    "device_revoked": "that key was removed as a device and can never be added again: "
    "`kontra devices add --new-key` adds this machine with a key of its own",
    "too_many_devices": "remove a device first: `kontra devices list`",
}

#: Hints that tell a human to log in again; a dev profile is pointed at `kontra dev up` instead.
_RELOGIN_CODES = frozenset({"invalid_token", "stale_session"})


#: C0 and C1 control characters but tab and newline: an escape sequence in a seller's text
#: (a rejection reason, a failure message) must not drive the buyer's terminal.
_CONTROL = re.compile(r"[\x00-\x08\x0b-\x1f\x7f-\x9f]")


def printable(text: object) -> str:
    """``text`` without control characters, for printing what another party wrote."""
    return _CONTROL.sub("", str(text))


class CliError(click.ClickException):
    """A failure the user can act on: printed as ``error: <message>``, exit code 1."""

    def show(self, file: IO[Any] | None = None) -> None:
        click.echo(f"error: {printable(self.format_message())}", file=file or sys.stderr)


def fail(message: str) -> NoReturn:
    raise CliError(message)


def describe(exc: BaseException) -> str | None:
    """The one line a known failure prints, or None for an unexpected one (a bug: the
    traceback is shown). Imports are local so ``kontra --help`` stays fast."""
    from kontra.artifacts import ArtifactError
    from kontra.auth import LoginError
    from kontra.codec import CodecError
    from kontra.codec.age import AgeError
    from kontra.contract import ContractError, OfferingError, QuoteError
    from kontra.contract.names import NameError_
    from kontra.gate_client import GateError
    from kontra.ledger import LedgerError
    from kontra.profile import ProfileError, profile_is_dev

    if isinstance(exc, GateError):
        hint = _HINTS.get(exc.code)
        if hint and exc.code in _RELOGIN_CODES and profile_is_dev():
            # A dev profile is never told `kontra login` (decisions I34, OD14).
            hint = "the dev gate restarted? run `kontra dev up`, then restart what you were running"
        line = f"the gate answered {exc.status} {exc.code}: {exc.message}"
        return f"{line}\n  {hint}" if hint else line
    if isinstance(exc, OfferingError):
        return str(exc)  # "<file>: <key>: <message>"
    if isinstance(exc, ContractError):
        return f"contract: {exc.path}: {exc.message}"
    if isinstance(exc, httpx.TransportError):
        where = ""
        with contextlib.suppress(RuntimeError):  # no request attached
            where = f" {exc.request.url.copy_with(query=None)}"
        line = f"cannot reach{where}: {str(exc) or type(exc).__name__}"
        if profile_is_dev():  # the local dev stack is not up (decisions I34, OD14)
            return f"{line}\n  is the dev stack up? run `kontra dev up` (`kontra dev status`)"
        return line
    if isinstance(exc, LoginError):
        return exc.message
    known: tuple[type[BaseException], ...] = (
        ProfileError,
        NameError_,
        ArtifactError,
        CodecError,
        AgeError,
        QuoteError,
        LedgerError,
    )
    if isinstance(exc, known):
        return str(exc)
    # Classes whose modules pull in temporalio: only consulted once the cheap ones missed.
    from kontra.hire import KontraError
    from kontra.hire._client import TemporalUnavailable
    from kontra.serve import BindingError, ServeError

    if isinstance(exc, KontraError | ServeError | BindingError):
        return str(exc)
    if isinstance(exc, TemporalUnavailable):
        return f"cannot reach Temporal: {exc}"
    return _temporal_failure(exc)


#: temporalio raises a bare RuntimeError with this prefix when it cannot connect (as
#: kontra.hire notes too); nothing narrower exists to catch.
_CONNECT_FAILED = "Failed client connect"


def _temporal_failure(exc: BaseException) -> str | None:
    """Temporal refusing or not answering (``kontra serve`` before ``kontra dev`` is up,
    a token Temporal does not accept yet): the server's own status and message, one line."""
    from temporalio.service import RPCError

    if isinstance(exc, RPCError):
        return f"Temporal answered {exc.status.name}: {exc.message}"
    if type(exc) is RuntimeError and str(exc).startswith(_CONNECT_FAILED):
        return f"cannot reach Temporal ({exc}); is it up? (`kontra dev status` for the local stack)"
    return None


class KontraGroup(click.Group):
    """A group whose commands turn known failures into :class:`CliError`."""

    def invoke(self, ctx: click.Context) -> Any:
        try:
            return super().invoke(ctx)
        except (click.ClickException, click.exceptions.Exit, click.Abort):
            raise
        except Exception as exc:
            line = describe(exc)
            if line is None or os.environ.get(DEBUG_ENV) == "1":
                raise
            raise CliError(line) from exc


#: What a Ctrl-C ended (the note of the :func:`interruptible` wait it stopped), for :func:`run`.
_stopped: list[str] = []


@contextlib.contextmanager
def interruptible(note: str) -> Generator[None]:
    """A wait for a person (a confirmation) that Ctrl-C may end: :func:`run` then prints
    ``note`` instead of a traceback and exits 130."""
    try:
        yield
    except asyncio.CancelledError:  # what Ctrl-C is inside asyncio.run
        _stopped.append(note)
        raise


def run(factory: Callable[[], Coroutine[Any, Any, _T]]) -> _T:
    """``asyncio.run`` one command's coroutine."""
    _stopped.clear()
    try:
        return asyncio.run(factory())
    except KeyboardInterrupt:
        if not _stopped:
            raise
        click.echo(_stopped[-1], err=True)
        raise click.exceptions.Exit(130) from None


def cli_session() -> Session:
    """The session of a command that makes changes (``Session.from_env``): when the gate
    asks for a confirmation, a person at this terminal is asked in place
    (:mod:`kontra.cli._confirm`, decisions I23)."""
    from kontra.auth import Session
    from kontra.cli._confirm import terminal_confirmer

    return Session.from_env(confirm=terminal_confirmer)


def to_jsonable(value: Any) -> Any:
    """Models, records, decimals and datetimes as JSON values."""
    from kontra.hire import Record

    if isinstance(value, BaseModel):
        return value.model_dump(mode="json")
    if isinstance(value, Record):
        return value.to_dict()
    if isinstance(value, dict):
        return {str(k): to_jsonable(v) for k, v in value.items()}  # pyright: ignore[reportUnknownVariableType]
    if isinstance(value, list | tuple):
        return [to_jsonable(v) for v in value]  # pyright: ignore[reportUnknownVariableType]
    if isinstance(value, Decimal):
        return str(value)
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


def emit_json(value: Any) -> None:
    click.echo(json.dumps(to_jsonable(value), indent=2, sort_keys=True))


def _no_constant(name: str) -> Any:
    raise ValueError(f"{name} is not a JSON number")


def _no_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    """An object whose key repeats means two things; JSON parsers disagree on which."""
    out: dict[str, Any] = {}
    for key, value in pairs:
        if key in out:
            raise ValueError(f"the key {key!r} appears twice in one object")
        out[key] = value
    return out


def read_input(source: str) -> dict[str, Any]:
    """The JSON object in file ``source`` (``-``: stdin), refused with a clear line when it
    is missing, too large, not JSON, or not an object (an operation's input is one)."""
    try:
        if source == "-":
            raw = sys.stdin.buffer.read(MAX_INPUT_BYTES + 1)
        else:
            with Path(source).open("rb") as f:
                raw = f.read(MAX_INPUT_BYTES + 1)
    except OSError as e:
        fail(f"cannot read the input {source}: {e.strerror or e}")
    if len(raw) > MAX_INPUT_BYTES:
        fail(
            f"the input {source} is larger than {MAX_INPUT_BYTES // (1024 * 1024)} MiB; "
            "send large data as an Artifact (kontra.hire.Kontra.upload)"
        )
    try:
        value: object = json.loads(
            raw, parse_constant=_no_constant, object_pairs_hook=_no_duplicates
        )
    except (ValueError, RecursionError) as e:  # UnicodeDecodeError is a ValueError
        fail(f"the input {source} is not JSON: {e or 'nested too deeply'}")
    if not isinstance(value, dict):
        fail(f"the input {source} must be a JSON object, got {type(value).__name__}")
    return {str(k): v for k, v in value.items()}  # pyright: ignore[reportUnknownVariableType]


_NOT_HANDLE = re.compile(r"[^a-z0-9]+", re.ASCII)


def default_handle(user: str | None = None) -> str:
    """A party handle derived from the OS user name, for ``kontra dev`` and ``kontra init``
    when no handle is given: lowercased, runs of other characters turned into one hyphen,
    trimmed to a letter start and :data:`HANDLE_MAX`; a reserved result (``root``) gets
    ``-dev``. The result is then validated like any typed handle."""
    if user is None:
        try:
            user = getpass.getuser()
        except (OSError, KeyError):  # no passwd entry (a container), no USER variable
            user = ""
    text = _NOT_HANDLE.sub("-", user.lower()).strip("-")
    text = re.sub(r"^[^a-z]+", "", text, flags=re.ASCII)[:HANDLE_MAX].rstrip("-")
    if not text:
        text = "dev"
    if text in RESERVED_HANDLES:
        text = f"{text}-dev"[:HANDLE_MAX]
    if not is_handle(text):  # cannot happen by construction; never pass a bad default on
        fail(f"cannot derive a party handle from the user name {user!r}; pass --handle")
    return text
