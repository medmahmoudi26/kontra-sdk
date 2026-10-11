"""Validating values against a contract (§11.3: "neither side trusts the other").

The seller's handler validates the input before it starts a workflow; the buyer's SDK
validates before it encrypts, and validates the delivery after it decrypts. Both use this
class, so both apply the same rules: JSON Schema 2020-12 with format *assertions* (the
formats nexgen asserts) and ECMA/RE2 ``pattern`` semantics, resolving references only
inside the document -- the registry can retrieve nothing.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from jsonschema import ValidationError
from referencing import Registry, Resource
from referencing.jsonschema import DRAFT202012

from kontra.contract.canonical import check_json_value
from kontra.contract.errors import ROOT, ContractError, join_path
from kontra.contract.formats import ContractValidator, format_checker
from kontra.contract.validate import ContractInfo, validate_document

__all__ = ["MAX_VALUE_DEPTH", "InputValidator"]

#: Values nest at most this deep. jsonschema recurses per level, and a hostile payload of
#: a few hundred nested objects would otherwise end in RecursionError, not a refusal.
MAX_VALUE_DEPTH = 64


def _refuse(uri: str) -> Resource[Any]:
    raise LookupError(f"refusing to retrieve {uri!r}: contracts never fetch anything")


class InputValidator:
    """``InputValidator(doc).validate(value)`` raises :class:`ContractError` naming the
    first offending path (``input.urls[3]``); ``.errors`` on the exception lists all.

    The document is checked with :func:`validate_document` first (hash included), so a
    validator never trusts a document the catalog would refuse.
    """

    def __init__(self, doc: Mapping[str, Any] | ContractInfo) -> None:
        info = doc if isinstance(doc, ContractInfo) else validate_document(doc)
        self.info = info
        uri = f"urn:kontra:contract:{info.contract_hash.removeprefix('sha256:')}"
        body = {k: v for k, v in info.document.items() if k != "x-kontra"}
        registry: Registry[Any] = Registry(retrieve=_refuse).with_resource(
            uri, DRAFT202012.create_resource(body)
        )
        checker = format_checker()
        self._input = ContractValidator(
            {"$ref": f"{uri}#/$defs/{info.input_type}"}, registry=registry, format_checker=checker
        )
        self._output = ContractValidator(
            {"$ref": f"{uri}#/$defs/{info.output_type}"}, registry=registry, format_checker=checker
        )
        self._quote = ContractValidator(
            {"$ref": f"{uri}#/$defs/KontraQuote"}, registry=registry, format_checker=checker
        )

    @staticmethod
    def _check(validator: Any, value: Any, root: str) -> None:
        try:
            check_json_value(value, max_depth=MAX_VALUE_DEPTH)
        except ContractError as exc:
            path = (
                root
                if exc.path == ROOT
                else (root + exc.path if exc.path[0] == "[" else f"{root}.{exc.path}")
            )
            raise ContractError(path, exc.message) from None
        try:
            errors: list[ValidationError] = sorted(
                validator.iter_errors(value),
                key=lambda e: (list(map(str, e.absolute_path)), e.message),
            )
        except RecursionError:
            # Defence in depth: validate_document refuses reference loops and the value's
            # depth is capped, but a refusal must never surface as a crash.
            raise ContractError(root, "is too deeply nested to validate") from None
        if errors:
            listed = [(join_path(root, *e.absolute_path), e.message) for e in errors]
            path, message = listed[0]
            raise ContractError(path, message, errors=listed)

    def validate(self, value: Any) -> None:
        """The ``execute`` (and ``quote``) input, as JSON (dicts, lists, scalars)."""
        self._check(self._input, value, "input")

    def validate_output(self, value: Any) -> None:
        """The ``execute`` output, as JSON."""
        self._check(self._output, value, "output")

    def validate_quote(self, value: Any) -> None:
        """A ``quote`` answer, as JSON (``#/$defs/KontraQuote``)."""
        self._check(self._quote, value, "quote")
