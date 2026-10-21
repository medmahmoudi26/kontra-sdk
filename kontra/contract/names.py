"""Names: parties, offerings, versions, and the Temporal identifiers built from them.

Two rules keep this module from becoming a source of injection or confusion bugs:

1. **Identifiers are constructed, never parsed back.** A task queue, service name or
   workflow id is built from validated parts here and used as an opaque string everywhere
   else. No code splits one to recover the parts, and no security decision reads one.
2. **Nexus endpoint names carry no meaning.** An endpoint is ``k`` + 32 random hex
   characters (:func:`new_endpoint_name`), assigned by the catalog when a version is first
   published and created by the platform itself. The catalog maps endpoint <-> offering in
   both directions; nothing derives an offering from an endpoint name or authorizes by its
   prefix.

The only parsing left is of what people type (``acme/enrich@1.2``, ``1.2.0``, a handle), and
it is anchored, ASCII-only, ``fullmatch`` validation with length limits -- no stripping, no
normalising, no partial matches (``re.match`` with ``$`` would accept a trailing newline).

Temporal constraints the constructed strings satisfy (verified on server v1.32):
endpoint names match ``^[a-zA-Z][a-zA-Z0-9-]*[a-zA-Z0-9]$``; a ``/`` in a Nexus service name
breaks the server's HTTP routing, so the service is ``acme.enrich``; task queues accept any
string; nexgen wants service keys matching ``^[A-Z][a-zA-Z0-9]+$``.
"""

from __future__ import annotations

import re
import secrets
import uuid
from dataclasses import dataclass
from functools import total_ordering

__all__ = [
    "HANDLE_MAX",
    "PLATFORM_NAMESPACE",
    "PROGRESS_TOPIC",
    "RESERVED_HANDLES",
    "RESERVED_TOPIC_PREFIX",
    "STREAM_OFFSET_QUERY",
    "STREAM_POLL_UPDATE",
    "STREAM_PUBLISH_SIGNAL",
    "WORKER_CONTROLLER_SUBJECT",
    "NameError_",
    "OfferingId",
    "OfferingRef",
    "Version",
    "agent_workflow_id",
    "batch_workflow_id",
    "is_endpoint_name",
    "is_handle",
    "is_job_id",
    "job_id_from_workflow_id",
    "job_workflow_id",
    "new_endpoint_name",
    "new_job_id",
    "relay_subject",
    "validate_handle",
]

HANDLE_MAX = 32
_VERSION_PART_MAX = 999_999_999
_REF_MAX = 2 * HANDLE_MAX + 1 + 1 + 3 * 9 + 2

_HANDLE = r"[a-z][a-z0-9]*(?:-[a-z0-9]+)*"
_NUM = r"(?:0|[1-9][0-9]{0,8})"
_HANDLE_RE = re.compile(_HANDLE, re.ASCII)
_VERSION_RE = re.compile(rf"({_NUM})\.({_NUM})\.({_NUM})", re.ASCII)
_REF_RE = re.compile(
    rf"(?P<party>{_HANDLE})/(?P<name>{_HANDLE})(?:@(?P<ver>{_NUM}(?:\.{_NUM}){{0,2}}|latest))?",
    re.ASCII,
)
_HEX32_RE = re.compile(r"[0-9a-f]{32}", re.ASCII)
_ENDPOINT_RE = re.compile(r"k[0-9a-f]{32}", re.ASCII)
_JOB_WF_PREFIX = "job-"
#: What follows ``job-<id>`` in the id of a bound-handler job's agent child.
_AGENT_SUFFIX = "/agent"

#: Handles a party may never take: Temporal's own namespaces, and the platform's names.
RESERVED_HANDLES = frozenset(
    {
        "kontra",
        "kontra-system",
        "kontra-world",
        "temporal",
        "temporal-system",
        "default",
        "system",
        "admin",
        "root",
        "api",
        "www",
        "mcp",
        "gate",
        "console",
        "catalog",
    }
)


class NameError_(ValueError):
    """A name, version or reference that does not follow the rules in this module."""


def _full(pattern: re.Pattern[str], value: object, limit: int) -> re.Match[str] | None:
    if not isinstance(value, str) or len(value) > limit:
        return None
    return pattern.fullmatch(value)


def is_handle(value: object) -> bool:
    return _full(_HANDLE_RE, value, HANDLE_MAX) is not None


def validate_handle(value: str, *, what: str = "handle", allow_reserved: bool = False) -> str:
    if not is_handle(value):
        raise NameError_(
            f"{what} {value!r} must be lowercase ASCII letters, digits and single hyphens, "
            f"start with a letter, and be at most {HANDLE_MAX} characters"
        )
    if not allow_reserved and value in RESERVED_HANDLES:
        raise NameError_(f"{what} {value!r} is reserved")
    return value


@total_ordering
@dataclass(frozen=True, slots=True)
class Version:
    major: int
    minor: int
    patch: int

    def __post_init__(self) -> None:
        for part in (self.major, self.minor, self.patch):
            if (
                isinstance(part, bool)
                or not isinstance(part, int)  # pyright: ignore[reportUnnecessaryIsInstance]
                or not 0 <= part <= _VERSION_PART_MAX
            ):
                raise NameError_("version parts must be integers between 0 and 999999999")

    @classmethod
    def parse(cls, value: str) -> Version:
        m = _full(_VERSION_RE, value, 3 * 9 + 2)
        if m is None:
            raise NameError_(f"version {value!r} must be MAJOR.MINOR.PATCH (e.g. 1.2.0)")
        return cls(int(m.group(1)), int(m.group(2)), int(m.group(3)))

    def __str__(self) -> str:
        return f"{self.major}.{self.minor}.{self.patch}"

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, Version):
            return NotImplemented
        return (self.major, self.minor, self.patch) < (other.major, other.minor, other.patch)


@dataclass(frozen=True, slots=True)
class OfferingId:
    """One published version of an offering: ``acme/enrich@1.2.0``.

    The properties below *construct* Temporal identifiers. They are one-way: nothing in
    Kontra turns a task queue or service name back into an offering.
    """

    party: str
    name: str
    version: Version

    def __post_init__(self) -> None:
        validate_handle(self.party, what="party")
        validate_handle(self.name, what="offering name", allow_reserved=True)
        if not isinstance(self.version, Version):  # pyright: ignore[reportUnnecessaryIsInstance]
            raise NameError_("version must be a Version")

    @classmethod
    def parse(cls, value: str) -> OfferingId:
        ref = OfferingRef.parse(value)
        if ref.exact is None:
            raise NameError_(f"{value!r} must name an exact version, e.g. {ref.full}@1.2.0")
        return cls(ref.party, ref.name, ref.exact)

    @classmethod
    def from_parts(cls, full_name: str, version: str) -> OfferingId:
        """``("acme/enrich", "1.2.0")`` as written in offering.yaml."""
        ref = OfferingRef.parse(full_name)
        if ref.major is not None:
            raise NameError_(f"name {full_name!r} must not carry a version; put it in `version`")
        return cls(ref.party, ref.name, Version.parse(version))

    @property
    def full_name(self) -> str:
        """``acme/enrich`` -- the catalog name, without the version."""
        return f"{self.party}/{self.name}"

    @property
    def key(self) -> str:
        """``acme/enrich@1.2.0``."""
        return f"{self.party}/{self.name}@{self.version}"

    @property
    def namespace(self) -> str:
        """The seller's Temporal namespace. A party's namespace is its handle."""
        return self.party

    @property
    def task_queue(self) -> str:
        """Constructed, never configurable (PRD §6.5 item 4)."""
        return self.key

    @property
    def service_name(self) -> str:
        """The Nexus service name on the wire (the nexgen ``fqn``)."""
        return f"{self.party}.{self.name}"

    @property
    def service_key(self) -> str:
        """The nexgen service key: ``acme/enrich`` -> ``AcmeEnrich``. Only unique within one
        contract document, which holds exactly one service."""
        return "".join(p[:1].upper() + p[1:] for p in self.full_name.replace("/", "-").split("-"))

    def __str__(self) -> str:
        return self.key


@dataclass(frozen=True, slots=True)
class OfferingRef:
    """What a buyer writes: ``acme/enrich``, ``acme/enrich@1``, ``acme/enrich@1.2`` or
    ``acme/enrich@1.2.0`` (``@latest`` is the same as none). Anything short of an exact version
    resolves to the latest published version matching the given prefix (§7.4)."""

    party: str
    name: str
    major: int | None = None
    minor: int | None = None
    patch: int | None = None

    @classmethod
    def parse(cls, value: str) -> OfferingRef:
        m = _full(_REF_RE, value, _REF_MAX)
        if m is None or not is_handle(m.group("party")) or not is_handle(m.group("name")):
            raise NameError_(
                f"{value!r} is not an offering reference; expected party/name[@version], "
                "e.g. acme/enrich or acme/enrich@1.2.0"
            )
        ver = m.group("ver")
        nums: list[int | None] = [] if ver in (None, "latest") else [int(x) for x in ver.split(".")]
        nums += [None] * (3 - len(nums))
        return cls(m.group("party"), m.group("name"), nums[0], nums[1], nums[2])

    @property
    def full(self) -> str:
        return f"{self.party}/{self.name}"

    @property
    def exact(self) -> Version | None:
        if self.major is None or self.minor is None or self.patch is None:
            return None
        return Version(self.major, self.minor, self.patch)

    def matches(self, version: Version) -> bool:
        return (
            (self.major is None or self.major == version.major)
            and (self.minor is None or self.minor == version.minor)
            and (self.patch is None or self.patch == version.patch)
        )

    def resolve(self, published: list[Version]) -> Version | None:
        """The highest published version this reference accepts."""
        candidates = [v for v in published if self.matches(v)]
        return max(candidates) if candidates else None

    def __str__(self) -> str:
        nums = [n for n in (self.major, self.minor, self.patch) if n is not None]
        return self.full + ("@" + ".".join(map(str, nums)) if nums else "")


def new_endpoint_name() -> str:
    """A fresh, meaningless Nexus endpoint name. The catalog assigns one per published
    version and is the only map between it and the offering."""
    return "k" + secrets.token_hex(16)


def is_endpoint_name(value: object) -> bool:
    """Shape check for input validation only; it says nothing about which offering."""
    return _full(_ENDPOINT_RE, value, 33) is not None


def new_job_id() -> str:
    return uuid.uuid4().hex


def is_job_id(value: object) -> bool:
    return _full(_HEX32_RE, value, 32) is not None


def job_workflow_id(job_id: str) -> str:
    """The seller's handler workflow id for a job (§6.5 item 1)."""
    if not is_job_id(job_id):
        raise NameError_(f"{job_id!r} is not a job id")
    return _JOB_WF_PREFIX + job_id


#: Temporal identities that are not parties. Each contains "/", which no handle can
#: (``_HANDLE``), so no party token's ``sub`` is ever one of them (temporal/authorizer,
#: decisions I30).
WORKER_CONTROLLER_SUBJECT = "platform/worker-controller"
#: Kontra's own Temporal namespace (KON-15); a reserved handle, so no party can take it.
PLATFORM_NAMESPACE = "kontra-system"
#: The two workflow-stream handlers a relay may use (``temporalio.contrib.workflow_streams``
#: wire names, Public Preview; pinned by tests/contract/test_names.py). The gate depends on
#: these names, never on the contrib package.
STREAM_POLL_UPDATE = "__temporal_workflow_stream_poll"
STREAM_OFFSET_QUERY = "__temporal_workflow_stream_offset"
#: The stream's publish signal: what a seller's publishers (and Kontra's progress forwarder
#: and turn mirror) send to ``job-<id>``. Never a relay's: the authorizer refuses it one.
STREAM_PUBLISH_SIGNAL = "__temporal_workflow_stream_publish"
#: The topic of a job's stream that carries the seller's heartbeats as progress
#: (:class:`kontra.serve.progress.ProgressForwarder`): relayed for every job, whatever the
#: offering lists (decisions I31).
PROGRESS_TOPIC = "kontra.progress"
#: Topic names an offering may not list (``offering.yaml`` ``streams:``): Kontra's own.
RESERVED_TOPIC_PREFIX = "kontra."


def relay_subject(namespace: str, job_id: str) -> str:
    """The Temporal subject of the gate's stream relay for one job:
    ``relay/<namespace>/job-<job id>``.

    Constructed, never parsed (I1): the authorizer rebuilds it from a request's namespace and
    workflow id and compares the whole string (temporal/authorizer ``RelaySubject``). The
    namespace must be a party's (a handle, not a reserved one) and the job id a job id, so
    neither part holds a ``/`` and only one job's requests rebuild it.
    """
    validate_handle(namespace, what="namespace")
    return "relay/" + namespace + "/" + job_workflow_id(job_id)


def batch_workflow_id(parent_workflow_id: str, activity: str, token: str) -> str:
    """The id of the ``kontra.batch.v1`` child a workflow starts for
    ``execute_activities_in_parallel(activity, ...)`` (:mod:`kontra.contrib.parallel`): the
    parent's id, the activity's Temporal name, and ``token`` (one ``workflow.uuid4()`` of the
    parent's, so batching the same activity twice in one run gets two ids). Never the shape
    of a job's workflow id, so :func:`job_id_from_workflow_id` reads none from it."""
    return f"{parent_workflow_id}/batch/{activity}/{token}"


def agent_workflow_id(job_id: str) -> str:
    """The id of the agent workflow a bound-handler job runs its one turn in
    (:mod:`kontra.serve.turn`): a child of the job's ``job-<id>`` wrapper. Never the shape of a
    job's workflow id, so :func:`job_id_from_workflow_id` reads none from it and the codec
    encrypts the agent's payloads to the seller alone -- the buyer receives the job's result
    from the wrapper, never the agent's history."""
    return job_workflow_id(job_id) + _AGENT_SUFFIX


def job_id_from_workflow_id(workflow_id: object) -> str | None:
    """The job id whose handler workflow id is exactly ``workflow_id``, else None.

    Construct-and-compare rather than split: the candidate is accepted only if building the
    workflow id from it reproduces the input byte for byte.
    """
    if not isinstance(workflow_id, str) or len(workflow_id) != len(_JOB_WF_PREFIX) + 32:
        return None
    candidate = workflow_id[len(_JOB_WF_PREFIX) :]
    if is_job_id(candidate) and _JOB_WF_PREFIX + candidate == workflow_id:
        return candidate
    return None
