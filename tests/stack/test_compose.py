"""The e2e stack's ``compose.yaml`` and the shipped ``observability/compose.yaml``.

``compose.yaml`` is repository tooling since decisions I34 (``make stack-up``/``make e2e``
through ``tests/e2e/stack.py``; kontra-cloud's ``make up`` overlays it), never ``kontra dev``.
It is validated by ``docker compose config`` itself (skipped without Docker): it must refuse
to run without its secrets, publish nothing but the public ports and only on the host's
loopback by default, never publish Temporal's internal frontend nor put Temporal on a network
with anything but the gate and its database relay, run every service without privileges, pin
its third-party images by digest, and hand the gate every setting it needs. The observability
file ``kontra dev up --observability`` runs is held to its six services, its walls and its
wiring, and names nothing of the prod gate or of Temporal. (Moved from
``tests/cli/test_dev.py``, which drove ``kontra dev`` through this file before I34.)
"""

from __future__ import annotations

import ipaddress
import json
import os
import re
import shutil
import subprocess
import uuid
from pathlib import Path
from typing import Any

import pytest

from kontra.devgate.state import Ports

REPO = Path(__file__).resolve().parents[2]
COMPOSE = REPO / "compose.yaml"
OBSERVABILITY = REPO / "observability" / "compose.yaml"
#: The one profile left in compose.yaml (`make stack-up UI=1`).
PROFILES = ("ui",)
REQUIRED = {
    "KONTRA_PG_PASSWORD": "pw-test",
    "KONTRA_S3_ACCESS_KEY": "ak-test",
    "KONTRA_S3_SECRET_KEY": "sk-test",
}
#: subprocess.run before any test replaces it with a Recorder.
_REAL_RUN = subprocess.run


def _docker_compose() -> bool:
    if shutil.which("docker") is None:
        return False
    probe = subprocess.run(
        ["docker", "compose", "version"], capture_output=True, timeout=60, check=False
    )
    return probe.returncode == 0


needs_compose = pytest.mark.skipif(not _docker_compose(), reason="needs docker compose")


def _config(
    tmp_path: Path, env: dict[str, str], profiles: tuple[str, ...] = ()
) -> subprocess.CompletedProcess[str]:
    env_file = tmp_path / "test.env"
    env_file.write_text("".join(f"{k}={v}\n" for k, v in env.items()))
    # Nothing from the developer's shell may override the file under test.
    clean = {k: v for k, v in os.environ.items() if not k.startswith("KONTRA_")}
    return subprocess.run(
        [
            *("docker", "compose", "-p", "kontra-config-test", "-f", str(COMPOSE)),
            *("--env-file", str(env_file)),
            *(arg for profile in profiles for arg in ("--profile", profile)),
            *("config", "--format", "json"),
        ],
        capture_output=True,
        text=True,
        env=clean,
        timeout=120,
        check=False,
    )


def _resolved(tmp_path: Path, **extra: str) -> dict[str, Any]:
    proc = _config(tmp_path, {**REQUIRED, **extra})
    assert proc.returncode == 0, proc.stderr
    return json.loads(proc.stdout)


def _with_profiles(tmp_path: Path, **extra: str) -> dict[str, Any]:
    """The configuration with its profile enabled (`make stack-up UI=1`)."""
    proc = _config(tmp_path, {**REQUIRED, **extra}, PROFILES)
    assert proc.returncode == 0, proc.stderr
    return json.loads(proc.stdout)


@needs_compose
def test_compose_refuses_to_run_without_its_secrets(tmp_path: Path) -> None:
    for missing in REQUIRED:
        env = {k: v for k, v in REQUIRED.items() if k != missing}
        proc = _config(tmp_path, env)
        assert proc.returncode != 0
        assert missing in proc.stderr


@needs_compose
def test_compose_publishes_only_public_ports_on_loopback(tmp_path: Path) -> None:
    config = _resolved(tmp_path)
    services = config["services"]
    assert set(services) == {
        *("postgres", "postgres-relay", "s3-owner", "s3", "temporal", "migrate", "gate"),
    }
    published: dict[str, list[tuple[str, str, int]]] = {
        name: [(p["host_ip"], str(p["published"]), int(p["target"])) for p in svc.get("ports", [])]
        for name, svc in services.items()
    }
    assert published == {
        "postgres": [("127.0.0.1", "5432", 5432)],
        "postgres-relay": [],
        "s3-owner": [],
        "s3": [("127.0.0.1", "8333", 8333)],
        "temporal": [("127.0.0.1", "7233", 7233)],  # the public frontend only
        "migrate": [],
        "gate": [("127.0.0.1", "8080", 8080)],
    }
    temporal = services["temporal"]["environment"]
    # The internal frontend (no authentication) is unpublished, and listens only on
    # Temporal's fixed address on `control`, the network it shares with the gate alone
    # (test_the_internal_frontend_listens_on_the_control_network_only) ...
    assert temporal["KONTRA_INTERNAL_FRONTEND_BIND_IP"] == "172.16.251.2"
    assert all(target not in (7236, 7246) for ports in published.values() for _, _, target in ports)
    assert temporal["KONTRA_JWKS_URLS"] == "http://gate:8080/.well-known/jwks.json"
    # ... and Temporal shares a network with the gate and its database relay only: not with
    # Postgres or the migrate job, and not with SeaweedFS, which serves the internet in
    # production and whose own master/volume/filer ports only the gate may reach.
    networks = {name: set(svc.get("networks", ())) for name, svc in services.items()}
    assert networks == {
        "postgres": {"database"},
        "postgres-relay": {"temporal-db", "database"},
        "s3-owner": set(),  # network_mode: none
        "s3": {"bucket"},
        "temporal": {"control", "temporal-db"},
        "migrate": {"database"},
        "gate": {"control", "database", "bucket"},
    }
    peers = {name for name, nets in networks.items() if nets & networks["temporal"]}
    assert peers == {"temporal", "gate", "postgres-relay"}
    assert {name for name, nets in networks.items() if nets & networks["s3"]} == {"s3", "gate"}
    assert all(not config["networks"][net].get("external") for net in config["networks"])
    # Temporal's database connections go through the relay, which only connects out.
    assert temporal["KONTRA_DB_HOST"] == "postgres-relay"
    relay = services["postgres-relay"]
    assert relay["entrypoint"] == ["nc"]
    assert relay["command"] == ["-lk", "-p", "5432", "-e", "nc", "postgres", "5432"]
    assert (relay["cap_drop"], relay["read_only"], relay["user"]) == (["ALL"], True, "postgres")
    assert relay["stop_signal"] == "SIGTERM"  # the image's SIGINT does not stop nc
    # A tmpfs over the image's VOLUME, or every relay container leaves an anonymous volume.
    assert relay["tmpfs"] == ["/var/lib/postgresql:size=1m"]
    assert relay["image"] == services["postgres"]["image"]  # the stack trusts no further image
    # A container inherits the host's ip_forward=1: a service on two networks would route
    # between them (a peer on `database` routing through the relay reaches Temporal's
    # temporal-db address one way). Every multi-homed service turns forwarding off.
    multi_homed = {name for name, nets in networks.items() if len(nets) > 1}
    assert multi_homed == {"postgres-relay", "temporal", "gate"}
    for name in multi_homed:
        assert services[name].get("sysctls") == {"net.ipv4.ip_forward": "0"}, name
    assert (temporal["KONTRA_DB"], temporal["KONTRA_AUTO_SETUP"]) == ("postgres", "1")
    # Production-safe defaults: not a dev gate unless an env file says so.
    assert services["gate"]["environment"]["KONTRA_DEV"] == "0"
    assert services["gate"]["image"] == "ghcr.io/medmahmoudi26/kontra-gate:dev"
    assert services["temporal"]["image"] == "ghcr.io/medmahmoudi26/kontra-temporal:dev"


@needs_compose
def test_every_service_runs_without_privileges(tmp_path: Path) -> None:
    """No capabilities and no way to gain any, in every service: code that takes one over
    cannot open raw sockets (ARP or DNS spoofing on the shared networks), change file
    ownership or use a setuid binary. A read-only root filesystem everywhere, with a tmpfs
    only where a service must write outside its volume, so nothing it writes outlives a
    restart. Proven on a running stack (dev login, a worker token accepted by Temporal,
    artifact uploads and downloads, a hire; Temporal's distroless image too) before it was
    written down here."""
    services = _resolved(tmp_path)["services"]
    owner = services["s3-owner"]
    for name, svc in services.items():
        assert svc.get("cap_drop") == ["ALL"], name
        if name != "s3-owner":
            assert not svc.get("cap_add"), f"{name} adds back {svc.get('cap_add')}"
        assert svc.get("security_opt") == ["no-new-privileges:true"], name
        assert not svc.get("privileged"), name
        assert svc.get("read_only") is True, name
        # None of it would hold with a host namespace, a device or a host path (the Docker
        # socket is one): named volumes and tmpfs only.
        for key in ("pid", "ipc", "uts", "userns_mode", "cgroup", "devices"):
            assert not svc.get(key), f"{name} sets {key}: {svc.get(key)}"
        assert svc.get("network_mode") in (None, "none"), name
        assert all(v["type"] == "volume" for v in svc.get("volumes", [])), name
    # The one exception, and why: s3-owner hands the bucket's volume to SeaweedFS's uid.
    # Two capabilities, one fixed command, no network, then it exits.
    assert owner["cap_add"] == ["CHOWN", "DAC_READ_SEARCH"]
    assert owner["entrypoint"] == ["chown", "-R", "10001:10001", "/data"]
    assert (owner["network_mode"], owner["restart"], owner.get("command")) == ("none", "no", None)
    assert [(v["source"], v["target"]) for v in owner["volumes"]] == [("s3", "/data")]
    assert owner["image"] == services["s3"]["image"]  # the stack trusts no further image
    s3_deps = services["s3"]["depends_on"]
    assert set(s3_deps) == {"s3-owner"}
    assert s3_deps["s3-owner"]["condition"] == "service_completed_successfully"
    # Every tmpfs is capped: a service taken over cannot fill the host's memory.
    tmpfs = {name: svc["tmpfs"] for name, svc in services.items() if svc.get("tmpfs")}
    assert tmpfs == {
        "postgres": ["/var/run/postgresql:size=1m"],  # its socket and lock file
        # the image's VOLUME, unused (18 raised it from .../data)
        "postgres-relay": ["/var/lib/postgresql:size=1m"],
        "s3": ["/tmp:size=1m"],  # the identities config and SeaweedFS's unix sockets
    }
    # No long-running service as root. Postgres as its own user: its entrypoint needs root
    # only to chown what the named volume already gives postgres. SeaweedFS as the uid
    # s3-owner gave the volume to (the image has no user of its own); the gate and Temporal
    # images set a non-root user of their own.
    users = {name: svc["user"] for name, svc in services.items() if "user" in svc}
    assert users == {"postgres": "postgres", "postgres-relay": "postgres", "s3": "10001:10001"}
    assert owner["entrypoint"][2] == services["s3"]["user"]
    # 18 keeps the cluster at /var/lib/postgresql/<major>/docker, inside this mount.
    assert {v["target"] for v in services["postgres"]["volumes"]} == {"/var/lib/postgresql"}
    assert {v["target"] for v in services["s3"]["volumes"]} == {"/data"}


@needs_compose
def test_the_internal_frontend_listens_on_the_control_network_only(tmp_path: Path) -> None:
    """Temporal binds its internal frontend (no authentication) to its fixed address on
    `control`, the network it shares with the gate alone, so postgres-relay on
    `temporal-db` has nothing listening to connect to. The subnet is pinned for that
    (Temporal must know the address before it starts), lies outside Docker's default pools,
    and moves as a whole with KONTRA_CONTROL_NET.

    This checks the configuration; the running stack was checked by hand (the review
    reports): the server listens on 7236/7246 at that address only, the relay, Postgres and
    SeaweedFS reach neither port at any address while the relay does reach the public
    frontend, and the gate works end to end. Measured there too: the bind is not the whole
    wall. Linux delivers a packet for any of its addresses on any interface, so a peer on
    `temporal-db` with NET_ADMIN that routes the `control` address through Temporal's
    `temporal-db` address connects to 7236 and 7246. What closes it is that every peer of
    Temporal off `control` can neither add that route (NET_ADMIN) nor forge the frame
    (NET_RAW): no capability at all, and not root."""
    for prefix, extra in (("172.16.251", {}), ("10.213.77", {"KONTRA_CONTROL_NET": "10.213.77"})):
        config = _resolved(tmp_path, **extra)
        [ipam] = config["networks"]["control"]["ipam"]["config"]
        subnet = ipaddress.ip_network(ipam["subnet"])
        dynamic = ipaddress.ip_network(ipam["ip_range"])
        assert (str(subnet), str(dynamic)) == (f"{prefix}.0/28", f"{prefix}.8/29")
        temporal = config["services"]["temporal"]
        address = ipaddress.ip_address(temporal["networks"]["control"]["ipv4_address"])
        assert str(address) == temporal["environment"]["KONTRA_INTERNAL_FRONTEND_BIND_IP"]
        assert address in subnet
        assert address not in dynamic  # never handed to another container
        assert temporal["networks"]["temporal-db"] in ({}, None)
        # The gate dials it by name, which resolves to that address on `control`.
        gate = config["services"]["gate"]["environment"]
        assert gate["KONTRA_TEMPORAL_INTERNAL"] == "temporal:7236"
        # Every other member of a network Temporal is on, but `control`, can reach
        # Temporal's other addresses: none may route (or forge) its way to the bind.
        services = config["services"]
        shared = set(temporal["networks"]) - {"control"}
        peers = {
            name
            for name, svc in services.items()
            if name != "temporal" and shared & set(svc.get("networks") or ())
        }
        assert peers == {"postgres-relay"}
        for name in peers:
            svc = services[name]
            assert (svc.get("cap_drop"), svc.get("cap_add")) == (["ALL"], None), name
            assert svc.get("user") not in (None, "", "root", "0", "0:0"), name
            assert svc.get("security_opt") == ["no-new-privileges:true"], name
            assert not svc.get("privileged"), name
    default = ipaddress.ip_network("172.16.251.0/28")
    pools = [ipaddress.ip_network(f"172.{n}.0.0/16") for n in range(17, 32)]
    assert not any(
        default.overlaps(pool) for pool in [*pools, ipaddress.ip_network("192.168.0.0/16")]
    )


@needs_compose
def test_seaweedfs_runs_unprivileged_on_the_volume_s3_owner_hands_it(tmp_path: Path) -> None:
    """In the pinned image, under the services' own restrictions: a new volume is root's
    (Docker fills it from the image's /data); s3-owner hands it over, and SeaweedFS's uid
    can then write it. On every later start the volume holds directories SeaweedFS
    created 0700, which CHOWN alone cannot walk: hence DAC_READ_SEARCH."""
    services = _resolved(tmp_path)["services"]
    owner, s3 = services["s3-owner"], services["s3"]
    image = s3["image"]
    if _REAL_RUN(["docker", "image", "inspect", image], capture_output=True).returncode != 0:
        pytest.skip(f"{image} is not pulled here")
    volume = f"kontra-test-s3-{uuid.uuid4().hex[:12]}"
    locked = [*(f"--cap-drop={c}" for c in owner["cap_drop"]), "--read-only", "--network=none"]
    locked += [f"--security-opt={o}" for o in owner["security_opt"]]

    def run(*args: str) -> subprocess.CompletedProcess[str]:
        return _REAL_RUN(
            ["docker", "run", "--rm", *locked, f"--volume={volume}:/data", *args],
            capture_output=True,
            text=True,
            timeout=120,
            check=False,
        )

    def hand_over(caps: list[str]) -> subprocess.CompletedProcess[str]:
        entry, *args = owner["entrypoint"]
        return run(*(f"--cap-add={c}" for c in caps), f"--entrypoint={entry}", image, *args)

    as_seaweedfs = (f"--user={s3['user']}", "--entrypoint=/bin/sh", image, "-c")
    try:
        fresh = run(*as_seaweedfs, "touch /data/x")
        assert fresh.returncode != 0  # the image's root-owned /data
        assert hand_over(owner["cap_add"]).returncode == 0
        wrote = run(*as_seaweedfs, "mkdir -m 700 /data/own && touch /data/own/f && id -u")
        assert (wrote.returncode, wrote.stdout.strip()) == (0, s3["user"].split(":")[0])
        assert hand_over(["CHOWN"]).returncode != 0  # cannot walk /data/own
        assert hand_over(owner["cap_add"]).returncode == 0  # every later start
    finally:
        _REAL_RUN(["docker", "volume", "rm", "-f", volume], capture_output=True, check=False)


def _s3_entrypoint(services: dict[str, Any]) -> list[str]:
    """The s3 service's entrypoint as the container receives it: ``docker compose config``
    prints a literal ``$`` escaped as ``$$``."""
    return [arg.replace("$$", "$") for arg in services["s3"]["entrypoint"]]


@needs_compose
def test_s3_credentials_reach_seaweedfs_without_a_writable_root(tmp_path: Path) -> None:
    """Compose's ``configs`` copy their content into the container's root filesystem, which
    read_only refuses; so the s3 service hands the identities config over in its
    environment, and its entrypoint writes it to the tmpfs (0600) and drops the variable
    before the image's own entrypoint starts SeaweedFS with that file."""
    config = _resolved(tmp_path)
    assert "configs" not in config
    s3 = config["services"]["s3"]
    identities = json.loads(s3["environment"]["KONTRA_S3_IDENTITIES"])
    assert identities["identities"][0]["credentials"] == [
        {"accessKey": "ak-test", "secretKey": "sk-test"}
    ]
    assert "-s3.config=/tmp/s3.json" in s3["command"]
    entrypoint = _s3_entrypoint(config["services"])
    assert entrypoint[:2] == ["/bin/sh", "-c"]
    assert entrypoint[2].endswith('exec /entrypoint.sh "$@"')

    # The snippet itself, in the pinned image under the service's own restrictions, with a
    # stand-in for the image's entrypoint that reports what SeaweedFS would be started with.
    image = s3["image"]
    if _REAL_RUN(["docker", "image", "inspect", image], capture_output=True).returncode != 0:
        pytest.skip(f"{image} is not pulled here")
    probe = tmp_path / "entrypoint.sh"
    probe.write_text(
        "#!/bin/sh\n"
        'echo "args: $*"\n'
        'echo "env: ${KONTRA_S3_IDENTITIES-unset}"\n'
        "stat -c 'mode: %a' /tmp/s3.json\n"
        "cat /tmp/s3.json\n"
    )
    probe.chmod(0o755)
    flags = [f"--tmpfs={path}" for path in s3["tmpfs"]]
    flags += [f"--cap-drop={cap}" for cap in s3["cap_drop"]]
    flags += [f"--user={s3['user']}"]
    flags += [f"--security-opt={opt}" for opt in s3["security_opt"]]
    proc = _REAL_RUN(
        [
            *("docker", "run", "--rm", "--read-only", "--network=none", *flags),
            "--tmpfs=/data",  # in place of the named volume: nothing left behind
            f"--volume={probe}:/entrypoint.sh:ro",
            f"--env=KONTRA_S3_IDENTITIES={s3['environment']['KONTRA_S3_IDENTITIES']}",
            f"--entrypoint={entrypoint[0]}",
            *(image, *entrypoint[1:], *s3["command"]),
        ],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr
    lines = proc.stdout.splitlines()
    assert lines[:3] == ["args: " + " ".join(s3["command"]), "env: unset", "mode: 600"]
    assert json.loads("\n".join(lines[3:])) == identities


_PROXY_VARS = ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy")
#: A proxy nothing can reach: whatever is sent to it fails.
_PROXY = "http://proxy.invalid:3128"


def _pulled(image: str) -> bool:
    return _REAL_RUN(["docker", "image", "inspect", image], capture_output=True).returncode == 0


def _proxied_client(tmp_path: Path) -> dict[str, str]:
    """The environment of a Docker client configured with :data:`_PROXY` (its
    ``config.json``), which compose copies into every container it creates; nothing proxy
    related comes from our own environment."""
    client = tmp_path / "docker-config"
    client.mkdir()
    (client / "config.json").write_text(
        json.dumps({"proxies": {"default": {"httpProxy": _PROXY, "httpsProxy": _PROXY}}})
    )
    # The compose plugin is found through the client's config directory.
    real = Path(os.environ.get("DOCKER_CONFIG") or Path.home() / ".docker") / "cli-plugins"
    if real.is_dir():
        (client / "cli-plugins").symlink_to(real)
    clean = {
        k: v
        for k, v in os.environ.items()
        if k.upper() not in (*_PROXY_VARS, "NO_PROXY", "ALL_PROXY")
    }
    return {**clean, "DOCKER_CONFIG": str(client)}


@needs_compose
def test_temporal_never_sends_its_internal_traffic_to_a_proxy(tmp_path: Path) -> None:
    """Temporal dials its internal frontend where it binds it, its `control` address here,
    and Go exempts only loopback from HTTP(S)_PROXY: a proxy in its environment received the
    system worker's gRPC (the server then did not start) and would receive history's Nexus
    tasks and completion tokens. Compose copies a Docker client's proxy settings into every
    container, so the service sets all four variables empty; Go reads the upper and the
    lower case, the first one non-empty.

    The second half proves on this compose that an empty value wins over the injected one,
    with the service's own entries, a client configured with a proxy, and the pinned
    Postgres image (it has ``env``) standing in for Temporal's."""
    services = _resolved(tmp_path)["services"]
    env = services["temporal"]["environment"]
    assert {var: env.get(var) for var in _PROXY_VARS} == dict.fromkeys(_PROXY_VARS, "")

    image = services["postgres"]["image"]
    if not _pulled(image):
        pytest.skip(f"{image} is not pulled here")
    client = _proxied_client(tmp_path)
    proxy = _PROXY
    probe = tmp_path / "probe.yaml"
    probe.write_text(
        json.dumps(
            {
                "services": {
                    "probe": {
                        "image": image,
                        "entrypoint": ["env"],
                        "network_mode": "none",
                        "environment": {var: env[var] for var in _PROXY_VARS},
                    }
                }
            }
        )
    )

    def run(config: Path) -> dict[str, str]:
        proc = _REAL_RUN(
            [
                *("docker", "compose", "-p", "kontra-proxy-test", "-f", str(config)),
                *("run", "--rm", "--no-deps", "-T", "probe"),
            ],
            capture_output=True,
            text=True,
            env=client,
            timeout=120,
            check=False,
        )
        assert proc.returncode == 0, proc.stderr
        seen = dict(line.split("=", 1) for line in proc.stdout.splitlines() if "=" in line)
        return {var: seen.get(var, "<unset>") for var in _PROXY_VARS}

    assert run(probe) == dict.fromkeys(_PROXY_VARS, "")
    # Control: without those entries the client's proxy does reach the container.
    bare = tmp_path / "bare.yaml"
    bare.write_text(probe.read_text().replace('"environment"', '"labels"'))
    assert run(bare) == dict.fromkeys(_PROXY_VARS, proxy)


#: The healthchecks before they turned proxies off: what a check does that honours the
#: proxy compose copies into its container.
_PROXIED_CHECKS = {
    "s3": ["CMD-SHELL", "wget -q -O /dev/null http://127.0.0.1:8333/healthz"],
    "gate": [
        "CMD",
        *("python3", "-c"),
        "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8080/healthz', timeout=3)",
    ],
}
#: What answers each check where its service would, run in the service's own image (the
#: services need each other, and SeaweedFS half a minute): busybox nc reads the request and
#: answers 200; Python serves /healthz.
_STAND_INS: dict[str, list[str]] = {
    "s3": [
        *("nc", "-lk", "-p", "8333", "-e", "sh", "-c"),
        # $$: compose would interpolate a single one.
        'while IFS= read -r l && [ "$${l%?}" ]; do :; done;'
        " printf 'HTTP/1.0 200 OK\\r\\nContent-Length: 0\\r\\n\\r\\n'",
    ],
    "gate": [
        *("python3", "-c"),
        "import http.server as h\n"
        "class H(h.BaseHTTPRequestHandler):\n"
        "    def do_GET(self):\n"
        "        self.send_response(200 if self.path == '/healthz' else 404)\n"
        "        self.end_headers()\n"
        "h.HTTPServer(('127.0.0.1', 8080), H).serve_forever()\n",
    ],
}


@needs_compose
def test_healthchecks_never_go_through_a_proxy(tmp_path: Path) -> None:
    """A healthcheck runs in its container's environment, where compose has copied a
    Docker client's proxy settings, and wget and urllib send even 127.0.0.1 to that proxy:
    on a machine with a client proxy the bucket and the gate never became healthy, and
    the stack's `up` waited for nothing. Compose has no environment for a healthcheck
    alone, and the gate's own must stay as the operator set it (its internet egress,
    GitHub and Stripe, goes through an operator's proxy), so each HTTP check turns
    proxies off itself (busybox wget ``-Y off``, an empty urllib ``ProxyHandler``),
    whatever its service's environment holds (the bucket's own is proxy-free too, for
    SeaweedFS's sake: :func:`test_the_bucket_never_sends_its_own_traffic_to_a_proxy`).
    pg_isready speaks to Postgres's local socket, never through a proxy.

    The second half runs both checks, exactly as compose resolved them, as healthchecks of
    containers of their services' images created under a client configured with a proxy,
    each against a stand-in answering on its service's port: they become healthy. In the
    same containers the previous checks fail: the proxy is in their environment."""
    services = _resolved(tmp_path)["services"]
    checks = {
        name: svc["healthcheck"]["test"]
        for name, svc in services.items()
        if "healthcheck" in svc and not svc["healthcheck"].get("disable")
    }
    assert set(checks) == {"postgres", "s3", "gate"}
    kind, line = checks["postgres"]
    assert (kind, line.split()[0]) == ("CMD-SHELL", "pg_isready")
    assert " -h" not in line  # the local socket
    wget = ("wget", "-Y", "off", "-q", "-O", "/dev/null", "http://127.0.0.1:8333/healthz")
    assert checks["s3"] == ["CMD", *wget]
    assert checks["gate"][:3] == ["CMD", "python3", "-c"]
    assert "u.build_opener(u.ProxyHandler({})).open(" in checks["gate"][3]
    gate_env = services["gate"].get("environment") or {}  # the gate itself is left alone
    assert not [k for k in gate_env if k.upper() in (*_PROXY_VARS, "NO_PROXY", "ALL_PROXY")]

    s3_image = services["s3"]["image"]
    if not _pulled(s3_image):
        pytest.skip(f"{s3_image} is not pulled here")
    gate_images = [os.environ.get("KONTRA_GATE_IMAGE"), services["gate"]["image"]]
    gate_image = next((i for i in [*gate_images, "kontra-gate:dev"] if i and _pulled(i)), None)
    images = {"s3": s3_image} | ({"gate": gate_image} if gate_image else {})
    probes = {
        name: {
            "image": image,
            "entrypoint": _STAND_INS[name],
            "network_mode": "none",  # nothing leaves: the proxy is unreachable too
            "healthcheck": {"test": checks[name], "interval": "1s", "retries": 30},
        }
        for name, image in images.items()
    }
    probes["s3"]["tmpfs"] = ["/data"]  # the image's VOLUME: no anonymous volume to leak
    config = tmp_path / "health.yaml"
    config.write_text(json.dumps({"services": probes}))
    client = _proxied_client(tmp_path)
    compose = (
        *("docker", "compose", "-p", f"kontra-health-test-{uuid.uuid4().hex[:8]}"),
        *("-f", str(config)),
    )

    def run(*args: str, timeout: float = 120) -> subprocess.CompletedProcess[str]:
        return _REAL_RUN(
            [*compose, *args],
            capture_output=True,
            text=True,
            env=client,
            timeout=timeout,
            check=False,
        )

    try:
        up = run("up", "-d", "--wait", "--wait-timeout", "60", timeout=180)
        assert up.returncode == 0, up.stderr + run("logs").stdout
        for name in probes:
            seen = run("exec", "-T", name, "printenv", "http_proxy")
            assert seen.stdout.strip() == _PROXY  # what the checks ran with
            proxied = _PROXIED_CHECKS[name]
            argv = ["sh", "-c", proxied[1]] if proxied[0] == "CMD-SHELL" else proxied[1:]
            assert run("exec", "-T", name, *argv).returncode != 0, name
            assert run("exec", "-T", name, *checks[name][1:]).returncode == 0, name
    finally:
        down = run("down", "-v", "--remove-orphans", "-t", "1")
        assert down.returncode == 0, down.stderr
    if gate_image is None:
        pytest.skip("the gate's check ran statically only: no gate image is pulled here")


@needs_compose
def test_the_bucket_never_sends_its_own_traffic_to_a_proxy(tmp_path: Path) -> None:
    """SeaweedFS's master, volume server, filer and S3 gateway dial one another over gRPC
    at the container's own address, which Go does not exempt from HTTP(S)_PROXY as it does
    loopback. Under a Docker client configured with a proxy (compose copies it into every
    container) they reached nothing: the bucket never became healthy, so neither did the
    gate, whatever the healthchecks did. The service sets all four variables empty, as
    Temporal does; SeaweedFS connects to nothing outside its container.

    The second half runs the service as compose resolved it (entrypoint, command,
    environment, user, read-only root, healthcheck) on a network of its own, a tmpfs for its
    volume, under a client configured with an unreachable proxy: it becomes healthy, with the
    proxy cleared. The control, the same service without those four entries, shows the client
    really did inject the proxy. It no longer shows SeaweedFS dialling it: since 4.x its
    processes reach the master over a Unix socket and came up either way (measured, 4.48)."""
    s3 = _resolved(tmp_path)["services"]["s3"]
    env = s3["environment"]
    assert {var: env.get(var) for var in _PROXY_VARS} == dict.fromkeys(_PROXY_VARS, "")
    if not _pulled(s3["image"]):
        pytest.skip(f"{s3['image']} is not pulled here")
    kept = ("image", "entrypoint", "command", "environment", "user", "read_only", "cap_drop")
    fixed: dict[str, Any] = {k: s3[k] for k in (*kept, "security_opt") if k in s3}
    # The image's VOLUME as a tmpfs owned by the service's user (s3-owner's part): no
    # anonymous volume to leak. (Docker copies the image's root-owned /data mode onto it.)
    uid, gid = s3["user"].split(":")
    fixed["tmpfs"] = [*s3.get("tmpfs", []), f"/data:uid={uid},gid={gid},size=64m"]
    fixed["healthcheck"] = {"test": s3["healthcheck"]["test"], "interval": "1s", "retries": 120}
    bare = {**fixed, "environment": {k: v for k, v in env.items() if k not in _PROXY_VARS}}
    config = tmp_path / "bucket.yaml"
    config.write_text(json.dumps({"services": {"fixed": fixed, "bare": bare}}))
    client = _proxied_client(tmp_path)
    compose = (
        *("docker", "compose", "-p", f"kontra-bucket-proxy-test-{uuid.uuid4().hex[:8]}"),
        *("-f", str(config)),
    )

    def run(*args: str, timeout: float = 120) -> subprocess.CompletedProcess[str]:
        return _REAL_RUN(
            [*compose, *args],
            capture_output=True,
            text=True,
            env=client,
            timeout=timeout,
            check=False,
        )

    try:
        assert run("up", "-d", "bare").returncode == 0
        up = run("up", "-d", "--wait", "--wait-timeout", "150", "fixed", timeout=210)
        assert up.returncode == 0, up.stderr + run("logs", "fixed").stdout
        assert run("exec", "-T", "fixed", "printenv", "http_proxy").stdout == "\n"
        # Control: the same service under the same client, without the four entries.
        assert run("exec", "-T", "bare", "printenv", "http_proxy").stdout.strip() == _PROXY
    finally:
        down = run("down", "-v", "--remove-orphans", "-t", "1")
        assert down.returncode == 0, down.stderr


def test_third_party_images_are_pinned_by_digest() -> None:
    """A tag moves; a digest does not. The tag stays beside it for dependabot and readers.
    (Kontra's own images are named by KONTRA_*_IMAGE and published from this repository.)
    The observability file has no image of ours at all."""
    import yaml

    pinned = re.compile(r"[a-z0-9./-]+:[A-Za-z0-9._-]+@sha256:[0-9a-f]{64}")
    raw = yaml.safe_load(COMPOSE.read_text())
    for name in ("postgres", "postgres-relay", "s3", "ui"):
        image = raw["services"][name]["image"]
        assert pinned.fullmatch(image), f"{name}: {image}"
    for name in ("temporal", "migrate", "gate"):
        assert raw["services"][name]["image"].startswith("${KONTRA_")
    observability = yaml.safe_load(OBSERVABILITY.read_text())
    for name, svc in observability["services"].items():
        assert pinned.fullmatch(svc["image"]), f"{name}: {svc['image']}"
    # A digest nothing bumps is a frozen image, its security fixes included.
    bot = yaml.safe_load((REPO / ".github" / "dependabot.yml").read_text())
    [compose] = [u for u in bot["updates"] if u["package-ecosystem"] == "docker-compose"]
    assert compose["directories"] == ["/", "/observability"]


def test_compose_reads_only_the_host_settings_it_documents() -> None:
    """The host ports and binds the e2e stack reads, each named in the header. Their names
    predate `kontra dev`'s in-process stack and stay (kontra-cloud's `make up` sets them,
    OD8); `kontra dev` never reads them (tests/devgate/test_state.py). Nothing is remembered
    for `kontra dev` any more: no x-kontra-dev-settings."""
    import yaml

    text = COMPOSE.read_text()
    raw = yaml.safe_load(text)
    assert "x-kontra-dev-settings" not in raw
    body = "\n".join(line for line in text.splitlines() if not line.lstrip().startswith("#"))
    read = set(re.findall(r"\$\{(KONTRA_DEV_[A-Z0-9_]+)", body))
    assert read == {
        "KONTRA_DEV_BIND", "KONTRA_DEV_PG_BIND", "KONTRA_DEV_GATE_PORT",
        "KONTRA_DEV_TEMPORAL_PORT", "KONTRA_DEV_S3_PORT", "KONTRA_DEV_PG_PORT",
        "KONTRA_DEV_UI_PORT", "KONTRA_DEV_CODEC_PORT",
    }  # fmt: skip
    header = text[: text.index("\nname:")]
    assert all(name in header for name in read)
    # The Makefile runs the e2e stack on ports of its own, so `make e2e` and `kontra dev up`
    # (8080, 7233 by default) never collide.
    makefile = (REPO / "Makefile").read_text()
    for name, port in (
        ("KONTRA_DEV_GATE_PORT", 18080),
        ("KONTRA_DEV_TEMPORAL_PORT", 17233),
        ("KONTRA_DEV_S3_PORT", 18333),
        ("KONTRA_DEV_PG_PORT", 15432),
    ):
        assert re.search(rf"^export {name} \?= {port}$", makefile, re.MULTILINE), name


@needs_compose
def test_the_ui_profile_adds_the_ui_on_loopback_and_nothing_by_default(tmp_path: Path) -> None:
    """`make stack-up` is the stack alone and Temporal serves no metrics. `UI=1` adds
    exactly Temporal's UI, published on the host's loopback whatever KONTRA_DEV_BIND says:
    it controls every namespace."""
    default = _resolved(tmp_path, KONTRA_DEV_BIND="0.0.0.0")
    assert "ui" not in default["services"]
    assert default["services"]["temporal"]["environment"]["KONTRA_METRICS_ADDR"] == ""
    full = _with_profiles(tmp_path, KONTRA_DEV_BIND="0.0.0.0")
    services = full["services"]
    assert set(services) - set(default["services"]) == {"ui"}
    assert services["ui"]["profiles"] == ["ui"]
    published = [
        (p["host_ip"], str(p["published"]), int(p["target"])) for p in services["ui"]["ports"]
    ]
    assert published == [("127.0.0.1", "8233", 8080)]
    moved = _with_profiles(
        tmp_path,
        KONTRA_DEV_UI_PORT="18233",
        KONTRA_DEV_CODEC_PORT="18081",
        KONTRA_TEMPORAL_METRICS_ADDR="0.0.0.0:8000",
    )["services"]
    assert [p["published"] for p in moved["ui"]["ports"]] == ["18233"]
    ui = moved["ui"]["environment"]
    # The browser calls a codec server; the UI names it on loopback, for its own origins.
    assert ui["TEMPORAL_CODEC_ENDPOINT"] == "http://127.0.0.1:18081"
    assert ui["TEMPORAL_CORS_ORIGINS"] == "http://localhost:18233,http://127.0.0.1:18233"
    assert moved["temporal"]["environment"]["KONTRA_METRICS_ADDR"] == "0.0.0.0:8000"


@needs_compose
def test_the_ui_profile_keeps_the_stack_s_walls(tmp_path: Path) -> None:
    """The UI runs as every other service does (no capability, no privilege, a read-only
    root) and sits on `control` only, because it needs the internal frontend (that is its
    point, and why it never runs in production)."""
    services = _with_profiles(tmp_path)["services"]
    svc = services["ui"]
    assert svc.get("cap_drop") == ["ALL"]
    assert not svc.get("cap_add")
    assert svc.get("security_opt") == ["no-new-privileges:true"]
    assert svc.get("read_only") is True
    assert not svc.get("privileged")
    for key in ("pid", "ipc", "uts", "userns_mode", "cgroup", "devices", "network_mode"):
        assert not svc.get(key), f"ui sets {key}"
    assert not svc.get("volumes")
    env = svc.get("environment") or {}
    assert {var: env.get(var) for var in _PROXY_VARS} == dict.fromkeys(_PROXY_VARS, "")
    networks = {name: set(s.get("networks", ())) for name, s in services.items()}
    assert networks["ui"] == {"control"}
    assert {n for n, nets in networks.items() if "control" in nets} == {"temporal", "gate", "ui"}
    assert svc["environment"]["TEMPORAL_ADDRESS"] == "temporal:7236"
    peers = {n for n, nets in networks.items() if n != "temporal" and nets & {"temporal-db"}}
    assert peers == {"postgres-relay"}


@needs_compose
def test_compose_wires_the_gate_settings(tmp_path: Path) -> None:
    config = _resolved(
        tmp_path,
        KONTRA_DEV="1",
        KONTRA_DEV_GATE_PORT="8401",
        KONTRA_DEV_TEMPORAL_PORT="7433",
        KONTRA_DEV_S3_PORT="9401",
        KONTRA_DEV_PG_PORT="5443",
        KONTRA_DEV_BIND="0.0.0.0",
        KONTRA_GATE_IMAGE="kontra-gate:dev",
        KONTRA_TEMPORAL_IMAGE="kontra-temporal:dev",
    )
    services = config["services"]
    gate = services["gate"]["environment"]
    assert gate == gate | {
        "KONTRA_DEV": "1",
        "KONTRA_DATABASE_URL": "postgresql+asyncpg://kontra:pw-test@postgres:5432/kontra",
        "KONTRA_PUBLIC_URL": "http://localhost:8401",
        "KONTRA_TEMPORAL_INTERNAL": "temporal:7236",
        "KONTRA_TEMPORAL_INTERNAL_HTTP": "http://temporal:7246",
        "KONTRA_TEMPORAL_PUBLIC": "localhost:7433",
        "KONTRA_S3_ENDPOINT": "http://s3:8333",
        "KONTRA_S3_PUBLIC_ENDPOINT": "http://localhost:9401",
        "KONTRA_S3_ACCESS_KEY": "ak-test",
        "KONTRA_S3_SECRET_KEY": "sk-test",
        "KONTRA_LEDGER": "postgres",
    }
    assert services["migrate"]["environment"]["KONTRA_DATABASE_URL"] == gate["KONTRA_DATABASE_URL"]
    assert services["migrate"]["command"] == ["alembic", "upgrade", "head"]
    assert {p["host_ip"] for s in ("gate", "s3", "temporal") for p in services[s]["ports"]} == {
        "0.0.0.0"
    }
    assert services["postgres"]["ports"][0]["host_ip"] == "127.0.0.1"  # its own switch
    assert services["gate"]["image"] == "kontra-gate:dev"
    deps = services["gate"]["depends_on"]
    assert deps["migrate"]["condition"] == "service_completed_successfully"
    assert deps["postgres"]["condition"] == deps["s3"]["condition"] == "service_healthy"
    assert services["gate"]["restart"] == "unless-stopped"  # retries until Temporal is up
    identities = json.loads(services["s3"]["environment"]["KONTRA_S3_IDENTITIES"])
    creds = identities["identities"][0]["credentials"][0]
    assert creds == {"accessKey": "ak-test", "secretKey": "sk-test"}


def test_gate_env_names_are_the_gate_settings() -> None:
    """Every KONTRA_* the compose file hands the gate is a setting the gate reads (its
    settings live in kontra-cloud; the names are the GATE_BRIEF's)."""
    import yaml

    raw = yaml.safe_load(COMPOSE.read_text())
    names = set(raw["services"]["gate"]["environment"])
    known = {
        "KONTRA_DEV", "KONTRA_DATABASE_URL", "KONTRA_BIND", "KONTRA_PUBLIC_URL",
        "KONTRA_TEMPORAL_INTERNAL", "KONTRA_TEMPORAL_INTERNAL_HTTP", "KONTRA_TEMPORAL_PUBLIC",
        "KONTRA_TEMPORAL_TLS", "KONTRA_S3_ENDPOINT", "KONTRA_S3_PUBLIC_ENDPOINT",
        "KONTRA_S3_BUCKET", "KONTRA_S3_REGION", "KONTRA_S3_ACCESS_KEY", "KONTRA_S3_SECRET_KEY",
        "KONTRA_LEDGER", "KONTRA_SIGNING_KEYS", "KONTRA_RESOLVERS", "KONTRA_FEE_BPS",
        "KONTRA_GRACE_SECONDS", "KONTRA_SWEEP_INTERVAL", "KONTRA_CONFIRM_TTL",
        "KONTRA_PUBLISHER_FRESHNESS",
        "KONTRA_MAX_INPUT_BYTES", "KONTRA_GITHUB_CLIENT_ID", "KONTRA_GITHUB_CLIENT_SECRET",
        "KONTRA_STRIPE_SECRET", "KONTRA_STRIPE_WEBHOOK_SECRET",
    }  # fmt: skip
    assert names == known


# --------------------------------------------------------------------------- observability/compose.yaml

_OBSERVED = {"prometheus", "prometheus-agent", "grafana", "otelcol", "loki", "tempo"}


def _observability(**extra: str) -> dict[str, Any]:
    """``docker compose config`` of the observability file exactly as `kontra dev up
    --observability` runs it (``-p``/``-f`` only), with ``extra`` port variables."""
    clean = {k: v for k, v in os.environ.items() if not k.startswith("KONTRA_")}
    proc = subprocess.run(
        [
            *("docker", "compose", "-p", "kontra-dev-observability", "-f", str(OBSERVABILITY)),
            *("config", "--format", "json"),
        ],
        capture_output=True,
        text=True,
        env={**clean, **extra},
        timeout=120,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr
    return json.loads(proc.stdout)


def test_the_observability_file_is_six_services_and_nothing_of_the_gate() -> None:
    """Not a back door to the prod gate (decisions I34, §7): the six services, no profile, no
    env file, no include or extends (no other file can come in), no gate and no Temporal."""
    import yaml

    text = OBSERVABILITY.read_text()
    raw = yaml.safe_load(text)
    assert raw["name"] == "kontra-dev-observability"
    assert set(raw["services"]) == _OBSERVED
    assert not {"include", "extends"} & set(raw)
    for name, svc in raw["services"].items():
        assert not {"profiles", "env_file", "extends", "build"} & set(svc), name
        assert not svc["image"].startswith(("temporalio/", "ghcr.io/", "${")), name
    for forbidden in ("kontra-gate", "kontra-temporal", "KONTRA_GATE_IMAGE", "KONTRA_DEV:"):
        assert forbidden not in text, forbidden


@needs_compose
def test_the_observability_stack_publishes_on_loopback_and_keeps_its_walls() -> None:
    """Every service runs with no capability, no privilege, a read-only root and not as root;
    Prometheus, Grafana and the collector's OTLP/gRPC port are published on 127.0.0.1 only,
    the agent, Loki and Tempo publish nothing. Prometheus's agent alone is in the host's
    network namespace, the one way to a worker on the host's loopback, and holds nothing but
    its tmpfs (its configuration comes in its environment). The collector, which takes
    whatever reaches its published port, shares a network (`telemetry`) with the two stores
    it writes and with Grafana, which reads them, and with nothing else; Grafana's peers are
    its three data sources and the collector. The only host paths are the read-only `configs`
    of observability/ and docs/dashboards/."""
    config = _observability()
    services = config["services"]
    assert set(services) == _OBSERVED
    for name in _OBSERVED:
        svc = services[name]
        assert svc.get("cap_drop") == ["ALL"], name
        assert not svc.get("cap_add"), name
        assert svc.get("security_opt") == ["no-new-privileges:true"], name
        assert svc.get("read_only") is True, name
        assert not svc.get("privileged"), name
        for key in ("pid", "ipc", "uts", "userns_mode", "cgroup", "devices"):
            assert not svc.get(key), f"{name} sets {key}"
        assert svc.get("network_mode") == ("host" if name == "prometheus-agent" else None), name
        assert all(v["type"] == "volume" for v in svc.get("volumes", [])), name
        assert all(entry.split(":", 1)[1].startswith("size=") for entry in svc.get("tmpfs", []))
        env = svc.get("environment") or {}
        assert {var: env.get(var) for var in _PROXY_VARS} == dict.fromkeys(_PROXY_VARS, ""), name
    assert services["prometheus"]["user"] == "65534:65534"
    assert services["prometheus-agent"]["user"] == "65534:65534"
    assert services["grafana"]["user"] == "472"
    for name in ("otelcol", "loki", "tempo"):  # the images' own unprivileged user, named
        assert services[name]["user"] == "10001:10001", name
    published = {
        name: [(p["host_ip"], str(p["published"]), int(p["target"])) for p in svc.get("ports", [])]
        for name, svc in services.items()
    }
    assert published == {
        "prometheus": [("127.0.0.1", "9090", 9090)],
        "prometheus-agent": [],
        "grafana": [("127.0.0.1", "3000", 3000)],
        "otelcol": [("127.0.0.1", "4317", 4317)],  # OTLP/gRPC, what `kontra serve --otlp` sends
        "loki": [],
        "tempo": [],
    }
    agent = services["prometheus-agent"]
    assert "--agent" in agent["command"]  # scrapes and forwards: no database, no queries
    assert "--web.listen-address=127.0.0.1:0" in agent["command"]
    assert not agent.get("volumes")
    assert not agent.get("configs")
    networks = {name: set(svc.get("networks", ())) for name, svc in services.items()}
    assert networks == {
        "prometheus": {"observability"},
        "prometheus-agent": set(),  # network_mode: host
        "grafana": {"observability", "telemetry"},
        "otelcol": {"telemetry"},
        "loki": {"telemetry"},
        "tempo": {"telemetry"},
    }

    def peers_of(name: str) -> set[str]:
        return {n for n, nets in networks.items() if nets & networks[name]}

    assert peers_of("grafana") == {"grafana", "prometheus", "otelcol", "loki", "tempo"}
    assert peers_of("otelcol") == {"otelcol", "loki", "tempo", "grafana"}
    assert services["grafana"].get("sysctls") == {"net.ipv4.ip_forward": "0"}  # on two networks
    grafana = services["grafana"]["environment"]
    # Explore (the graph page's Logs and Trace links) needs an Editor in Grafana 13; nothing
    # an Editor could make Grafana send out is on: alerting, external snapshots.
    assert grafana["GF_AUTH_ANONYMOUS_ORG_ROLE"] == "Editor"
    assert (grafana["GF_AUTH_DISABLE_LOGIN_FORM"], grafana["GF_AUTH_BASIC_ENABLED"]) == (
        "true",
        "false",
    )
    assert grafana["GF_UNIFIED_ALERTING_ENABLED"] == "false"
    assert grafana["GF_SNAPSHOTS_EXTERNAL_ENABLED"] == "false"
    mounted = {name for name, svc in services.items() if svc.get("configs")}
    assert mounted == {"grafana", "otelcol", "loki", "tempo"}
    for source in config["configs"].values():
        path = Path(source["file"]).resolve()
        assert path.is_file(), path
        assert path.is_relative_to(REPO / "observability") or path.is_relative_to(
            REPO / "docs" / "dashboards"
        ), path


@needs_compose
def test_prometheus_and_its_agent_scrape_what_kontra_dev_turns_on() -> None:
    """The dev server's metrics are where `kontra dev up --observability` puts them
    (KONTRA_DEV_TEMPORAL_METRICS_PORT, 9463 by default: kontra.devgate.Ports), the serve ports
    are the documented range, on the host's loopback for the agent and at
    host.docker.internal for Prometheus's own scrape (Docker Desktop); the agent writes to
    Prometheus's published port, wherever KONTRA_DEV_PROMETHEUS_PORT moves it."""
    import yaml

    serve = list(range(9464, 9474))
    for metrics, port in ((str(Ports().temporal_metrics), "9090"), ("19463", "19090")):
        extra = {} if port == "9090" else {
            "KONTRA_DEV_TEMPORAL_METRICS_PORT": metrics,
            "KONTRA_DEV_PROMETHEUS_PORT": port,
        }  # fmt: skip
        services = _observability(**extra)["services"]
        agent_svc, prom_svc = services["prometheus-agent"], services["prometheus"]
        # What their entrypoints write to the files their commands name (JSON, which is YAML).
        agent = yaml.safe_load(agent_svc["environment"]["KONTRA_AGENT_CONFIG"])
        prom = yaml.safe_load(prom_svc["environment"]["KONTRA_PROM_CONFIG"])
        assert "--config.file=/tmp/agent.yml" in agent_svc["command"]
        assert '> /tmp/agent.yml && exec /bin/prometheus "$$@"' in agent_svc["entrypoint"][2]
        assert "--config.file=/tmp/prometheus.yml" in prom_svc["command"]
        assert '> /tmp/prometheus.yml && exec /bin/prometheus "$$@"' in prom_svc["entrypoint"][2]
        jobs = {j["job_name"]: j["static_configs"][0]["targets"] for j in agent["scrape_configs"]}
        assert jobs == {
            "temporal": [f"127.0.0.1:{metrics}"],
            "kontra-serve": [f"127.0.0.1:{p}" for p in serve],
        }
        assert agent["remote_write"] == [{"url": f"http://127.0.0.1:{port}/api/v1/write"}]
        jobs = {j["job_name"]: j["static_configs"][0]["targets"] for j in prom["scrape_configs"]}
        assert jobs == {
            "temporal": [f"host.docker.internal:{metrics}"],
            "kontra-serve": [f"host.docker.internal:{p}" for p in serve],
        }
        # Compose prints `name=address` or `name:address`, by version.
        hosts = [h.replace("=", ":", 1) for h in prom_svc["extra_hosts"]]
        assert hosts == ["host.docker.internal:host-gateway"]
        [published] = prom_svc["ports"]
        assert (published["host_ip"], str(published["published"])) == ("127.0.0.1", port)
        assert "--web.enable-remote-write-receiver" in prom_svc["command"]


def test_grafana_loads_the_dashboards_and_the_collector_writes_where_it_reads() -> None:
    """Grafana's files are where its provider looks; what `kontra serve --otlp` sends reaches
    the collector's published port, leaves it for Tempo (traces) and Loki's OTLP endpoint
    (logs), and is read back by Grafana under the uids the graph page's links name; each
    service reads the file compose mounts."""
    import yaml

    observability = REPO / "observability"
    raw = yaml.safe_load(OBSERVABILITY.read_text())
    services, configs = raw["services"], raw["configs"]
    providers = yaml.safe_load((observability / "grafana" / "dashboards.yaml").read_text())
    [provider] = providers["providers"]
    targets = [c["target"] for c in services["grafana"]["configs"]]
    boards = [t for t in targets if t.endswith(".json")]
    assert {str(Path(t).parent) for t in boards} == {provider["options"]["path"]}
    boards_dir = REPO / "docs" / "dashboards"
    assert {Path(t).name for t in boards} == {b.name for b in boards_dir.glob("*.json")}
    for board in boards_dir.glob("*.json"):
        assert json.loads(board.read_text())["title"]  # vendored as published: valid JSON

    def mounted(service: str) -> dict[str, Any]:
        """The service's one configuration file, as the path its command names."""
        [config] = services[service]["configs"]
        [flag] = [a for a in services[service]["command"] if a.endswith(config["target"])]
        assert flag.split("=", 1)[1] == config["target"]
        return yaml.safe_load((observability / configs[config["source"]]["file"]).read_text())

    collector, loki, tempo = mounted("otelcol"), mounted("loki"), mounted("tempo")
    assert collector == yaml.safe_load((observability / "otelcol.yaml").read_text())
    [port] = services["otelcol"]["ports"]
    assert port == "127.0.0.1:${KONTRA_DEV_OTLP_PORT:-4317}:4317"
    assert collector["receivers"]["otlp"]["protocols"] == {"grpc": {"endpoint": "0.0.0.0:4317"}}
    exporters = collector["exporters"]
    pipelines = collector["service"]["pipelines"]
    [traces_out] = pipelines["traces"]["exporters"]
    [logs_out] = pipelines["logs"]["exporters"]
    assert pipelines["traces"]["receivers"] == pipelines["logs"]["receivers"] == ["otlp"]
    receiver = tempo["distributor"]["receivers"]["otlp"]["protocols"]["grpc"]["endpoint"]
    assert traces_out.startswith("otlp_grpc/")
    assert exporters[traces_out]["endpoint"] == f"tempo:{receiver.rsplit(':', 1)[1]}"
    loki_http = f"http://loki:{loki['server']['http_listen_port']}"
    assert logs_out.startswith("otlp_http/")
    assert exporters[logs_out]["endpoint"] == f"{loki_http}/otlp"
    # Record attributes (workflow_id, run_id, ...) become Loki's structured metadata.
    assert loki["limits_config"]["allow_structured_metadata"] is True
    sources = yaml.safe_load((observability / "grafana" / "datasources.yaml").read_text())
    by_uid = {s["uid"]: s for s in sources["datasources"]}
    assert set(by_uid) == {"prometheus", "loki", "tempo"}
    [default] = [s for s in sources["datasources"] if s.get("isDefault")]
    assert (default["uid"], default["url"]) == ("prometheus", "http://prometheus:9090")
    assert by_uid["loki"]["type"] == "loki"
    assert by_uid["loki"]["url"] == loki_http
    assert by_uid["tempo"]["type"] == "tempo"
    assert by_uid["tempo"]["url"] == f"http://tempo:{tempo['server']['http_listen_port']}"
    assert all(s["editable"] is False for s in by_uid.values())
    # A log line's trace_id leads to its trace, and a span to its trace's lines.
    [derived] = by_uid["loki"]["jsonData"]["derivedFields"]
    assert (derived["matcherRegex"], derived["datasourceUid"]) == ("trace_id", "tempo")
    assert by_uid["tempo"]["jsonData"]["tracesToLogsV2"]["datasourceUid"] == "loki"


def test_the_e2e_suite_describes_the_stack_it_runs() -> None:
    """``tests/e2e`` runs ``compose.yaml`` through ``tests/e2e/stack.py`` (``make stack-up``),
    not ``kontra dev``: its prose names neither the old ``kontra dev up``/``down --volumes``
    lifecycle nor ``$KONTRA_HOME/dev.env``, and it reads the stack's env file one way."""
    e2e = REPO / "tests" / "e2e"
    for path in e2e.glob("*.py"):
        text = path.read_text(encoding="utf-8")
        for stale in ("kontra dev down --volumes", "dev.env", "with ``kontra dev up``"):
            assert stale not in text, f"{path.name} still says {stale!r}"
    defined = [p.name for p in e2e.glob("*.py") if "def read_env_file(" in p.read_text("utf-8")]
    assert defined == ["stack.py"]
