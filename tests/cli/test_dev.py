"""``kontra dev`` and the compose file it runs (PRD §19.3).

The file is validated by ``docker compose config`` itself (skipped without Docker): it must
refuse to run without its secrets, publish nothing but the public ports and only on the
host's loopback by default, never publish Temporal's internal frontend nor put Temporal on a
network with anything but the gate and its database relay, pin its third-party images by
digest, and hand the gate every setting it needs. The subcommands are checked for the compose
invocations they make; ``kontra dev up`` against real images is the e2e suite's.
"""

from __future__ import annotations

import ipaddress
import json
import os
import re
import shutil
import stat
import subprocess
import uuid
from pathlib import Path
from typing import Any

import pytest

import kontra.cli._dev as dev_module
from kontra.cli._dev import DEV_ENV_FILE, PROJECT, compose_file, dev_env

from .support import Cli

REPO = Path(__file__).resolve().parents[2]
COMPOSE = REPO / "compose.yaml"
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


def _config(tmp_path: Path, env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    env_file = tmp_path / "test.env"
    env_file.write_text("".join(f"{k}={v}\n" for k, v in env.items()))
    # Nothing from the developer's shell may override the file under test.
    clean = {k: v for k, v in os.environ.items() if not k.startswith("KONTRA_")}
    return subprocess.run(
        [
            *("docker", "compose", "-p", "kontra-config-test", "-f", str(COMPOSE)),
            *("--env-file", str(env_file), "config", "--format", "json"),
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
    `kontra dev up` waited for nothing. Compose has no environment for a healthcheck
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
    (Kontra's own images are named by KONTRA_*_IMAGE and published from this repository.)"""
    import yaml

    raw = yaml.safe_load(COMPOSE.read_text())
    pinned = re.compile(r"[a-z0-9./-]+:[A-Za-z0-9._-]+@sha256:[0-9a-f]{64}")
    for name in ("postgres", "postgres-relay", "s3"):
        image = raw["services"][name]["image"]
        assert pinned.fullmatch(image), f"{name}: {image}"
    for name in ("temporal", "migrate", "gate"):
        assert raw["services"][name]["image"].startswith("${KONTRA_")
    # A digest nothing bumps is a frozen image, its security fixes included.
    bot = yaml.safe_load((REPO / ".github" / "dependabot.yml").read_text())
    updates = [u for u in bot["updates"] if u["package-ecosystem"] == "docker-compose"]
    assert [u["directory"] for u in updates] == ["/"]


def test_compose_lists_every_host_port_it_reads() -> None:
    """`kontra dev up` remembers the settings x-kontra-dev-settings lists: every host port
    the file reads must be there, or `status` would lose it again -- and no bind address,
    or one `KONTRA_DEV_BIND=0.0.0.0` would publish every later stack to the network."""
    import yaml

    raw = yaml.safe_load(COMPOSE.read_text())
    listed = raw.pop("x-kontra-dev-settings")
    read = set(re.findall(r"\$\{(KONTRA_DEV_[A-Z0-9_]+)", json.dumps(raw)))
    ports = {name for name in read if name.endswith("_PORT")}
    assert len(ports) == 4
    assert read - ports == {"KONTRA_DEV_BIND", "KONTRA_DEV_PG_BIND"}
    assert set(listed) == ports
    assert all(value == f"${{{name}:-}}" for name, value in listed.items())


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
        "KONTRA_GRACE_SECONDS", "KONTRA_SWEEP_INTERVAL", "KONTRA_PUBLISHER_FRESHNESS",
        "KONTRA_MAX_INPUT_BYTES", "KONTRA_GITHUB_CLIENT_ID", "KONTRA_GITHUB_CLIENT_SECRET",
        "KONTRA_STRIPE_SECRET", "KONTRA_STRIPE_WEBHOOK_SECRET",
    }  # fmt: skip
    assert names == known
    assert compose_file() == COMPOSE  # a checkout: the root file is the packaged one


# --------------------------------------------------------------------------- the commands


def test_dev_env_is_private_random_and_kept(tmp_path: Path) -> None:
    path = dev_env(tmp_path / "home")
    assert path == tmp_path / "home" / DEV_ENV_FILE
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    values = dict(
        line.split("=", 1) for line in path.read_text().splitlines() if line and line[0] != "#"
    )
    assert values["KONTRA_DEV"] == "1"
    assert len(values["KONTRA_PG_PASSWORD"]) >= 32
    first = path.read_text()
    assert dev_env(tmp_path / "home").read_text() == first  # the volume remembers it
    other = dev_env(tmp_path / "other")
    assert other.read_text() != first


def _docker_at(name: str) -> str | None:
    return "/usr/bin/docker"


def _no_docker(name: str) -> str | None:
    return None


def _healthy_at_once(url: str, timeout: float) -> None:
    return None


def _after_options(cmd: list[str]) -> list[str]:
    """``docker compose -p P -f F --env-file E... <verb> <args>`` -> ``[<verb>, <args>]``."""
    rest = cmd[cmd.index("compose") + 1 :]
    while rest[0] in ("-p", "-f", "--env-file"):
        rest = rest[2:]
    return rest


class Recorder:
    """Stands in for ``docker compose``: records each call, answers ``config``/``ps``."""

    def __init__(self, config: dict[str, Any] | None = None, ps: str = "") -> None:
        self.calls: list[list[str]] = []
        self.config = config or {}
        self.ps = ps

    def __call__(self, cmd: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        self.calls.append(cmd)
        verb = _after_options(cmd)[0]
        out = {"config": json.dumps(self.config), "ps": self.ps}.get(verb, "")
        return subprocess.CompletedProcess(cmd, 0, stdout=out, stderr="")


def test_dev_down_and_logs_run_the_packaged_compose(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    rec = Recorder()
    monkeypatch.setattr(dev_module.subprocess, "run", rec)
    monkeypatch.setattr(dev_module.shutil, "which", _docker_at)
    cli = Cli(tmp_path / "home")
    extra = tmp_path / "prod.env"
    extra.write_text("KONTRA_DEV=0\n")

    result = cli("dev", "down", "--volumes", "--env-file", str(extra))
    assert result.exit_code == 0, result.output
    head = ["/usr/bin/docker", "compose", "-p", PROJECT, "-f", str(COMPOSE)]
    env_files = ["--env-file", str(tmp_path / "home" / DEV_ENV_FILE), "--env-file", str(extra)]
    assert rec.calls == [[*head, *env_files, "down", "--remove-orphans", "--volumes"]]

    rec.calls.clear()
    assert cli("dev", "logs", "--tail", "50", "gate").exit_code == 0
    assert rec.calls[0][-5:] == ["logs", "--tail", "50", "--", "gate"]
    assert cli("dev", "logs", "--tail", "50; rm -rf /").exit_code == 1
    assert cli("dev", "logs", "--tail", "\uff15\uff10").exit_code == 1  # fullwidth digits

    rec.calls.clear()  # an env file given to the group reaches the subcommand, first
    later = tmp_path / "later.env"
    later.write_text("KONTRA_DEV_GATE_PORT=8402\n")
    assert cli("dev", "--env-file", str(extra), "down", "--env-file", str(later)).exit_code == 0
    assert rec.calls[0][-6:] == [
        "--env-file", str(extra), "--env-file", str(later), "down", "--remove-orphans",
    ]  # fmt: skip


def test_dev_status_reports_services_and_health(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ps = "\n".join(
        json.dumps({"Service": s, "State": "running", "Health": h, "Status": "Up"})
        for s, h in (("gate", "healthy"), ("temporal", ""))
    )
    config = {"services": {"gate": {"environment": {"KONTRA_PUBLIC_URL": "http://127.0.0.1:9"}}}}
    monkeypatch.setattr(dev_module.subprocess, "run", Recorder(config, ps))
    monkeypatch.setattr(dev_module.shutil, "which", _docker_at)
    status = Cli(tmp_path / "home")("dev", "status", "--json")
    assert status.exit_code == 0, status.output
    out = json.loads(status.stdout)
    assert out["services"] == [
        {"service": "gate", "state": "running", "health": "healthy", "status": "Up"},
        {"service": "temporal", "state": "running", "health": None, "status": "Up"},
    ]
    assert out["gate"] == {"url": "http://127.0.0.1:9", "healthy": False}
    assert out["profile"] is None


def test_dev_up_never_replaces_a_real_login(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from datetime import UTC, datetime

    from kontra.profile import Profile

    home = tmp_path / "home"
    Profile(
        gate_url="https://gate.example",
        temporal_address="temporal.example:7233",
        party="alice",
        namespace="alice",
        refresh_token="secret-refresh",
        auth_time=datetime.now(UTC),
    ).save(home)
    rec = Recorder()
    monkeypatch.setattr(dev_module.subprocess, "run", rec)
    monkeypatch.setattr(dev_module.shutil, "which", _docker_at)
    result = Cli(home)("dev", "up")
    assert result.exit_code == 1
    assert "alice" in result.stderr
    assert "KONTRA_HOME" in result.stderr
    assert rec.calls == []  # nothing started
    assert Profile.load(home).refresh_token == "secret-refresh"


def test_dev_needs_docker(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(dev_module.shutil, "which", _no_docker)
    result = Cli(tmp_path / "home")("dev", "status")
    assert (result.exit_code, "needs Docker" in result.stderr) == (1, True)


@needs_compose
def test_dev_reads_the_real_compose_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The CLI and the file agree: with the dev.env the CLI writes, compose resolves a dev
    gate, on the URL the CLI then waits on and logs in to, with the generated password."""
    for name in list(os.environ):
        if name.startswith("KONTRA_"):
            monkeypatch.delenv(name)
    monkeypatch.setenv("KONTRA_HOME", str(tmp_path / "home"))
    config = dev_module._config(())
    env = dev_module._gate_env(config)
    written = (tmp_path / "home" / DEV_ENV_FILE).read_text()
    values = dict(ln.split("=", 1) for ln in written.splitlines() if ln and ln[0] != "#")
    password = values["KONTRA_PG_PASSWORD"]
    assert env["KONTRA_DEV"] == "1"
    assert env["KONTRA_PUBLIC_URL"] == "http://localhost:8080"
    assert f":{password}@postgres:5432/" in env["KONTRA_DATABASE_URL"]
    assert dev_module._published(config, "postgres") == "localhost:5432"


def _up_world(
    monkeypatch: pytest.MonkeyPatch, gate_env: dict[str, str]
) -> tuple[Recorder, list[dict[str, Any]]]:
    """`kontra dev up` with compose, the health wait, the login and the Temporal check
    replaced by recorders: what it asks of each, in order."""
    import kontra.auth

    config = {
        "services": {
            "gate": {"environment": gate_env},
            "postgres": {"ports": [{"host_ip": "127.0.0.1", "published": "5444"}]},
        }
    }
    rec = Recorder(config)
    logins: list[dict[str, Any]] = []
    monkeypatch.setattr(dev_module.subprocess, "run", rec)
    monkeypatch.setattr(dev_module.shutil, "which", _docker_at)
    monkeypatch.setattr(dev_module, "_wait_healthy", _healthy_at_once)

    async def no_wait(home: Path, within: float) -> None:
        return None

    monkeypatch.setattr(dev_module, "_wait_temporal", no_wait)

    async def fake_login(url: str, **kwargs: Any) -> Any:
        from datetime import UTC, datetime

        from kontra.profile import Profile

        logins.append({"url": url, **kwargs})
        profile = Profile(
            gate_url=url,
            temporal_address="localhost:7434",
            party=kwargs["handle"],
            namespace=kwargs["handle"],
            refresh_token="r",
            auth_time=datetime.now(UTC),
            public_key=None,
            tls=False,
            dev=True,
        )
        profile.save(kwargs["home"])
        return profile

    monkeypatch.setattr(kontra.auth, "login", fake_login)
    return rec, logins


def test_dev_up_brings_the_stack_up_then_logs_in(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    env = {
        "KONTRA_PUBLIC_URL": "http://localhost:8402",
        "KONTRA_DEV": "1",
        "KONTRA_TEMPORAL_PUBLIC": "localhost:7434",
        "KONTRA_S3_PUBLIC_ENDPOINT": "http://localhost:9402",
    }
    rec, logins = _up_world(monkeypatch, env)
    cli = Cli(tmp_path / "home")
    result = cli("dev", "--handle", "carol")  # `kontra dev` alone is `up`
    assert result.exit_code == 0, result.output
    verbs = [_after_options(c) for c in rec.calls]
    assert verbs == [["config", "--format", "json"], ["up", "--detach", "--remove-orphans"]]
    assert logins == [
        {
            "url": "http://localhost:8402",
            "handle": "carol",
            "dev": True,
            "home": tmp_path / "home",
            "replace_key": True,
        }
    ]
    for line in ("gate      http://localhost:8402", "postgres  localhost:5444", "as carol"):
        assert line in result.stdout

    # Again, without --handle: the same dev party, not one derived from the user name.
    again = cli("dev", "up")
    assert again.exit_code == 0, again.output
    assert logins[-1]["handle"] == "carol"


def test_dev_up_does_not_dev_login_to_a_production_gate(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _, logins = _up_world(
        monkeypatch, {"KONTRA_PUBLIC_URL": "https://gate.example", "KONTRA_DEV": "0"}
    )
    result = Cli(tmp_path / "home")("dev", "up", "--handle", "carol")
    assert result.exit_code == 0, result.output
    assert logins == []
    assert "not a dev gate" in result.stdout
    bad = Cli(tmp_path / "home")("dev", "up", "--handle", "Not-A-Handle")
    assert bad.exit_code == 1


def _env_lines(path: Path) -> dict[str, str]:
    return dict(ln.split("=", 1) for ln in path.read_text().splitlines() if ln and ln[0] != "#")


def test_dev_up_remembers_its_host_settings_in_dev_env(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """What compose resolved for `up` lands in dev.env (which every later command passes
    to compose); an empty setting is dropped again, and the credentials are kept."""
    home = tmp_path / "home"
    rec, _ = _up_world(monkeypatch, {"KONTRA_PUBLIC_URL": "http://localhost:8459"})
    rec.config["x-kontra-dev-settings"] = {
        "KONTRA_DEV_PG_PORT": "",
        "KONTRA_DEV_GATE_PORT": "8459",
        "KONTRA_DEV_S3_PORT": "9409",
    }
    cli = Cli(home)
    assert cli("dev", "up", "--no-login").exit_code == 0
    path = home / DEV_ENV_FILE
    first = _env_lines(path)
    assert (first["KONTRA_DEV_GATE_PORT"], first["KONTRA_DEV_S3_PORT"]) == ("8459", "9409")
    assert "KONTRA_DEV_PG_PORT" not in first
    assert len(first["KONTRA_PG_PASSWORD"]) >= 32
    assert stat.S_IMODE(path.stat().st_mode) == 0o600

    rec.config["x-kontra-dev-settings"] = {
        "KONTRA_DEV_PG_PORT": "5449",
        "KONTRA_DEV_GATE_PORT": "",  # set empty for this run: back to compose's default
        "KONTRA_DEV_S3_PORT": "9409",
    }
    assert cli("dev", "up", "--no-login").exit_code == 0
    second = _env_lines(path)
    assert "KONTRA_DEV_GATE_PORT" not in second
    assert second["KONTRA_DEV_PG_PORT"] == "5449"
    assert {k: v for k, v in second.items() if not k.startswith("KONTRA_DEV_")} == {
        k: v for k, v in first.items() if not k.startswith("KONTRA_DEV_")
    }
    assert path.read_text().count("KONTRA_DEV_S3_PORT=") == 1
    assert [p.name for p in home.iterdir() if p.name.startswith(".")] == []  # no temp file

    # A value dev.env could not hold as written (a second line, a comment, a "$"), or that
    # is not a port, stops `up` before anything starts, and dev.env is left as it was.
    for bad in ("8459\nKONTRA_PG_PASSWORD=x", "8459 # c", "${HOME}", "8459'", "0.0.0.0", "\uff18"):
        rec.calls.clear()
        rec.config["x-kontra-dev-settings"] = {"KONTRA_DEV_GATE_PORT": bad}
        result = cli("dev", "up", "--no-login")
        assert result.exit_code == 1, bad
        assert "KONTRA_DEV_GATE_PORT" in result.stderr
        assert [_after_options(c)[0] for c in rec.calls] == ["config"]
        assert _env_lines(path) == second
    # A compose file that lists a bind address to remember is refused, not obeyed.
    rec.calls.clear()
    rec.config["x-kontra-dev-settings"] = {"KONTRA_DEV_BIND": "0.0.0.0"}
    result = cli("dev", "up", "--no-login")
    assert result.exit_code == 1
    assert "KONTRA_DEV_BIND" in result.stderr
    assert [_after_options(c)[0] for c in rec.calls] == ["config"]
    assert _env_lines(path) == second


@needs_compose
def test_dev_status_finds_the_stack_where_up_put_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`KONTRA_DEV_GATE_PORT=8459 kontra dev up`, then `kontra dev status` without it: the
    real compose file resolves the remembered port (only `up` and `ps` are stood in for)."""
    for name in list(os.environ):
        if name.startswith("KONTRA_"):
            monkeypatch.delenv(name)
    home = tmp_path / "home"
    monkeypatch.setenv("KONTRA_HOME", str(home))  # for the direct calls below, as for Cli

    class RealConfig(Recorder):
        def __call__(self, cmd: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
            if _after_options(cmd)[0] == "config":
                self.calls.append(cmd)
                return _REAL_RUN(cmd, **kwargs)
            return super().__call__(cmd, **kwargs)

    rec = RealConfig()
    monkeypatch.setattr(dev_module.subprocess, "run", rec)
    monkeypatch.setattr(dev_module.shutil, "which", _docker_at)
    monkeypatch.setattr(dev_module, "_wait_healthy", _healthy_at_once)
    cli = Cli(home)
    up = cli("dev", "up", "--no-login", KONTRA_DEV_GATE_PORT="8459", KONTRA_DEV_PG_PORT="5449")
    assert up.exit_code == 0, up.output
    assert "gate      http://localhost:8459" in up.stdout

    status = cli("dev", "status", "--json")
    assert status.exit_code == 0, status.output
    assert json.loads(status.stdout)["gate"]["url"] == "http://localhost:8459"
    assert dev_module._published(dev_module._config(()), "postgres") == "localhost:5449"
    # The environment still wins over what was remembered, as everywhere in compose.
    monkeypatch.setenv("KONTRA_DEV_GATE_PORT", "8458")
    assert dev_module._gate_env(dev_module._config(()))["KONTRA_PUBLIC_URL"].endswith(":8458")


@needs_compose
def test_dev_up_never_remembers_a_bind_address(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`KONTRA_DEV_BIND=0.0.0.0 kontra dev up` publishes that stack on every interface; the
    next `up` without it is back on loopback (a dev gate's login and play money are not
    published to the network because of an earlier run), while the port is remembered."""
    for name in list(os.environ):
        if name.startswith("KONTRA_"):
            monkeypatch.delenv(name)
    home = tmp_path / "home"
    monkeypatch.setenv("KONTRA_HOME", str(home))

    class RealConfig(Recorder):
        def __call__(self, cmd: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
            if _after_options(cmd)[0] == "config":
                self.calls.append(cmd)
                return _REAL_RUN(cmd, **kwargs)
            return super().__call__(cmd, **kwargs)

    monkeypatch.setattr(dev_module.subprocess, "run", RealConfig())
    monkeypatch.setattr(dev_module.shutil, "which", _docker_at)
    monkeypatch.setattr(dev_module, "_wait_healthy", _healthy_at_once)
    cli = Cli(home)
    up = cli(
        "dev",
        "up",
        "--no-login",
        KONTRA_DEV_BIND="0.0.0.0",
        KONTRA_DEV_PG_BIND="0.0.0.0",
        KONTRA_DEV_GATE_PORT="8459",
    )
    assert up.exit_code == 0, up.output
    remembered = _env_lines(home / DEV_ENV_FILE)
    assert remembered["KONTRA_DEV_GATE_PORT"] == "8459"
    assert not {"KONTRA_DEV_BIND", "KONTRA_DEV_PG_BIND"} & set(remembered)

    services = dev_module._config(())["services"]
    hosts = {
        (name, port["host_ip"], str(port["published"]))
        for name, svc in services.items()
        for port in svc.get("ports") or []
    }
    assert hosts == {
        ("gate", "127.0.0.1", "8459"),
        ("temporal", "127.0.0.1", "7233"),
        ("s3", "127.0.0.1", "8333"),
        ("postgres", "127.0.0.1", "5432"),
    }


def test_dev_up_keeps_the_rest_of_dev_env_as_written(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Remembering the ports rewrites only its own lines: a value holding a character
    str.splitlines treats as a line break (form feed, U+2028) comes back unchanged."""
    home = tmp_path / "home"
    home.mkdir()
    path = home / DEV_ENV_FILE
    odd = "KONTRA_GITHUB_CLIENT_SECRET=a\x0cb\u2028c\n"
    path.write_text("KONTRA_DEV=1\nKONTRA_PG_PASSWORD=pw\n" + odd, encoding="utf-8")
    path.chmod(0o600)
    rec, _ = _up_world(monkeypatch, {"KONTRA_PUBLIC_URL": "http://localhost:8459"})
    rec.config["x-kontra-dev-settings"] = {"KONTRA_DEV_GATE_PORT": "8459"}
    assert Cli(home)("dev", "up", "--no-login").exit_code == 0
    text = path.read_text(encoding="utf-8")
    assert odd in text
    assert text.endswith("KONTRA_DEV_GATE_PORT=8459\n")
