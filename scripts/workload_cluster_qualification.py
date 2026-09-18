"""Small local process fault correctness; never Cloud performance qualification.

Uses metadata/data CLI topology from zig/e2e/antfly/conftest.py. Every signal
addresses a process spawned by this run. No remote hosts, Docker or Kubernetes.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import hmac
import http.client
import json
import math
import os
import re
import secrets
import shutil
import signal
import socket
import subprocess
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import workload_qualification as q
import workload_scenarios as scenarios
import workload_fault_proxy as proxy_module
import workload_attempt_evidence as attempt_evidence

NAME = re.compile(r"[a-z][a-z0-9_-]{0,63}")


def template():
    return {
        "schema": 1,
        "purpose": "fault_correctness",
        "startup_timeout": 60,
        "request_timeout": 10,
        "max_schedule_lateness_seconds": 1,
        "artifacts": {
            "candidate": {
                "binary": "/absolute/path/to/antfly",
                "sha256": "REPLACE",
                "revision": "REPLACE",
                "optimization": "Debug",
            }
        },
        "nodes": [
            {
                "name": "metadata",
                "role": "metadata",
                "artifact": "candidate",
                "config": {},
            },
            {
                "name": "data",
                "role": "data",
                "node_id": 2,
                "store_role": "data",
                "metadata": "metadata",
                "artifact": "candidate",
                "config": {},
            },
            {
                "name": "api",
                "role": "data",
                "node_id": 3,
                "store_role": "api",
                "metadata": "metadata",
                "artifact": "candidate",
                "config": {},
            },
        ],
        "setup": [],
        "actions": [
            {"at": 0, "action": "pause", "node": "data"},
            {"at": 0.2, "action": "resume", "node": "data"},
            {"at": 0.3, "action": "ready", "node": "data"},
        ],
        "note": "Lifecycle correctness template only. Add exact workload/protocol assertions and predeclared faults. Pause is not a network partition; timer-triggered kill is not proof of a durable-decision boundary.",
    }


def validate(plan):
    if plan.get("schema") != 1 or plan.get("purpose") != "fault_correctness":
        raise ValueError("only schema1 fault_correctness process plans are accepted")
    for key, ceiling in (
        ("startup_timeout", 120),
        ("request_timeout", 60),
        ("max_schedule_lateness_seconds", 5),
    ):
        value = plan.get(key)
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(value)
            or not 0 < value <= ceiling
        ):
            raise ValueError(f"invalid bounded {key}")
    if type(plan.get("observe_unknown_setup", False)) is not bool:
        raise ValueError("observe_unknown_setup must be an explicit diagnostic boolean")
    artifacts = plan.get("artifacts", {})
    if not 1 <= len(artifacts) <= 8:
        raise ValueError("need 1..8 pinned local artifacts")
    for name, artifact in artifacts.items():
        if (
            not NAME.fullmatch(name)
            or not Path(artifact.get("binary", "")).is_file()
            or not re.fullmatch(r"[a-f0-9]{64}", artifact.get("sha256", ""))
            or not re.fullmatch(r"[a-f0-9]{40}", artifact.get("revision", ""))
        ):
            raise ValueError(
                "artifact requires safe name, existing binary, full revision and SHA256"
            )
        if artifact.get("optimization") not in {"Debug", "ReleaseSafe", "ReleaseFast"}:
            raise ValueError("declare actual artifact optimization")
    nodes = plan.get("nodes", [])
    if not 1 <= len(nodes) <= 8:
        raise ValueError("need 1..8 local nodes")
    names, ids, roles = set(), set(), {}
    for node in nodes:
        if not NAME.fullmatch(node.get("name", "")) or node["name"] in names:
            raise ValueError("unique safe node names required")
        if (
            node.get("artifact") not in artifacts
            or node.get("role") not in {"metadata", "data", "standalone"}
            or not isinstance(node.get("config"), dict)
        ):
            raise ValueError("invalid node artifact/role/config")
        if "proxy_api" in node and (
            type(node["proxy_api"]) is not bool or node["role"] != "data"
        ):
            raise ValueError("API advertisement proxy requires an explicit data role")
        if "proxy_capture_response_bytes" in node and (
            type(node["proxy_capture_response_bytes"]) is not int
            or not 1 <= node["proxy_capture_response_bytes"] <= 16384
            or not node.get("proxy_api")
        ):
            raise ValueError(
                "bounded response capture requires advertised proxy and1..16384bytes"
            )
        if node["role"] == "data":
            if roles.get(node.get("metadata")) != "metadata" or node.get(
                "store_role", "data"
            ) not in {"data", "api"}:
                raise ValueError(
                    "data nodes require an earlier metadata node and explicit role"
                )
            if (
                type(node.get("node_id")) is not int
                or node["node_id"] <= 0
                or node["node_id"] in ids
            ):
                raise ValueError("unique positive data node IDs required")
            ids.add(node["node_id"])
        names.add(node["name"])
        roles[node["name"]] = node["role"]
    if (
        len(plan.get("setup", [])) > 10000
        or not 1 <= len(plan.get("actions", [])) <= 1000
    ):
        raise ValueError("bounded setup and fault schedule required")
    submitted = set()
    checkpoints = set()
    discovery_ids, attempt_ids = set(), set()
    previous = 0
    for action in plan.get("setup", []) + plan["actions"]:
        kind = action.get("action")
        if kind not in {
            "request",
            "submit",
            "await",
            "pause",
            "resume",
            "stop",
            "kill",
            "restart",
            "ready",
            "discover",
            "metrics",
            "partition",
            "heal",
            "delay",
            "drop_response",
            "proxy_checkpoint",
            "assert_proxy",
            "attempt_status",
            "close_generation",
        }:
            raise ValueError("unsupported fault/action; no implicit shell commands")
        if kind != "await" and action.get("node") not in names:
            raise ValueError("action must target a node owned by this plan")
        if action in plan["actions"]:
            at = action.get("at")
            if (
                isinstance(at, bool)
                or not isinstance(at, (int, float))
                or not math.isfinite(at)
                or not previous <= at <= 600
            ):
                raise ValueError("fault schedule needs monotone finite offsets <=600s")
            previous = at
        elif kind != "request":
            raise ValueError("setup accepts synchronous checked requests only")
        if kind in {"request", "submit"}:
            if (
                action.get("method") not in {"GET", "POST", "PUT", "DELETE"}
                or not action.get("path", "").startswith("/")
                or "\r" in action["path"]
                or "\n" in action["path"]
            ):
                raise ValueError("invalid local request")
            if type(action.get("is_write")) is not bool:
                raise ValueError("request must declare unknown-write ambiguity")
            expect = action.get("expect", {})
            if expect.get("transport_error") is not True and (
                type(expect.get("status")) is not int
                or not 100 <= expect["status"] <= 599
            ):
                raise ValueError(
                    "request needs exact status or explicit transport failure expectation"
                )
            if 200 <= expect.get("status", 0) < 300 and not expect.get("checks"):
                raise ValueError("successful response needs semantic checks")
            for check in expect.get("checks", []):
                if not isinstance(check.get("path"), list) or set(check) not in (
                    {"path", "equals"},
                    {"path", "length"},
                    {"path", "sorted_equals"},
                ):
                    raise ValueError("checks need exact JSON path/value semantics")
            if len(json.dumps(action.get("body")).encode()) > 16 << 20:
                raise ValueError("request exceeds16MiB bounded fixture body")
        if kind in {
            "partition",
            "heal",
            "delay",
            "drop_response",
            "proxy_checkpoint",
            "assert_proxy",
        }:
            target = next(node for node in nodes if node["name"] == action["node"])
            if target.get("proxy_api") is not True or target["role"] != "data":
                raise ValueError("network faults need an advertised data API proxy")
            if kind == "delay" and (
                type(action.get("delay_ms")) is not int
                or not 0 <= action["delay_ms"] <= 10000
            ):
                raise ValueError("delay_ms must be0..10000")
            if kind == "proxy_checkpoint":
                key = (action["node"], action.get("id"))
                if not NAME.fullmatch(action.get("id", "")) or key in checkpoints:
                    raise ValueError("proxy checkpoint needs unique safe ID")
                checkpoints.add(key)
            if kind == "assert_proxy":
                if (action["node"], action.get("checkpoint")) not in checkpoints:
                    raise ValueError(
                        "proxy assertion requires an earlier checkpoint on the same node"
                    )
                if (
                    not action.get("minimums")
                    and not action.get("paths_include")
                    and not action.get("path_prefixes_include")
                ):
                    raise ValueError(
                        "proxy assertion needs actual forwarded traffic evidence"
                    )
                for metric, value in action.get("minimums", {}).items():
                    if (
                        metric
                        not in {
                            "accepted_connections",
                            "forwarded_upstream_bytes",
                            "forwarded_downstream_bytes",
                            "dropped_response_bytes",
                            "partition_rejections",
                        }
                        or type(value) is not int
                        or value < 1
                    ):
                        raise ValueError("invalid positive proxy counter delta")
        if action.get("via_proxy") and not next(
            node for node in nodes if node["name"] == action["node"]
        ).get("proxy_api"):
            raise ValueError("via_proxy requires configured owned proxy")
        if kind == "metrics":
            validate_metrics_action(action)
        if kind == "discover" and "compare" in action:
            relation = action["compare"]
            if (
                relation.get("previous") not in discovery_ids
                or relation.get("namespace") != "same"
                or relation.get("epoch") not in {"same", "increased"}
            ):
                raise ValueError(
                    "discovery comparison requires a prior identity and exact namespace/epoch relation"
                )
        if kind == "discover" and action.get("id"):
            if not NAME.fullmatch(action["id"]) or action["id"] in discovery_ids:
                raise ValueError("discovery IDs must be unique")
            discovery_ids.add(action["id"])
        if "attempt" in action:
            if (
                kind not in {"request", "submit"}
                or not NAME.fullmatch(action.get("id", ""))
                or action["attempt"].get("from_discovery") not in discovery_ids
                or action["id"] in attempt_ids
            ):
                raise ValueError(
                    "signed request requires unique ID and prior named discovery"
                )
            for field, bits in (
                ("generation", 64),
                ("sequence", 64),
                ("operation", 128),
            ):
                value = action["attempt"].get(field)
                if type(value) is not int or not 0 < value < 1 << bits:
                    raise ValueError("invalid configured attempt identity")
            attempt_ids.add(action["id"])
        if (
            kind in {"attempt_status", "close_generation"}
            and action.get("attempt_ref") not in attempt_ids
        ):
            raise ValueError("signed status/fence requires a named prior attempt")
        if kind == "submit":
            if (
                not NAME.fullmatch(action.get("id", ""))
                or action["id"] in submitted
                or len(submitted) >= 64
            ):
                raise ValueError("at most64 unique submitted requests")
            submitted.add(action["id"])
        if kind == "await" and action.get("id") not in submitted:
            raise ValueError("await must reference prior submit")
        if (
            kind == "restart"
            and action.get("artifact") is not None
            and action["artifact"] not in artifacts
        ):
            raise ValueError("rolling restart artifact must be pinned")
        if kind == "discover" and (
            type(action.get("coordinator")) is not int or action["coordinator"] <= 0
        ):
            raise ValueError("discovery needs explicit coordinator node ID")


def observe_unknown_setup(cluster, action, timeout):
    """Read-only diagnostic receipts never resolve or retry an ambiguous mutation."""
    target = action["node"]
    requests = [(target, "/db/v1/tables")]
    if action["path"].startswith("/db/v1/tables/"):
        requests.append((target, "/".join(action["path"].split("/")[:5])))
    metadata = cluster.nodes[target].get("metadata")
    if metadata:
        requests.append((metadata, "/metadata/v1/tables"))
    observations = []
    for node, path in requests:
        receipt = request(
            cluster.ports[node]["api"],
            {
                "method": "GET",
                "path": path,
                "is_write": False,
                "expect": {"status": 200},
            },
            min(2, timeout),
            raw=True,
        )
        receipt.update(
            node=node,
            path=path,
            diagnostic_only=True,
            original_mutation_remains_unknown=True,
        )
        cluster.record({"event": "unknown_setup_observation", **receipt})
        observations.append(receipt)
    return observations


def validate_metrics_action(action):
    for key, lower, upper in (
        ("timeout_seconds", 0.1, 30),
        ("interval_seconds", 0.1, 1),
        ("max_age_seconds", 0.001, 1),
        ("stable_seconds", 0, 10),
    ):
        value = action.get(key)
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(value)
            or not lower <= value <= upper
        ):
            raise ValueError(f"invalid bounded metrics {key}")
    if action["stable_seconds"] >= action["timeout_seconds"]:
        raise ValueError("metrics stability window must fit original timeout")
    if not action.get("expected"):
        raise ValueError("metrics action needs exact expected series")
    q.evidence.validate(action)


def compare_discovery(previous, current, relation):
    # Both values must already have verified signatures and request-bound nonces.
    for name in ("coordinator", "destination", "protocol_version", "worker_namespace"):
        if previous.get(name) != current.get(name) or name not in current:
            raise ValueError(f"discovery relation changed {name}")
    before, after = previous["worker_incarnation"], current["worker_incarnation"]
    if not (after == before if relation["epoch"] == "same" else after > before):
        raise ValueError("discovery incarnation relation failed")


def poll_metrics(port, action, submitted, emit):
    """Health listener only; stale caches never satisfy a sampled stability window."""
    validate_metrics_action(action)
    deadline = submitted + action["timeout_seconds"]
    stable_since, count, passed, failure = None, 0, False, None
    while time.monotonic() < deadline:
        began = time.monotonic()
        row = request(
            port,
            {
                "method": "GET",
                "path": "/metrics",
                "is_write": False,
                "expect": {"status": 200},
            },
            min(1, deadline - began),
            submitted=began,
            raw=True,
        )
        count += 1
        valid = False
        try:
            if not row["passed"]:
                raise ValueError("metrics transport/status unavailable")
            headers = {key.lower(): value for key, value in row["headers"].items()}
            if not headers.get("content-type", "").startswith(
                ("text/plain", "application/openmetrics-text")
            ):
                raise ValueError("health listener did not return Prometheus metrics")
            values = q.evidence.metrics(row["body"].encode())
            source_age = float(headers["x-antfly-metrics-age-ms"]) / 1000
            if not math.isfinite(source_age) or source_age < 0:
                raise ValueError("invalid metrics source age")
            age = source_age + time.monotonic() - began
            row["source_age_seconds"] = age
            if not math.isfinite(age) or not 0 <= age <= action["max_age_seconds"]:
                raise ValueError("metrics source age unavailable or stale")
            for name, maximum in action.get("ceilings", {}).items():
                if name not in values:
                    raise ValueError(f"missing required metric {name}")
                if values[name] > maximum:
                    failure = f"hard metric ceiling exceeded: {name}"
            valid = all(
                values.get(name) == expected
                for name, expected in action["expected"].items()
            )
            row["observed"] = {
                name: values.get(name)
                for name in {*action["expected"], *action.get("ceilings", {})}
            }
        except (ValueError, KeyError, TypeError) as error:
            row["evidence_error"] = str(error)
        now = time.monotonic()
        valid = valid and now <= deadline and failure is None
        stable_since = (
            (now if stable_since is None else stable_since) if valid else None
        )
        row["matches_expected"] = valid
        row["stable_seconds"] = 0 if stable_since is None else now - stable_since
        emit(row)  # Retain every raw body, including malformed HTML and stale samples.
        if failure:
            break
        if stable_since is not None and now - stable_since >= action["stable_seconds"]:
            passed = True
            break
        time.sleep(min(action["interval_seconds"], max(0, deadline - time.monotonic())))
    return {
        "passed": passed,
        "samples": count,
        "started_monotonic": submitted,
        "deadline_monotonic": deadline,
        "finished_monotonic": time.monotonic(),
        "failure": (
            failure
            if failure
            else None if passed else "original metrics action deadline expired"
        ),
        "expected": action["expected"],
        "sampled_stable_seconds": action["stable_seconds"],
        "scope": "fresh sampled production metrics, not signed per-attempt retirement proof",
    }


def encode(value):
    return base64.urlsafe_b64encode(
        json.dumps(value, separators=(",", ":"), sort_keys=True).encode()
    ).rstrip(b"=")


def service_headers(secret, issuer, coordinator):
    now = int(time.time())
    payload = {
        "iss": issuer,
        "sub": f"node:{coordinator}",
        "aud": "antfly-internal-v1",
        "principal_kind": "service",
        "admin": True,
        "iat": now,
        "exp": now + 60,
    }
    unsigned = encode({"alg": "HS256", "typ": "JWT"}) + b"." + encode(payload)
    signature = base64.urlsafe_b64encode(
        hmac.new(secret.encode(), unsigned, hashlib.sha256).digest()
    ).rstrip(b"=")
    return {"X-Antfly-Trusted-Principal": (unsigned + b"." + signature).decode()}


def verify_discovery(
    frame, secret, issuer, coordinator, destination, nonce, protocol_version=2
):
    if len(frame) > 8192:
        raise ValueError("oversized discovery evidence")
    payload, supplied = frame.encode().split(b".")
    mac = hmac.new(secret.encode(), digestmod=hashlib.sha256)
    for part in (b"antfly-workload-discovery-v1", issuer.encode(), payload):
        mac.update(len(part).to_bytes(8, "big"))
        mac.update(part)
    if not hmac.compare_digest(
        mac.digest(), base64.urlsafe_b64decode(supplied + b"=" * (-len(supplied) % 4))
    ):
        raise ValueError("invalid discovery signature")
    value = json.loads(base64.urlsafe_b64decode(payload + b"=" * (-len(payload) % 4)))
    if (
        any(
            value.get(key) != expected
            for key, expected in {
                "version": 1,
                "protocol_version": protocol_version,
                "coordinator": coordinator,
                "destination": destination,
                "nonce": nonce,
            }.items()
        )
        or type(value.get("worker_incarnation")) is not int
        or value["worker_incarnation"] <= 0
    ):
        raise ValueError("discovery identity/nonce/protocol mismatch")
    if protocol_version == 3 and (
        type(value.get("worker_namespace")) is not int or value["worker_namespace"] <= 0
    ):
        raise ValueError("protocol3 discovery requires durable namespace identity")
    return value


def node_command(node, binary, directory, ports, all_ports):
    role = node["role"]
    command = [str(binary), role]
    if role == "standalone":
        command += ["--host", "127.0.0.1", "--port", str(ports["api"])]
    else:
        command += [
            "--api-host",
            "127.0.0.1",
            "--api-port",
            str(ports["api"]),
            "--raft-host",
            "127.0.0.1",
            "--raft-port",
            str(ports["raft"]),
            "--raft-tick-ms",
            "5",
        ]
    command += [
        "--health",
        "true",
        "--health-port",
        str(ports["health"]),
        "--data-dir",
        str(directory / "data"),
        "--config",
        str(directory / "config.json"),
        "--control-tick-ms",
        "5",
        "--replica-root-dir",
        str(directory / "replicas"),
        "--replica-catalog-path",
        str(directory / "catalog.txt"),
    ]
    if role == "metadata":
        command += ["--snapshot-root-dir", str(directory / "snapshots")]
    if role == "data":
        command += [
            "--metadata-api",
            f"http://127.0.0.1:{all_ports[node['metadata']]['api']}",
            "--node-id",
            str(node["node_id"]),
            "--store-id",
            str(node["node_id"]),
            "--store-role",
            node.get("store_role", "data"),
        ]
    if node.get("proxy_api"):
        command += ["--api-advertise-url", f"http://127.0.0.1:{ports['api_proxy']}"]
    return command


def request(port, action, timeout, headers=None, submitted=None, *, raw=False):
    dispatched = time.monotonic()
    started = dispatched if submitted is None else submitted
    deadline = started + timeout
    connection = http.client.HTTPConnection(
        "127.0.0.1", port, timeout=max(0.001, deadline - dispatched)
    )
    receipt = {
        "started_monotonic": started,
        "dispatch_monotonic": dispatched,
        "client_wait_ms": (dispatched - started) * 1000,
        "is_write": action["is_write"],
    }
    send_started = False
    try:
        if time.monotonic() >= deadline:
            raise TimeoutError("original request deadline expired before dispatch")
        body = action.get("body")
        body = (
            None if body is None else json.dumps(body, separators=(",", ":")).encode()
        )
        send_started = True
        connection.request(
            action["method"],
            action["path"],
            body,
            {"Content-Type": "application/json", **(headers or {})},
        )
        response = connection.getresponse()
        chunks, size = [], 0
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("original request deadline expired")
            if connection.sock:
                connection.sock.settimeout(remaining)
            chunk = response.read1(min(65536, (1 << 20) + 1 - size))
            if not chunk:
                break
            size += len(chunk)
            if size > 1 << 20:
                raise ValueError("response exceeds1MiB receipt bound")
            chunks.append(chunk)
        if time.monotonic() > deadline:
            raise TimeoutError("original request deadline expired")
        data = b"".join(chunks)
        receipt.update(
            status=response.status,
            headers=dict(response.getheaders()),
            body=data.decode(errors="replace"),
        )
        # Receiving an explicit unknown-outcome response does not resolve a write.
        normalized_headers = {
            key.lower(): value for key, value in receipt["headers"].items()
        }
        if (
            action["is_write"]
            and normalized_headers.get("x-antfly-raft-mutation-outcome", "").strip()
            == "unknown-v1"
        ):
            receipt["unknown_write_outcome"] = True
        expect = action["expect"]
        if response.status != expect.get("status"):
            receipt["passed"] = False
        elif raw:
            receipt["passed"] = True
        elif 200 <= response.status < 300:
            receipt["passed"] = (
                scenarios.classify(action, response.status, data) == "completed"
            )
        else:
            receipt["passed"] = True
            value = json.loads(data) if expect.get("checks") else None
            for check in expect.get("checks", []):
                selected = value
                for key in check["path"]:
                    selected = selected[key]
                if "equals" in check and selected != check["equals"]:
                    receipt["passed"] = False
                if "length" in check and len(selected) != check["length"]:
                    receipt["passed"] = False
                if "sorted_equals" in check and sorted(selected) != sorted(
                    check["sorted_equals"]
                ):
                    receipt["passed"] = False
    except (ValueError, KeyError, IndexError, TypeError) as error:
        receipt.update(
            error=f"{type(error).__name__}: {error}",
            passed=False,
            invalid_result=True,
            unknown_write_outcome=action["is_write"] and send_started,
        )
    except (OSError, http.client.HTTPException) as error:
        receipt.update(
            error=f"{type(error).__name__}: {error}",
            passed=action["expect"].get("transport_error") is True,
            unknown_write_outcome=action["is_write"] and send_started,
        )
    finally:
        connection.close()
        receipt.update(
            finished_monotonic=time.monotonic(),
            elapsed_seconds=time.monotonic() - started,
        )
    return receipt


class Cluster:
    def __init__(self, plan, output):
        self.plan, self.output = plan, output
        self.nodes = {node["name"]: node for node in plan["nodes"]}
        self.ports, self.reservations, self.processes, self.logs = {}, {}, {}, {}
        self.generations, self.paused, self.expected_stopped = {}, set(), set()
        self.events, self.lock = deque(maxlen=1024), threading.Lock()
        self.proxies, self.proxy_checkpoints = {}, {}
        self.secret, self.issuer = (
            secrets.token_hex(32),
            "antfly-local-workload-correctness",
        )
        q.save(
            output / "local-test-auth.json",
            {
                "secret": self.secret,
                "issuer": self.issuer,
                "scope": "disposable local fixture credential",
            },
        )
        (output / "local-test-auth.json").chmod(0o600)
        try:
            for name in self.nodes:
                self.ports[name], self.reservations[name] = {}, []
                for kind in (
                    "api",
                    "raft",
                    "health",
                    *(["api_proxy"] if self.nodes[name].get("proxy_api") else []),
                ):
                    reservation = socket.socket()
                    self.reservations[name].append(reservation)
                    reservation.bind(("127.0.0.1", 0))
                    self.ports[name][kind] = reservation.getsockname()[1]
            q.save(output / "ports.json", self.ports)
            for name, node in self.nodes.items():
                if node.get("proxy_api"):
                    listener = self.reservations[name].pop()
                    self.proxies[name] = proxy_module.FaultProxy(
                        listener,
                        self.ports[name]["api"],
                        self.record,
                        name,
                        capture_response_bytes=node.get(
                            "proxy_capture_response_bytes", 0
                        ),
                        redact_values=(self.secret,),
                    )
        except BaseException:
            for proxy in self.proxies.values():
                proxy.close()
            for reservations in self.reservations.values():
                for reservation in reservations:
                    reservation.close()
            raise

    def record(self, event):
        with self.lock:
            event = {"monotonic": time.monotonic(), **event}
            self.events.append(event)
            with (self.output / "events.jsonl").open("a") as stream:
                stream.write(json.dumps(event, separators=(",", ":")) + "\n")

    def start(self, name, artifact=None):
        if name in self.processes and self.processes[name].poll() is None:
            raise RuntimeError("cannot start an already running node")
        node = self.nodes[name]
        artifact = artifact or node["artifact"]
        directory = self.output / name
        directory.mkdir(exist_ok=True)
        q.save(directory / "config.json", node["config"])
        generation = self.generations.get(name, 0) + 1
        self.generations[name] = generation
        argv = node_command(
            node,
            self.output / "artifacts" / artifact,
            directory,
            self.ports[name],
            self.ports,
        )
        log = (directory / f"server-{generation}.log").open("w")
        self.logs[name] = log
        for reservation in self.reservations[name]:
            reservation.close()
        self.reservations[name] = []
        env = {
            **os.environ,
            "ANTFLY_INTERNAL_SERVICE_SECRET": self.secret,
            "ANTFLY_INTERNAL_SERVICE_ISSUER": self.issuer,
        }
        process = subprocess.Popen(
            argv, stdout=log, stderr=subprocess.STDOUT, env=env, cwd=directory
        )
        self.processes[name] = process
        self.expected_stopped.discard(name)
        self.record(
            {
                "event": "start",
                "node": name,
                "generation": generation,
                "pid": process.pid,
                "artifact": artifact,
                "command": argv,
                "config_sha256": q.checksum(directory / "config.json"),
            }
        )
        self.ready(name)

    def ready(self, name):
        deadline = time.monotonic() + self.plan["startup_timeout"]
        path = (
            "/metadata/v1/status"
            if self.nodes[name]["role"] == "metadata"
            else "/db/v1/tables"
        )
        while time.monotonic() < deadline:
            if self.processes[name].poll() is not None:
                raise RuntimeError(
                    f"{name} exited before readiness: {self.processes[name].returncode}"
                )
            client = q.HTTP(
                self.ports[name]["api"], min(1, max(0.01, deadline - time.monotonic()))
            )
            try:
                status, body, _ = client.request("GET", path)
                if status == 200:
                    json.loads(body)
                    self.record(
                        {"event": "ready", "node": name, "path": path, "status": status}
                    )
                    return
            except (OSError, ValueError, http.client.HTTPException):
                pass
            finally:
                client.close()
            time.sleep(0.05)
        raise TimeoutError(f"{name} readiness deadline")

    def stop(self, name, kill=False, cleanup=False):
        process = self.processes.get(name)
        if process is None:
            return
        if name in self.paused and process.poll() is None:
            process.send_signal(signal.SIGCONT)
            self.paused.remove(name)
        forced = False
        if process.poll() is None:
            process.kill() if kill else process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                forced = True
                process.kill()
                process.wait(timeout=5)
        self.expected_stopped.add(name)
        self.record(
            {
                "event": "stop",
                "node": name,
                "pid": process.pid,
                "exit_code": process.returncode,
                "intentional_kill": kill,
                "forced_kill": forced,
                "cleanup": cleanup,
            }
        )
        if name in self.logs:
            self.logs.pop(name).close()
        if forced or (not kill and process.returncode != 0):
            raise RuntimeError(f"{name} did not stop cleanly ({process.returncode})")

    def fault(self, action):
        name, kind = action["node"], action["action"]
        if kind in {
            "partition",
            "heal",
            "delay",
            "drop_response",
            "proxy_checkpoint",
            "assert_proxy",
        }:
            proxy = self.proxies[name]
            snapshot = proxy.snapshot()
            if snapshot["error"]:
                raise RuntimeError(snapshot["error"])
            if kind == "proxy_checkpoint":
                self.proxy_checkpoints[(name, action["id"])] = snapshot
            elif kind == "assert_proxy":
                before = self.proxy_checkpoints[(name, action["checkpoint"])]
                for metric, minimum in action.get("minimums", {}).items():
                    if snapshot.get(metric, 0) - before.get(metric, 0) < minimum:
                        raise AssertionError(
                            f"proxy {name} lacks required {metric} traffic since checkpoint"
                        )
                for path in action.get("paths_include", []):
                    if snapshot["paths"].get(path, 0) <= before["paths"].get(path, 0):
                        raise AssertionError(
                            f"proxy {name} did not observe expected path {path}"
                        )
                for prefix in action.get("path_prefixes_include", []):
                    if not any(
                        path.startswith(prefix) and count > before["paths"].get(path, 0)
                        for path, count in snapshot["paths"].items()
                    ):
                        raise AssertionError(
                            f"proxy {name} did not observe expected prefix {prefix}"
                        )
            else:
                policy = snapshot["policy"]
                if kind == "heal":
                    policy = {}
                elif kind == "partition":
                    policy["partition"] = True
                elif kind == "delay":
                    policy["delay_ms"] = action["delay_ms"]
                else:
                    policy["drop_response"] = True
                proxy.set_policy(**policy)
            self.record(
                {"event": kind, "node": name, "proxy_snapshot": proxy.snapshot()}
            )
            return
        process = self.processes[name]
        if kind in {"pause", "resume"}:
            if process.poll() is not None:
                raise RuntimeError("cannot signal exited process")
            if (kind == "pause") == (name in self.paused):
                raise RuntimeError("invalid pause/resume state")
            process.send_signal(signal.SIGSTOP if kind == "pause" else signal.SIGCONT)
            self.paused.add(name) if kind == "pause" else self.paused.remove(name)
            self.record({"event": kind, "node": name, "pid": process.pid})
        elif kind in {"kill", "stop"}:
            self.stop(name, kill=kind == "kill")
        elif kind == "restart":
            if process.poll() is None:
                self.stop(name)
            elif name not in self.expected_stopped:
                raise RuntimeError(
                    "unexpected prior crash cannot become an expected restart"
                )
            self.start(name, action.get("artifact"))
        elif kind == "ready":
            self.ready(name)

    def close(self):
        errors = []
        for name in reversed(list(self.nodes)):
            try:
                process = self.processes.get(name)
                if (
                    process is not None
                    and process.poll() is not None
                    and name not in self.expected_stopped
                ):
                    errors.append(f"unexpected {name} exit {process.returncode}")
                self.stop(
                    name,
                    kill=name in self.expected_stopped
                    and process is not None
                    and process.returncode == -signal.SIGKILL,
                    cleanup=True,
                )
            except Exception as error:
                errors.append(str(error))
        for name, proxy in self.proxies.items():
            try:
                proxy.close()
                self.record(
                    {
                        "event": "proxy_final",
                        "node": name,
                        "proxy_snapshot": proxy.snapshot(),
                    }
                )
            except Exception as error:
                errors.append(str(error))
        for reservations in self.reservations.values():
            for reservation in reservations:
                reservation.close()
        for log in self.logs.values():
            log.close()
        return errors


def run(plan, output):
    validate(plan)
    output = output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    q.save(output / "plan.json", plan)
    for source in (
        __file__,
        q.__file__,
        scenarios.__file__,
        q.vectors.__file__,
        q.evidence.__file__,
        proxy_module.__file__,
        attempt_evidence.__file__,
    ):
        shutil.copy2(source, output / Path(source).name)
    q.save(
        output / "host.json",
        {
            "platform": q.platform.platform(),
            "machine": q.platform.machine(),
            "purpose": "local fault correctness",
            "resource_envelope_enforced": False,
        },
    )
    cluster, failure, cleanup_errors = None, None, []
    results, pending, schedule_invalid = [], {}, []
    setup_observations = []
    discoveries, signed_attempts, protocol_debt = {}, {}, {}
    results_lock = threading.Lock()
    try:
        (output / "artifacts").mkdir()
        for name, artifact in plan["artifacts"].items():
            path = output / "artifacts" / name
            shutil.copyfile(artifact["binary"], path)
            if q.checksum(path) != artifact["sha256"]:
                raise ValueError(f"artifact {name} SHA256 mismatch")
            path.chmod(0o500)
        cluster = Cluster(plan, output)
        for name in cluster.nodes:
            cluster.start(name)

        def execute(action, submitted=None):
            node = cluster.nodes[action["node"]]
            headers = None
            if action.get("coordinator"):
                headers = service_headers(
                    cluster.secret, cluster.issuer, action["coordinator"]
                )
            actual = action
            attempt, protocol_version = None, action.get("protocol_version", 3)
            if action["action"] in {"attempt_status", "close_generation"}:
                with results_lock:
                    attempt = signed_attempts[action["attempt_ref"]].copy()
                actual = {
                    **action,
                    "method": "POST",
                    "path": "/internal/v1/workload/control",
                    "is_write": False,
                    "body": {
                        "workload_attempt_control": (
                            "status"
                            if action["action"] == "attempt_status"
                            else "close_generation"
                        )
                    },
                    "expect": action.get(
                        "expect",
                        {"status": 200, "checks": [{"path": [], "equals": {}}]},
                    ),
                }
            elif "attempt" in action:
                configured = action["attempt"]
                with results_lock:
                    discovery = discoveries[configured["from_discovery"]]
                attempt = {
                    key: configured[key]
                    for key in ("generation", "sequence", "operation")
                }
                attempt.update(
                    {
                        key: discovery[key]
                        for key in (
                            "coordinator",
                            "destination",
                            "worker_incarnation",
                            "worker_namespace",
                        )
                    }
                )
                attempt_evidence.validate_attempt(attempt, protocol_version)
                with results_lock:
                    signed_attempts[action["id"]] = attempt.copy()
                    protocol_debt[action["id"]] = "unproven"
            if attempt is not None:
                if attempt["destination"] != node["node_id"]:
                    raise ValueError(
                        "signed attempt destination differs from target node"
                    )
                headers = service_headers(
                    cluster.secret, cluster.issuer, attempt["coordinator"]
                )
                body = json.dumps(actual.get("body"), separators=(",", ":")).encode()
                remaining = plan["request_timeout"] - (
                    time.monotonic() - submitted if submitted is not None else 0
                )
                headers["X-Antfly-Workload-Attempt"] = attempt_evidence.sign_request(
                    cluster.secret,
                    cluster.issuer,
                    attempt,
                    actual["method"],
                    actual["path"],
                    body,
                    max(1, int(remaining * 1e9)),
                    version=protocol_version,
                    acknowledged_through=action.get("acknowledged_through", 0),
                )
            if action["action"] == "discover":
                nonce = secrets.randbits(127) + 1
                actual = {
                    **action,
                    "method": "POST",
                    "path": "/internal/v1/workload/control",
                    "is_write": False,
                    "body": {"workload_attempt_control": "discover", "nonce": nonce},
                    "expect": {"status": 200, "checks": [{"path": [], "equals": {}}]},
                }
            result = request(
                cluster.ports[action["node"]][
                    "api_proxy" if action.get("via_proxy") else "api"
                ],
                actual,
                plan["request_timeout"],
                headers,
                submitted,
            )
            if action["action"] == "discover" and result["passed"]:
                try:
                    frame = {
                        key.lower(): value for key, value in result["headers"].items()
                    }["x-antfly-workload-evidence"]
                    result["discovery"] = verify_discovery(
                        frame,
                        cluster.secret,
                        cluster.issuer,
                        action["coordinator"],
                        node["node_id"],
                        nonce,
                        protocol_version,
                    )
                    if action.get("compare"):
                        with results_lock:
                            prior = discoveries[action["compare"]["previous"]]
                        compare_discovery(prior, result["discovery"], action["compare"])
                        result["verified_discovery_relation"] = action["compare"]
                    if action.get("id"):
                        with results_lock:
                            discoveries[action["id"]] = result["discovery"]
                except (ValueError, KeyError, TypeError) as error:
                    result.update(passed=False, evidence_error=str(error))
            if attempt is not None:
                result["attempt"] = attempt
                frame = {
                    key.lower(): value
                    for key, value in result.get("headers", {}).items()
                }.get("x-antfly-workload-evidence")
                if frame is not None:
                    try:
                        if action["action"] == "close_generation":
                            proof = attempt_evidence.fence(
                                cluster.secret,
                                cluster.issuer,
                                frame,
                                attempt,
                                protocol_version,
                            )
                        else:
                            proof = attempt_evidence.terminal(
                                cluster.secret,
                                cluster.issuer,
                                frame,
                                attempt,
                                result["status"],
                                result["body"].encode(),
                                protocol_version,
                            )
                        result["verified_retirement_evidence"] = proof
                        with results_lock:
                            if action["action"] == "close_generation":
                                for key, prior in signed_attempts.items():
                                    if (
                                        all(
                                            prior[field] == attempt[field]
                                            for field in (
                                                "coordinator",
                                                "destination",
                                                "worker_incarnation",
                                                "worker_namespace",
                                            )
                                        )
                                        and prior["generation"]
                                        <= proof["quiesced_through"]
                                    ):
                                        protocol_debt[key] = "verified_fence"
                            else:
                                protocol_debt[
                                    action.get("attempt_ref", action.get("id"))
                                ] = "verified_terminal"
                    except (ValueError, KeyError, TypeError) as error:
                        result.update(passed=False, evidence_error=str(error))
                elif (
                    action["action"] in {"attempt_status", "close_generation"}
                    and result.get("status") == 200
                ):
                    result.update(
                        passed=False,
                        evidence_error="successful control response lacks signed retirement proof",
                    )
            result.update(
                node=action["node"], action=action["action"], id=action.get("id")
            )
            with results_lock:
                results.append(result)
            cluster.record({"event": "request", **result})
            return result

        for action in plan.get("setup", []):
            setup_result = execute(action)
            if setup_result.get("unknown_write_outcome"):
                if plan.get("observe_unknown_setup"):
                    setup_observations.extend(
                        observe_unknown_setup(cluster, action, plan["request_timeout"])
                    )
                raise RuntimeError("setup mutation outcome unknown; no writes replayed")
            if not setup_result["passed"]:
                raise RuntimeError("setup assertion failed; no writes replayed")
        origin = time.monotonic()
        cluster.record({"event": "schedule_start", "origin": origin})
        with ThreadPoolExecutor(max_workers=32) as executor:
            for index, action in enumerate(plan["actions"]):
                due = origin + action["at"]
                time.sleep(max(0, due - time.monotonic()))
                lateness = max(0, time.monotonic() - due)
                if lateness > plan["max_schedule_lateness_seconds"]:
                    schedule_invalid.append(
                        {"index": index, "lateness_seconds": lateness}
                    )
                cluster.record(
                    {
                        "event": "dispatch",
                        "index": index,
                        "scheduled_at": action["at"],
                        "lateness_seconds": lateness,
                        "action": action["action"],
                        "node": action.get("node"),
                    }
                )
                for name, process in cluster.processes.items():
                    if (
                        process.poll() is not None
                        and name not in cluster.expected_stopped
                    ):
                        raise RuntimeError(
                            f"unexpected {name} exit {process.returncode}"
                        )
                if action["action"] == "metrics":
                    result = poll_metrics(
                        cluster.ports[action["node"]]["health"],
                        action,
                        due,
                        lambda row: cluster.record(
                            {
                                "event": "metrics_sample",
                                "index": index,
                                "node": action["node"],
                                **row,
                            }
                        ),
                    )
                    result.update(node=action["node"], action="metrics")
                    with results_lock:
                        results.append(result)
                    cluster.record({"event": "metrics_result", **result})
                    if not result["passed"]:
                        raise RuntimeError(f"action{index} metrics assertion failed")
                elif action["action"] in {
                    "request",
                    "discover",
                    "attempt_status",
                    "close_generation",
                }:
                    if not execute(action)["passed"]:
                        raise RuntimeError(f"action{index} assertion failed")
                elif action["action"] == "submit":
                    pending[action["id"]] = executor.submit(
                        execute, action, time.monotonic()
                    )
                elif action["action"] == "await":
                    if not pending[action["id"]].result(
                        timeout=plan["request_timeout"] + 1
                    )["passed"]:
                        raise RuntimeError(
                            f"submitted request {action['id']} assertion failed"
                        )
                else:
                    cluster.fault(action)
            for name, future in pending.items():
                if not future.result(timeout=plan["request_timeout"] + 1)["passed"]:
                    raise RuntimeError(f"submitted request {name} assertion failed")
    except BaseException as error:
        failure = f"{type(error).__name__}: {error}"
    finally:
        if cluster is not None:
            cleanup_errors = cluster.close()
        unresolved = [
            result for result in results if result.get("unknown_write_outcome")
        ]
        passed = (
            failure is None
            and not cleanup_errors
            and not unresolved
            and all(value != "unproven" for value in protocol_debt.values())
            and not schedule_invalid
            and all(result["passed"] for result in results)
        )
        summary = {
            "status": (
                "fault_correctness_evidence" if passed else "fault_correctness_failed"
            ),
            "exit_code": 0 if passed else 1,
            "correctness_passed": passed,
            "error": failure,
            "cleanup_errors": cleanup_errors,
            "schedule_invalid": schedule_invalid,
            "setup_observations": setup_observations,
            "unknown_write_outcomes": len(unresolved),
            "unresolved_durable_obligations": len(unresolved),
            "fixture_protocol_obligations": protocol_debt,
            "protocol_obligation_scope": "manual signed fixture attempts only; not coordinator production ledger accounting",
            "requests": len(results),
            "results": results,
            "performance_qualified": False,
            "release_qualified": False,
            "unmeasured": [
                "Cloud resource envelopes and performance",
                "network faults not explicitly asserted through an advertised proxy",
                "exact durable-decision crash boundary",
                "unasserted remote ownership/reconciliation states",
            ],
        }
        q.save(output / "summary.json", summary)
        q.save(
            output / "checksums.json",
            {
                str(path.relative_to(output)): q.checksum(path)
                for path in sorted(output.rglob("*"))
                if path.is_file()
                and path.name != "checksums.json"
                and not any(
                    part in {"data", "replicas", "snapshots"}
                    for part in path.relative_to(output).parts
                )
            },
        )
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    create = sub.add_parser("template")
    create.add_argument("--output", type=Path, required=True)
    execute = sub.add_parser("run")
    execute.add_argument("plan", type=Path)
    execute.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "template":
        if args.output.exists():
            parser.error("refusing to replace an existing plan")
        q.save(args.output, template())
        return 0
    result = run(json.loads(args.plan.read_text()), args.output)
    print(
        json.dumps(
            {
                key: result[key]
                for key in (
                    "status",
                    "requests",
                    "unknown_write_outcomes",
                    "error",
                    "cleanup_errors",
                )
            }
        )
    )
    return result["exit_code"]


if __name__ == "__main__":
    raise SystemExit(main())
