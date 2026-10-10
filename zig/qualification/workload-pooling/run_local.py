#!/usr/bin/env python3
"""Launch an isolated native instance, qualify idle clients, and join shutdown."""

import argparse
import base64
import os
import secrets
import hashlib
import http.client
import json
import pathlib
import resource
import signal
import socket
import subprocess
import sys
import tempfile
import time
from limits import enforced_limits, check_container


def configuration_for_policy(manifest, policy):
    if policy not in ("baseline", "fixed", "failfast", "runnable"):
        raise ValueError("unknown qualification policy")
    config = json.loads(json.dumps(manifest.get("configuration", {})))
    if policy != "baseline":
        for key, value in manifest.get("candidate_configuration", {}).items():
            config.setdefault(key, {}).update(json.loads(json.dumps(value)))
    if "runnable_policy" in manifest:
        # The paired candidate uses exactly the same admission/memory ceilings.
        # Baseline binaries need not recognize the new configuration field.
        config.setdefault("admission", {}).pop("runnable", None)
    if policy == "runnable":
        runnable = manifest.get("runnable_policy")
        if not isinstance(runnable, dict) or not runnable:
            raise ValueError("runnable policy requires a fixed manifest policy")
        config.setdefault("admission", {})["runnable"] = json.loads(
            json.dumps(runnable)
        )
    if policy == "failfast":
        config.setdefault("admission", {}).setdefault("query", {})["waiting"] = {
            "max_queued_requests": 0,
            "max_queued_bytes": 0,
            "max_wait_ms": 0,
        }
    return config


def free_port():
    with socket.socket() as peer:
        peer.bind(("127.0.0.1", 0))
        return peer.getsockname()[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True, type=pathlib.Path)
    parser.add_argument("--manifest", required=True, type=pathlib.Path)
    parser.add_argument("--output", required=True, type=pathlib.Path)
    parser.add_argument(
        "--policy",
        choices=("fixed", "failfast", "baseline", "runnable"),
        default="fixed",
    )
    args = parser.parse_args()
    manifest = json.loads(args.manifest.read_text())
    args.output.parent.mkdir(parents=True, exist_ok=True)
    if args.output.exists():
        parser.error("output already exists; use a fresh receipt path")
    observed_limits = enforced_limits()
    limit_failures = check_container(manifest, observed_limits)
    if limit_failures:
        args.output.write_text(
            json.dumps(
                dict(
                    manifest=manifest,
                    passed=False,
                    failures=limit_failures,
                    enforced_limits=observed_limits,
                ),
                indent=2,
            )
            + "\n"
        )
        return True
    binary = args.binary.resolve()
    api_port, sql_port = free_port(), free_port()
    while sql_port == api_port:
        sql_port = free_port()
    root = pathlib.Path(tempfile.mkdtemp(prefix="antfly-pooling-qualification-"))
    try:
        config = configuration_for_policy(manifest, args.policy)
    except ValueError as error:
        args.output.write_text(
            json.dumps(
                dict(
                    manifest=manifest,
                    policy=args.policy,
                    passed=False,
                    failures=[str(error)],
                    enforced_limits=observed_limits,
                ),
                indent=2,
            )
            + "\n"
        )
        return True
    config.setdefault("pgwire", {}).update(enabled=True, bind_port=sql_port)
    config["pgwire"].setdefault(
        "max_connections", manifest.get("pgwire_connections", 32)
    )
    limits = manifest.get("process_limits", {})

    def child_limits():
        if "max_fds" in limits:
            resource.setrlimit(
                resource.RLIMIT_NOFILE, (limits["max_fds"], limits["max_fds"])
            )
        if "cpu_count" in limits and hasattr(os, "sched_getaffinity"):
            available = sorted(os.sched_getaffinity(0))
            os.sched_setaffinity(0, available[: limits["cpu_count"]])

    config_path = root / "config.json"
    config_path.write_text(json.dumps(config))
    log_path = root / "server.log"
    command = [
        str(binary),
        "standalone",
        "--config",
        str(config_path),
        "--auth",
        "true",
        "--host",
        "127.0.0.1",
        "--port",
        str(api_port),
        "--health",
        "false",
        "--data-dir",
        str(root / "data"),
        "--models-dir",
        str(root / "models"),
        "--ml-dir",
        str(root / "ml"),
        "--storage-engine",
        "lite",
        "--storage-path",
        str(root / "data.aflite"),
    ]
    password = secrets.token_urlsafe(24)
    authorization = "Basic " + base64.b64encode(("admin:" + password).encode()).decode()
    server_env = dict(os.environ, ANTFLY_BOOTSTRAP_ADMIN_PASSWORD=password)
    probe_env = dict(
        os.environ,
        ANTFLY_QUALIFICATION_AUTHORIZATION=authorization,
        ANTFLY_QUALIFICATION_PGWIRE_PASSWORD=password,
        ANTFLY_QUALIFICATION_POLICY=args.policy,
    )
    result = None
    startup_problem = None
    with log_path.open("w") as log:
        child = subprocess.Popen(
            command,
            stdout=log,
            stderr=subprocess.STDOUT,
            env=server_env,
            preexec_fn=child_limits,
        )
        try:
            deadline = time.monotonic() + 60
            while True:
                if child.poll() is not None:
                    raise RuntimeError(
                        f"server exited {child.returncode}; see {log_path}"
                    )
                try:
                    connection = http.client.HTTPConnection(
                        "127.0.0.1", api_port, timeout=1
                    )
                    try:
                        connection.request(
                            "GET",
                            "/db/v1/status",
                            headers={"Authorization": authorization},
                        )
                        response = connection.getresponse()
                        response.read()
                        if response.status == 200:
                            break
                    finally:
                        connection.close()
                except (OSError, http.client.HTTPException):
                    pass
                if time.monotonic() >= deadline:
                    raise RuntimeError(f"server did not become ready; see {log_path}")
                time.sleep(0.1)
            if manifest.get("http_auth_mode") == "api_key":
                peer = http.client.HTTPConnection("127.0.0.1", api_port, timeout=5)
                try:
                    peer.request(
                        "POST",
                        "/auth/v1/users/admin/api-keys",
                        json.dumps({"name": "pooling-qualification"}),
                        headers={
                            "Authorization": authorization,
                            "Content-Type": "application/json",
                        },
                    )
                    response = peer.getresponse()
                    payload = response.read()
                    if response.status != 201:
                        raise RuntimeError(
                            "ephemeral qualification API key creation failed"
                        )
                    probe_env["ANTFLY_QUALIFICATION_AUTHORIZATION"] = (
                        "ApiKey " + json.loads(payload)["encoded"]
                    )
                finally:
                    peer.close()
            result = subprocess.run(
                [
                    sys.executable,
                    str(
                        pathlib.Path(__file__).with_name(
                            "load_clients.py"
                            if manifest.get("kind") == "load"
                            else "idle_clients.py"
                        )
                    ),
                    "--pid",
                    str(child.pid),
                    "--status-url",
                    f"http://127.0.0.1:{api_port}/db/v1/status",
                    "--pgwire-port",
                    str(sql_port),
                    "--manifest",
                    str(args.manifest),
                    "--output",
                    str(args.output),
                    "--memory-map",
                ],
                env=probe_env,
            )
        except Exception as error:
            startup_problem = str(error)
        finally:
            if child.poll() is None:
                child.send_signal(signal.SIGINT)
                try:
                    child.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    child.kill()
                    child.wait()
    if not args.output.exists():
        args.output.write_text(
            json.dumps(
                {
                    "manifest": json.loads(args.manifest.read_text()),
                    "passed": False,
                    "failures": [
                        startup_problem
                        or "qualification probe failed before producing a receipt"
                    ],
                },
                indent=2,
            )
            + "\n"
        )
    # Container /tmp disappears after qualification. Keep a redacted server log
    # beside the durable receipt, including failed startup/shutdown attempts.
    saved_log = args.output.with_suffix(".server.log")
    log_text = log_path.read_text(errors="replace")
    for secret in (
        password,
        authorization,
        probe_env["ANTFLY_QUALIFICATION_AUTHORIZATION"],
    ):
        log_text = log_text.replace(secret, "[REDACTED]")
    saved_log.write_text(log_text)
    if args.output.exists():
        receipt = json.loads(args.output.read_text())
        receipt.update(
            policy=args.policy,
            process_limits=limits,
            enforced_limits=observed_limits,
            manifest_sha256=hashlib.sha256(args.manifest.read_bytes()).hexdigest(),
            binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),
            configuration=config,
            server_exit_code=child.returncode,
            server_log=str(saved_log),
            command=command,
        )
        if child.returncode != 0:
            receipt["failures"].append(f"server shutdown exited {child.returncode}")
            receipt["passed"] = False
        args.output.write_text(json.dumps(receipt, indent=2) + "\n")
        return not receipt["passed"]
    return result.returncode


if __name__ == "__main__":
    raise SystemExit(main())
