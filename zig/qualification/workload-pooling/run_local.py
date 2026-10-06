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
import signal
import socket
import subprocess
import sys
import tempfile
import time


def free_port():
    with socket.socket() as peer:
        peer.bind(("127.0.0.1", 0))
        return peer.getsockname()[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True, type=pathlib.Path)
    parser.add_argument("--manifest", required=True, type=pathlib.Path)
    parser.add_argument("--output", required=True, type=pathlib.Path)
    args = parser.parse_args()
    binary = args.binary.resolve()
    api_port, sql_port = free_port(), free_port()
    while sql_port == api_port:
        sql_port = free_port()
    root = pathlib.Path(tempfile.mkdtemp(prefix="antfly-pooling-qualification-"))
    config = {
        "pgwire": {"enabled": True, "bind_port": sql_port, "max_connections": 32},
        "admission": {"max_request_memory_bytes": 67108864},
    }
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
    )
    result = None
    startup_problem = None
    with log_path.open("w") as log:
        child = subprocess.Popen(
            command, stdout=log, stderr=subprocess.STDOUT, env=server_env
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
            result = subprocess.run(
                [
                    sys.executable,
                    str(pathlib.Path(__file__).with_name("idle_clients.py")),
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
    if args.output.exists():
        receipt = json.loads(args.output.read_text())
        receipt.update(
            binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),
            configuration=config,
            server_exit_code=child.returncode,
            server_log=str(log_path),
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
