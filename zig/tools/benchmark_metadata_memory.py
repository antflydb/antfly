#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Launch a local split antfly cluster, run metadata-heavy API operations, and report memory.

This helper is macOS-specific because it shells out to `vmmap -summary`.
It uses only the Python standard library.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_BINARY = REPO_ROOT / "zig-out" / "bin" / "antfly"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", default=str(DEFAULT_BINARY), help="Path to the antfly binary")
    parser.add_argument("--tables", type=int, default=25, help="Number of tables to create")
    parser.add_argument("--indexes-per-table", type=int, default=1, help="Number of secondary indexes to create per table")
    parser.add_argument(
        "--schema-updates-per-table",
        type=int,
        default=1,
        help="Number of schema updates to apply per table",
    )
    parser.add_argument("--settle-seconds", type=float, default=3.0, help="Seconds to wait before and after the workload")
    parser.add_argument("--host", default="127.0.0.1", help="Bind host for local servers")
    parser.add_argument("--keep-tempdir", action="store_true", help="Keep the temporary data directory for inspection")
    parser.add_argument("--json", action="store_true", help="Emit the final report as JSON")
    return parser.parse_args()


def find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def wait_for_http(url: str, timeout_s: float, path: str) -> None:
    deadline = time.monotonic() + timeout_s
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            request_json("GET", f"{url}{path}")
            return
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            time.sleep(0.25)
    raise RuntimeError(f"timed out waiting for {url}{path}: {last_error}")


def request_json(method: str, url: str, payload: dict | None = None) -> dict | list | None:
    body = None if payload is None else json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=body, method=method)
    if body is not None:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = resp.read()
    except urllib.error.HTTPError as exc:
        err_body = exc.read().decode("utf-8", "replace")
        raise RuntimeError(f"{method} {url} failed with {exc.code}: {err_body}") from exc
    if not data:
        return None
    return json.loads(data)


def run_vmmap_summary(pid: int) -> str:
    proc = subprocess.run(
        ["vmmap", "-summary", str(pid)],
        check=True,
        capture_output=True,
        text=True,
    )
    lines = []
    for raw in proc.stdout.splitlines():
        if "Physical footprint" in raw or "VM_ALLOCATE" in raw or "STACK" in raw:
            lines.append(raw.strip())
    return "\n".join(lines)


def parse_physical_footprint(summary: str) -> str | None:
    for line in summary.splitlines():
        if line.startswith("Physical footprint:"):
            return line.split(":", 1)[1].strip()
    return None


class ManagedProc:
    def __init__(self, args: list[str], cwd: Path, log_path: Path):
        self.log_path = log_path
        self.log_file = log_path.open("w")
        self.proc = subprocess.Popen(args, cwd=str(cwd), stdout=self.log_file, stderr=subprocess.STDOUT)

    def stop(self) -> None:
        if self.proc.poll() is None:
            self.proc.send_signal(signal.SIGTERM)
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(timeout=10)
        self.log_file.close()

    def pid(self) -> int:
        return int(self.proc.pid)

    def log_tail(self, max_chars: int = 4000) -> str:
        self.log_file.flush()
        data = self.log_path.read_text(errors="replace")
        if len(data) <= max_chars:
            return data
        return data[-max_chars:]


def metadata_command(binary: str, host: str, raft_port: int, admin_port: int, root: Path) -> list[str]:
    return [
        binary,
        "metadata",
        "--raft",
        f"http://{host}:{raft_port}",
        "--api",
        f"http://{host}:{admin_port}",
        "--tick-ms",
        "5",
        "--replica-root-dir",
        str(root / "metadata-replicas"),
        "--replica-catalog-path",
        str(root / "metadata-catalog.txt"),
        "--snapshot-root-dir",
        str(root / "metadata-snapshots"),
    ]


def data_command(binary: str, host: str, port: int, metadata_admin_url: str, root: Path) -> list[str]:
    return [
        binary,
        "data",
        "--host",
        host,
        "--port",
        str(port),
        "--metadata-admin-base-uri",
        metadata_admin_url,
        "--node-id",
        "2",
        "--store-id",
        "2",
        "--tick-ms",
        "5",
        "--replica-root-dir",
        str(root / "data-replicas"),
        "--replica-catalog-path",
        str(root / "data-catalog.txt"),
    ]


def build_schema(version: int) -> dict:
    if version <= 0:
        version = 1
    props: dict[str, dict[str, str]] = {
        "title": {
            "type": "string",
            "x-antfly-types": ["text"],
            "x-antfly-include-in-all": True,
        },
        "body": {
            "type": "string",
            "x-antfly-types": ["text"],
            "x-antfly-include-in-all": True,
        },
    }
    for i in range(version):
        props[f"extra_field_{i}"] = {"type": "string"}
    return {
        "document_schemas": {
            "default": {
                "schema": {
                    "type": "object",
                    "properties": props,
                }
            }
        }
    }


def run_workload(base_url: str, table_count: int, indexes_per_table: int, schema_updates_per_table: int) -> None:
    for i in range(table_count):
        table_name = f"mem_bench_{i:04d}"
        request_json(
            "POST",
            f"{base_url}/tables/{table_name}",
            {"num_shards": 1, "description": f"metadata memory bench table {i}"},
        )
        for schema_version in range(schema_updates_per_table):
            request_json(
                "PUT",
                f"{base_url}/tables/{table_name}/schema",
                build_schema(schema_version + 1),
            )
        for index_i in range(indexes_per_table):
            index_name = f"embed_{index_i:02d}"
            request_json(
                "POST",
                f"{base_url}/tables/{table_name}/indexes/{index_name}",
                {
                    "name": index_name,
                    "type": "embeddings",
                    "external": True,
                    "dimension": 384,
                },
            )
        if i % 5 == 0:
            request_json("GET", f"{base_url}/tables/{table_name}")
            request_json("GET", f"{base_url}/tables/{table_name}/indexes")


def main() -> int:
    args = parse_args()
    binary = str(Path(args.binary).expanduser().resolve())
    if not Path(binary).exists():
        print(f"binary not found: {binary}", file=sys.stderr)
        return 1
    if shutil.which("vmmap") is None:
        print("vmmap not found; this tool currently supports macOS only", file=sys.stderr)
        return 1

    tmp = tempfile.TemporaryDirectory(prefix="antfly-metadata-memory-")
    root = Path(tmp.name)

    metadata_proc: ManagedProc | None = None
    data_proc: ManagedProc | None = None
    try:
        metadata_port = find_free_port()
        metadata_admin_port = find_free_port()
        data_port = find_free_port()

        metadata_url = f"http://{args.host}:{metadata_admin_port}"
        data_url = f"http://{args.host}:{data_port}"

        metadata_proc = ManagedProc(
            metadata_command(binary, args.host, metadata_port, metadata_admin_port, root),
            root,
            root / "metadata.log",
        )
        wait_for_http(metadata_url, 30.0, "/metadata/v1/status")

        data_proc = ManagedProc(
            data_command(binary, args.host, data_port, metadata_url, root),
            root,
            root / "data.log",
        )
        wait_for_http(data_url, 30.0, "/status")

        time.sleep(args.settle_seconds)

        before_metadata = run_vmmap_summary(metadata_proc.pid())
        before_data = run_vmmap_summary(data_proc.pid())

        workload_started = time.monotonic()
        run_workload(data_url, args.tables, args.indexes_per_table, args.schema_updates_per_table)
        workload_seconds = time.monotonic() - workload_started

        time.sleep(args.settle_seconds)

        after_metadata = run_vmmap_summary(metadata_proc.pid())
        after_data = run_vmmap_summary(data_proc.pid())

        report = {
            "binary": binary,
            "tempdir": str(root),
            "workload": {
                "tables": args.tables,
                "indexes_per_table": args.indexes_per_table,
                "schema_updates_per_table": args.schema_updates_per_table,
                "seconds": round(workload_seconds, 3),
            },
            "metadata": {
                "pid": metadata_proc.pid(),
                "before": before_metadata,
                "after": after_metadata,
                "before_physical_footprint": parse_physical_footprint(before_metadata),
                "after_physical_footprint": parse_physical_footprint(after_metadata),
            },
            "data": {
                "pid": data_proc.pid(),
                "before": before_data,
                "after": after_data,
                "before_physical_footprint": parse_physical_footprint(before_data),
                "after_physical_footprint": parse_physical_footprint(after_data),
            },
        }

        if args.json:
            print(json.dumps(report, indent=2, sort_keys=True))
        else:
            print(f"binary: {binary}")
            print(
                "workload: "
                f"{args.tables} tables, "
                f"{args.indexes_per_table} indexes/table, "
                f"{args.schema_updates_per_table} schema updates/table, "
                f"{report['workload']['seconds']}s"
            )
            print("")
            print(f"metadata pid {report['metadata']['pid']}")
            print("before:")
            print(report["metadata"]["before"])
            print("after:")
            print(report["metadata"]["after"])
            print("")
            print(f"data pid {report['data']['pid']}")
            print("before:")
            print(report["data"]["before"])
            print("after:")
            print(report["data"]["after"])

        if args.keep_tempdir:
            print(f"\nkept tempdir: {root}")
        return 0
    except Exception as exc:  # noqa: BLE001
        print(f"benchmark failed: {exc}", file=sys.stderr)
        if metadata_proc is not None:
            print("[metadata log]", file=sys.stderr)
            print(metadata_proc.log_tail(), file=sys.stderr)
        if data_proc is not None:
            print("[data log]", file=sys.stderr)
            print(data_proc.log_tail(), file=sys.stderr)
        return 1
    finally:
        if data_proc is not None:
            data_proc.stop()
        if metadata_proc is not None:
            metadata_proc.stop()
        if not args.keep_tempdir:
            tmp.cleanup()


if __name__ == "__main__":
    raise SystemExit(main())
