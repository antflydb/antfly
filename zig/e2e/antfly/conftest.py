# Copyright 2026 Antfly, Inc.
#
# Licensed under the Elastic License 2.0 (ELv2); you may not use this file
# except in compliance with the Elastic License 2.0. You may obtain a copy of
# the Elastic License 2.0 at
#
#     https://www.antfly.io/licensing/ELv2-license
#
# Unless required by applicable law or agreed to in writing, software distributed
# under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# Elastic License 2.0 for the specific language governing permissions and
# limitations.

"""Shared fixtures for antfly-zig E2E tests.

Usage:
    ANTFLY_SERVERLESS_URL=http://127.0.0.1:8080 uv run --project e2e/antfly pytest e2e/antfly

    # Start a local swarm binary automatically:
    ANTFLY_BIN=./zig-out/bin/antfly uv run --project e2e/antfly pytest e2e/antfly

    # Run stateful tests against an existing server:
    ANTFLY_STATEFUL_URL=http://127.0.0.1:8080 uv run --project e2e/antfly pytest e2e/antfly/test_schema_migration.py
    # For Go Antfly, also set ANTFLY_STATEFUL_API_ROOT=/api/v1.

    # Or start the local unified stateful entrypoint automatically:
    ANTFLY_BIN=./zig-out/bin/antfly uv run --project e2e/antfly pytest e2e/antfly/test_schema_migration.py
"""

from __future__ import annotations

import json
import os
import re
import signal
import socket
import subprocess
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import quote, urlparse
from typing import Any

import pytest
import requests

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_ANTFLY_BIN = REPO_ROOT / "zig-out" / "bin" / "antfly"
ANTFLY_PUBLIC_API_ROOT = "/api/v1"
ANTFLY_INTERNAL_API_ROOT = "/internal/v1"
TERMITE_PUBLIC_API_ROOT = "/ml/v1"
CLIPCLAP_MODEL = "antflydb/clipclap"
CLIPCLAP_GGUF_FILES = (
    "clipclap-clip.Q4_K.gguf",
    "clipclap-clap.Q4_K.gguf",
    "termite_variants.json",
)
ALLOW_REAL_MODEL_DOWNLOAD_ENV = "ANTFLY_E2E_ALLOW_REAL_MODEL_DOWNLOAD"


def resolve_binary_path(binary: str) -> str:
    return str(Path(binary).expanduser().resolve())


def preserve_e2e_root() -> bool:
    value = os.environ.get("ANTFLY_E2E_PRESERVE_ROOT", "")
    return value != "" and value not in {"0", "false", "False"}


def maybe_preserve_tempdir(tempdir: tempfile.TemporaryDirectory[str]) -> bool:
    if not preserve_e2e_root():
        return False
    tempdir._finalizer.detach()
    print(f"preserving e2e tempdir: {tempdir.name}")
    return True


def default_antfly_api_root(binary: str) -> str:
    return ANTFLY_PUBLIC_API_ROOT if Path(binary).name == "antfly" else ""


def with_api_root(base_url: str, root: str) -> str:
    normalized_root = root.strip()
    normalized_base = base_url.rstrip("/")
    if normalized_root and normalized_root != "/":
        if not normalized_root.startswith("/"):
            normalized_root = f"/{normalized_root}"
        normalized_base = f"{normalized_base}{normalized_root.rstrip('/')}"
    return normalized_base


def prefixed_api_path(root: str, path: str) -> str:
    normalized_root = root.rstrip("/")
    normalized_path = path.strip()
    if not normalized_path:
        return normalized_root or "/"
    if not normalized_path.startswith("/"):
        normalized_path = f"/{normalized_path}"
    return f"{normalized_root}{normalized_path}"


def antfly_public_api_url(base_url: str, *, binary: str | None = None, root: str | None = None) -> str:
    if root is None:
        root = default_antfly_api_root(binary) if binary is not None else ANTFLY_PUBLIC_API_ROOT
    return with_api_root(base_url, root)


def lookup_key_path(table_name: str, key: str) -> str:
    return f"/tables/{table_name}/lookup/{quote(key, safe='')}"


def antfly_internal_api_path(path: str) -> str:
    return prefixed_api_path(ANTFLY_INTERNAL_API_ROOT, path)


def termite_public_api_url(base_url: str) -> str:
    return with_api_root(base_url, TERMITE_PUBLIC_API_ROOT)


def find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def wait_for_server(
    url: str,
    timeout: float = 30.0,
    path: str = "/status",
    *,
    allow_unauthorized: bool = False,
) -> bool:
    deadline = time.monotonic() + timeout
    consecutive_successes = 0
    while time.monotonic() < deadline:
        try:
            resp = requests.get(f"{url}{path}", timeout=2)
            if resp.ok:
                consecutive_successes += 1
                if consecutive_successes >= 2:
                    return True
            elif allow_unauthorized and resp.status_code == 401:
                consecutive_successes += 1
                if consecutive_successes >= 2:
                    return True
            else:
                consecutive_successes = 0
        except requests.ConnectionError:
            consecutive_successes = 0
        time.sleep(0.25)
    return False


def wait_for_listener(url: str, timeout: float = 5.0) -> bool:
    parsed = urlparse(url)
    host = parsed.hostname or "127.0.0.1"
    port = parsed.port
    if port is None:
        return False
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.5):
                return True
        except OSError:
            time.sleep(0.05)
    return False


def _start_stateful_server_with_retry(binary: str, port: int) -> PublicAntflyServer | SwarmAntflyServer:
    if Path(binary).name != "antfly":
        return PublicAntflyServer(binary, "127.0.0.1", port)

    last_error: RuntimeError | None = None
    for _ in range(3):
        try:
            return SwarmAntflyServer(binary, "127.0.0.1", port)
        except RuntimeError as exc:
            last_error = exc
            time.sleep(0.5)
    assert last_error is not None
    raise last_error


def ready_index_status(index_info: dict[str, Any]) -> dict[str, Any] | None:
    status = index_info.get("status")
    if status is None:
        return None
    if not isinstance(status, dict):
        return None
    if status.get("rebuilding", status.get("backfill_active", False)):
        return None
    if status.get("dense_publish_pending", False):
        return None
    if status.get("replay_catch_up_required", False):
        return None
    if status.get("catch_up_active", False):
        return None
    return status


def raise_request_error_with_logs(
    err: requests.RequestException,
    server_ref: AntflyServer | PublicAntflyServer | SwarmAntflyServer | StatefulAntflyServer | None,
) -> None:
    logs = ""
    proc_status = None
    if server_ref is not None:
        logs = server_ref.debug_logs().strip()
        proc = getattr(server_ref, "proc", None)
        if proc is not None:
            proc_status = proc.poll()
    if not logs and proc_status is None:
        raise err
    message = f"{err}\nserver logs:\n{logs}"
    if proc_status is not None:
        message += f"\nserver exit status: {proc_status}"
    raise err.__class__(
        message,
        request=getattr(err, "request", None),
        response=getattr(err, "response", None),
    ) from err


def _read_log_tail(path: Path, *, limit: int = 20000) -> str:
    if not path.exists():
        return ""
    data = path.read_text(errors="replace")
    if len(data) <= limit:
        return data
    return data[-limit:]


class AntflyServer:
    def __init__(self, binary: str, host: str, port: int):
        self.url = f"http://{host}:{port}"
        self.tempdir = tempfile.TemporaryDirectory(prefix="antfly-zig-e2e-")
        root = Path(self.tempdir.name)
        self.log_path = root / "server.log"
        self.log_file = self.log_path.open("w")
        env = os.environ.copy()
        env.update(
            {
                "ANTFLY_SERVERLESS_ARTIFACTS_URI": f"file://{root / 'artifacts'}",
                "ANTFLY_SERVERLESS_MANIFESTS_URI": f"file://{root / 'manifests'}",
                "ANTFLY_SERVERLESS_WAL_URI": f"file://{root / 'wal'}",
                "ANTFLY_SERVERLESS_PROGRESS_URI": f"file://{root / 'progress'}",
                "ANTFLY_SERVERLESS_CATALOG_URI": f"file://{root / 'catalog'}",
                "ANTFLY_SERVERLESS_QUERY_CACHE_DIR": str(root / "cache"),
            }
        )
        command = _serverless_swarm_command(binary, host=host, port=port, root=root)
        self.proc = subprocess.Popen(command, stdout=self.log_file, stderr=subprocess.STDOUT, env=env, cwd=REPO_ROOT)
        if not wait_for_server(self.url):
            self.stop()
            out = _read_log_tail(self.log_path)
            raise RuntimeError(f"Server failed to start at {self.url}\n{out}")

    def debug_logs(self) -> str:
        self.log_file.flush()
        return _read_log_tail(self.log_path)

    def stop(self) -> None:
        if self.proc and self.proc.poll() is None:
            self.proc.send_signal(signal.SIGTERM)
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()
        self.log_file.close()
        if not maybe_preserve_tempdir(self.tempdir):
            self.tempdir.cleanup()


class PublicAntflyServer:
    def __init__(self, binary: str, host: str, port: int):
        self.binary = binary
        self.host = host
        self.port = port
        self.url = f"http://{host}:{port}"
        self.tempdir = tempfile.TemporaryDirectory(prefix="antfly-zig-public-e2e-")
        root = Path(self.tempdir.name)
        self.root = root
        self.replica_root = root / "replicas"
        self.log_path = root / "server.log"
        self.log_file = self.log_path.open("w")
        command = _legacy_stateful_command(binary, host=host, port=port, root=root)
        self.proc = subprocess.Popen(command, stdout=self.log_file, stderr=subprocess.STDOUT, cwd=root)
        if not wait_for_server(self.url):
            self.stop()
            out = _read_log_tail(self.log_path)
            raise RuntimeError(f"Public API server failed to start at {self.url}\n{out}")

    def debug_logs(self) -> str:
        self.log_file.flush()
        return _read_log_tail(self.log_path)

    def pause(self) -> None:
        if self.proc and self.proc.poll() is None:
            self.proc.send_signal(signal.SIGTERM)
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()
        self.proc = None

    def resume(self) -> None:
        command = _legacy_stateful_command(self.binary, host=self.host, port=self.port, root=self.root)
        self.proc = subprocess.Popen(command, stdout=self.log_file, stderr=subprocess.STDOUT, cwd=self.root)
        if not wait_for_server(self.url):
            out = _read_log_tail(self.log_path)
            self.stop()
            raise RuntimeError(f"Public API server failed to resume at {self.url}\n{out}")

    def stop(self) -> None:
        self.pause()
        self.log_file.close()
        if not maybe_preserve_tempdir(self.tempdir):
            self.tempdir.cleanup()


def _serverless_swarm_command(binary: str, *, host: str, port: int, root: Path) -> list[str]:
    basename = Path(binary).name
    if basename == "antfly":
        return [
            binary,
            "serverless",
            "swarm",
            "--host",
            host,
            "--port",
            str(port),
            "--tick-ms",
            "5",
        ]
    return [
        binary,
        "--host",
        host,
        "--port",
        str(port),
        "--tick-ms",
        "5",
        "--replica-root-dir",
        str(root / "replicas"),
        "--replica-catalog-path",
        str(root / "catalog.txt"),
        "--snapshot-root-dir",
        str(root / "snapshots"),
    ]


def _legacy_stateful_command(binary: str, *, host: str, port: int, root: Path) -> list[str]:
    return [
        binary,
        "--host",
        host,
        "--port",
        str(port),
        "--tick-ms",
        "5",
        "--replica-root-dir",
        str(root / "replicas"),
        "--replica-catalog-path",
        str(root / "catalog.txt"),
        "--snapshot-root-dir",
        str(root / "snapshots"),
    ]


def _swarm_stateful_command(binary: str, *, host: str, port: int, root: Path) -> list[str]:
    return [
        binary,
        "swarm",
        "--host",
        host,
        "--port",
        str(port),
        "--data-dir",
        str(root),
        "--tick-ms",
        "5",
        "--replica-root-dir",
        str(root / "replicas"),
        "--replica-catalog-path",
        str(root / "catalog.txt"),
        "--snapshot-root-dir",
        str(root / "snapshots"),
    ]


def _metadata_command(binary: str, *, host: str, raft_port: int, admin_port: int, root: Path) -> list[str]:
    return [
        binary,
        "metadata",
        "--raft-host",
        host,
        "--raft-port",
        str(raft_port),
        "--api-host",
        host,
        "--api-port",
        str(admin_port),
        "--data-dir",
        str(root),
        "--tick-ms",
        "5",
        "--replica-root-dir",
        str(root / "metadata-replicas"),
        "--replica-catalog-path",
        str(root / "metadata-catalog.txt"),
        "--snapshot-root-dir",
        str(root / "metadata-snapshots"),
    ]


def _data_command(
    binary: str,
    *,
    host: str,
    port: int,
    raft_port: int,
    metadata_admin_base_uri: str,
    root: Path,
    auth_enabled: bool = False,
) -> list[str]:
    command = [
        binary,
        "data",
        "--api-host",
        host,
        "--api-port",
        str(port),
        "--raft-host",
        host,
        "--raft-port",
        str(raft_port),
        "--metadata-api",
        metadata_admin_base_uri,
        "--node-id",
        "2",
        "--store-id",
        "2",
        "--data-dir",
        str(root),
        "--tick-ms",
        "5",
        "--replica-root-dir",
        str(root / "data-replicas"),
        "--replica-catalog-path",
        str(root / "data-catalog.txt"),
    ]
    if auth_enabled:
        command.extend(["--auth", "true"])
    return command


class StatefulAntflyServer:
    def __init__(self, binary: str, host: str, port: int, *, auth_enabled: bool = False):
        self.binary = binary
        self.host = host
        self.port = port
        self.auth_enabled = auth_enabled
        self.url = f"http://{host}:{port}"
        self.api_url = antfly_public_api_url(self.url, binary=binary)
        self.metadata_admin_url: str | None = None
        self.metadata_proc: subprocess.Popen[str] | None = None
        self.data_proc: subprocess.Popen[str] | None = None
        self.tempdir = tempfile.TemporaryDirectory(prefix="antfly-zig-stateful-e2e-")
        self.root = Path(self.tempdir.name)
        self.replica_root = self.root / "data-replicas"
        self.metadata_log_path = self.root / "metadata.log"
        self.data_log_path = self.root / "data.log"
        self.metadata_log_file = self.metadata_log_path.open("w")
        self.data_log_file = self.data_log_path.open("w")
        metadata_port = find_free_port()
        metadata_admin_port = find_free_port()
        self.metadata_port = metadata_port
        self.metadata_admin_port = metadata_admin_port
        self.data_raft_port = find_free_port()
        metadata_admin_url = f"http://{host}:{metadata_admin_port}"
        self.metadata_admin_url = metadata_admin_url

        self._start_processes(truncate_logs=False)

    def _start_processes(self, *, truncate_logs: bool) -> None:
        if truncate_logs:
            self.metadata_log_file = self.metadata_log_path.open("w")
            self.data_log_file = self.data_log_path.open("w")
        metadata_command = _metadata_command(
            self.binary,
            host=self.host,
            raft_port=self.metadata_port,
            admin_port=self.metadata_admin_port,
            root=self.root,
        )
        self.metadata_proc = subprocess.Popen(
            metadata_command,
            stdout=self.metadata_log_file,
            stderr=subprocess.STDOUT,
            cwd=self.root,
        )
        if not wait_for_server(self.metadata_admin_url, path="/metadata/v1/status"):
            self.metadata_log_file.flush()
            metadata_out = _read_log_tail(self.metadata_log_path)
            self.stop()
            raise RuntimeError(f"Metadata server failed to start at {self.metadata_admin_url}\n{metadata_out}")

        data_command = _data_command(
            self.binary,
            host=self.host,
            port=self.port,
            raft_port=self.data_raft_port,
            metadata_admin_base_uri=self.metadata_admin_url,
            root=self.root,
            auth_enabled=self.auth_enabled,
        )
        self.data_proc = subprocess.Popen(
            data_command,
            stdout=self.data_log_file,
            stderr=subprocess.STDOUT,
            cwd=self.root,
        )
        if not wait_for_server(self.api_url, allow_unauthorized=self.auth_enabled):
            self.metadata_log_file.flush()
            self.data_log_file.flush()
            metadata_out = _read_log_tail(self.metadata_log_path)
            data_out = _read_log_tail(self.data_log_path)
            self.stop()
            raise RuntimeError(
                f"Stateful API server failed to start at {self.api_url}\n"
                f"[metadata]\n{metadata_out}\n"
                f"[data]\n{data_out}"
            )

    def debug_logs(self) -> str:
        self.metadata_log_file.flush()
        self.data_log_file.flush()
        metadata_out = _read_log_tail(self.metadata_log_path)
        data_out = _read_log_tail(self.data_log_path)
        return f"[metadata]\n{metadata_out}\n[data]\n{data_out}"

    def _stop_processes(self) -> None:
        if self.data_proc is not None and self.data_proc.poll() is None:
            self.data_proc.send_signal(signal.SIGTERM)
            try:
                self.data_proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.data_proc.kill()
                self.data_proc.wait()
        if self.metadata_proc is not None and self.metadata_proc.poll() is None:
            self.metadata_proc.send_signal(signal.SIGTERM)
            try:
                self.metadata_proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.metadata_proc.kill()
                self.metadata_proc.wait()
        self.data_proc = None
        self.metadata_proc = None

    def restart(self) -> None:
        self._stop_processes()
        self.data_log_file.close()
        self.metadata_log_file.close()
        self.metadata_log_file = self.metadata_log_path.open("a")
        self.data_log_file = self.data_log_path.open("a")
        self._start_processes(truncate_logs=False)

    def pause(self) -> None:
        self._stop_processes()

    def resume(self) -> None:
        self._start_processes(truncate_logs=False)

    def stop(self) -> None:
        self._stop_processes()
        self.data_log_file.close()
        self.metadata_log_file.close()
        if not maybe_preserve_tempdir(self.tempdir):
            self.tempdir.cleanup()


class SwarmAntflyServer:
    def __init__(self, binary: str, host: str, port: int):
        self.binary = binary
        self.host = host
        self.port = port
        self.url = f"http://{host}:{port}"
        self.api_url = antfly_public_api_url(self.url, binary=binary)
        self.tempdir = tempfile.TemporaryDirectory(prefix="antfly-zig-swarm-e2e-")
        self.root = Path(self.tempdir.name)
        self.replica_root = self.root / "replicas"
        self.log_path = self.root / "server.log"
        self.log_file = self.log_path.open("w")
        self.proc: subprocess.Popen[str] | None = None
        self._start_process(truncate_logs=False)

    def _start_process(self, *, truncate_logs: bool) -> None:
        if truncate_logs:
            self.log_file = self.log_path.open("w")
        command = _swarm_stateful_command(self.binary, host=self.host, port=self.port, root=self.root)
        self.proc = subprocess.Popen(command, stdout=self.log_file, stderr=subprocess.STDOUT, cwd=self.root)
        if not wait_for_server(self.api_url):
            self.stop()
            out = _read_log_tail(self.log_path)
            raise RuntimeError(f"Swarm API server failed to start at {self.api_url}\n{out}")
        self.metadata_admin_url = self._poll_metadata_admin_url()

    def _poll_metadata_admin_url(self) -> str:
        """Poll log file for the metadata admin URL printed after server startup."""
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline:
            logs = _read_log_tail(self.log_path)
            matches = re.findall(r"(?:swarm )?metadata admin api listening on (http://[^\s]+)", logs)
            if matches:
                return matches[-1].rstrip("/")
            time.sleep(0.1)
        return ""

    def debug_logs(self) -> str:
        self.log_file.flush()
        return _read_log_tail(self.log_path)

    def _stop_process(self) -> None:
        if self.proc and self.proc.poll() is None:
            self.proc.send_signal(signal.SIGTERM)
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()
        self.proc = None

    def restart(self) -> None:
        self._stop_process()
        self.log_file.close()
        self.log_file = self.log_path.open("a")
        self._start_process(truncate_logs=False)

    def pause(self) -> None:
        self._stop_process()

    def resume(self) -> None:
        self._start_process(truncate_logs=False)

    def stop(self) -> None:
        self._stop_process()
        self.log_file.close()
        if not maybe_preserve_tempdir(self.tempdir):
            self.tempdir.cleanup()


class TermiteRerankerServer:
    def __init__(self, host: str = "127.0.0.1"):
        port = find_free_port()
        self.url = f"http://{host}:{port}"

        outer = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:  # noqa: N802
                if self.path != "/rerank":
                    self.send_error(404)
                    return

                content_length = int(self.headers.get("Content-Length", "0"))
                raw = self.rfile.read(content_length)
                payload = json.loads(raw.decode("utf-8") or "{}")
                prompts = payload.get("prompts", [])
                scores: list[float] = []
                for prompt in prompts:
                    text = str(prompt).lower()
                    if "beta" in text:
                        scores.append(0.95)
                    elif "alpha" in text or "search engine architecture" in text:
                        scores.append(0.6)
                    else:
                        scores.append(0.1)

                body = json.dumps({"scores": scores}).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, format: str, *args: object) -> None:
                _ = outer
                _ = format
                _ = args

        self._server = ThreadingHTTPServer((host, port), Handler)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()
        if not wait_for_listener(self.url):
            raise RuntimeError(f"Termite reranker server failed to start at {self.url}")

    def stop(self) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=5)


class TermiteGeneratorServer:
    def __init__(self, host: str = "127.0.0.1"):
        port = find_free_port()
        self.url = f"http://{host}:{port}"

        outer = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:  # noqa: N802
                if self.path != "/generate":
                    self.send_error(404)
                    return

                content_length = int(self.headers.get("Content-Length", "0"))
                raw = self.rfile.read(content_length)
                payload = json.loads(raw.decode("utf-8") or "{}")
                messages = payload.get("messages", [])
                prompt = ""
                if messages:
                    prompt = str(messages[-1].get("content", ""))

                if "tree_result(" in prompt and "doc:child" in prompt:
                    if (
                        "Selected tree branches:" in prompt
                        and "doc:root" in prompt
                        and "doc:child" in prompt
                        and
                        "Tree hierarchy context:" in prompt
                        and "Tree roots=1, tree_hits=1" in prompt
                        and "Root doc:root" in prompt
                    ):
                        content = "Generated tree answer citing doc:child from root doc:root along path doc:root > doc:child"
                    else:
                        content = "Generated tree answer citing doc:child from parent doc:root"
                elif "doc:a" in prompt or "hello retrieval" in prompt:
                    content = "Generated answer citing doc:a"
                else:
                    content = "Generated answer"

                body = json.dumps({"choices": [{"message": {"content": content}}]}).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, format: str, *args: object) -> None:
                _ = outer
                _ = format
                _ = args

        self._server = ThreadingHTTPServer((host, port), Handler)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()
        if not wait_for_listener(self.url):
            raise RuntimeError(f"Termite generator server failed to start at {self.url}")

    def stop(self) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=5)


class OpenAiEmbeddingServer:
    def __init__(self, host: str = "127.0.0.1"):
        port = find_free_port()
        self.url = f"http://{host}:{port}"

        outer = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:  # noqa: N802
                if self.path != "/v1/embeddings":
                    self.send_error(404)
                    return

                content_length = int(self.headers.get("Content-Length", "0"))
                raw = self.rfile.read(content_length)
                payload = json.loads(raw.decode("utf-8") or "{}")
                inputs = payload.get("input", [])
                if isinstance(inputs, str):
                    inputs = [inputs]

                model = payload.get("model", "text-embedding-3-small")
                data = []
                for i, text in enumerate(inputs):
                    vector = outer._vector_for_text(str(text))
                    data.append(
                        {
                            "object": "embedding",
                            "index": i,
                            "embedding": vector,
                        }
                    )

                body = json.dumps(
                    {
                        "object": "list",
                        "data": data,
                        "model": model,
                        "usage": {
                            "prompt_tokens": max(1, len(inputs)),
                            "total_tokens": max(1, len(inputs)),
                        },
                    }
                ).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, format: str, *args: object) -> None:
                _ = outer
                _ = format
                _ = args

        self._server = ThreadingHTTPServer((host, port), Handler)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()
        if not wait_for_listener(self.url):
            raise RuntimeError(f"OpenAI embedding server failed to start at {self.url}")

    @staticmethod
    def _vector_for_text(text: str) -> list[float]:
        lowered = text.lower()
        if "alpha" in lowered or "concept" in lowered:
            return [1.0, 0.0, 0.0]
        if "beta" in lowered:
            return [0.0, 1.0, 0.0]
        if "retrieval" in lowered or "semantic" in lowered:
            return [0.8, 0.2, 0.0]
        return [0.0, 0.0, 1.0]

    def stop(self) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=5)


class RateLimitedOpenAiEmbeddingServer:
    def __init__(self, host: str = "127.0.0.1", *, allowed_successes: int = 1):
        port = find_free_port()
        self.url = f"http://{host}:{port}"
        self._allowed_successes = allowed_successes
        self._successful_requests = 0
        self.total_requests = 0
        self.rate_limited_requests = 0
        self._lock = threading.Lock()

        outer = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:  # noqa: N802
                if self.path != "/v1/embeddings":
                    self.send_error(404)
                    return

                content_length = int(self.headers.get("Content-Length", "0"))
                raw = self.rfile.read(content_length)
                payload = json.loads(raw.decode("utf-8") or "{}")
                inputs = payload.get("input", [])
                if isinstance(inputs, str):
                    inputs = [inputs]

                model = payload.get("model", "text-embedding-3-small")
                with outer._lock:
                    outer.total_requests += 1
                    allow_success = outer._successful_requests < outer._allowed_successes
                    if allow_success:
                        outer._successful_requests += 1
                    else:
                        outer.rate_limited_requests += 1

                if not allow_success:
                    body = json.dumps(
                        {
                            "error": {
                                "message": "rate limit exceeded in test fixture",
                                "type": "rate_limit_exceeded",
                            }
                        }
                    ).encode("utf-8")
                    self.send_response(429)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                    return

                data = []
                for i, text in enumerate(inputs):
                    vector = OpenAiEmbeddingServer._vector_for_text(str(text))
                    data.append(
                        {
                            "object": "embedding",
                            "index": i,
                            "embedding": vector,
                        }
                    )

                body = json.dumps(
                    {
                        "object": "list",
                        "data": data,
                        "model": model,
                        "usage": {
                            "prompt_tokens": max(1, len(inputs)),
                            "total_tokens": max(1, len(inputs)),
                        },
                    }
                ).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, format: str, *args: object) -> None:
                _ = outer
                _ = format
                _ = args

        self._server = ThreadingHTTPServer((host, port), Handler)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()
        if not wait_for_listener(self.url):
            raise RuntimeError(f"Rate-limited OpenAI embedding server failed to start at {self.url}")

    def allow_all_requests(self) -> None:
        with self._lock:
            self._allowed_successes = 2**31 - 1

    def stats(self) -> dict[str, int]:
        with self._lock:
            return {
                "total_requests": self.total_requests,
                "rate_limited_requests": self.rate_limited_requests,
                "successful_requests": self._successful_requests,
            }

    def stop(self) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=5)


class PacingSensitiveOpenAiEmbeddingServer:
    def __init__(self, host: str = "127.0.0.1", *, min_interval_s: float = 0.01):
        port = find_free_port()
        self.url = f"http://{host}:{port}"
        self.min_interval_s = min_interval_s
        self.total_requests = 0
        self.rate_limited_requests = 0
        self.successful_requests = 0
        self._last_success_at = 0.0
        self._lock = threading.Lock()

        outer = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:  # noqa: N802
                if self.path != "/v1/embeddings":
                    self.send_error(404)
                    return

                content_length = int(self.headers.get("Content-Length", "0"))
                raw = self.rfile.read(content_length)
                payload = json.loads(raw.decode("utf-8") or "{}")
                inputs = payload.get("input", [])
                if isinstance(inputs, str):
                    inputs = [inputs]

                model = payload.get("model", "text-embedding-3-small")
                now = time.monotonic()
                with outer._lock:
                    outer.total_requests += 1
                    allowed = outer._last_success_at == 0.0 or (now - outer._last_success_at) >= outer.min_interval_s
                    if allowed:
                        outer._last_success_at = now
                        outer.successful_requests += 1
                    else:
                        outer.rate_limited_requests += 1

                if not allowed:
                    body = json.dumps(
                        {
                            "error": {
                                "message": "request arrived before pacing interval elapsed",
                                "type": "rate_limit_exceeded",
                            }
                        }
                    ).encode("utf-8")
                    self.send_response(429)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                    return

                data = []
                for i, text in enumerate(inputs):
                    vector = OpenAiEmbeddingServer._vector_for_text(str(text))
                    data.append(
                        {
                            "object": "embedding",
                            "index": i,
                            "embedding": vector,
                        }
                    )

                body = json.dumps(
                    {
                        "object": "list",
                        "data": data,
                        "model": model,
                        "usage": {
                            "prompt_tokens": max(1, len(inputs)),
                            "total_tokens": max(1, len(inputs)),
                        },
                    }
                ).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, format: str, *args: object) -> None:
                _ = outer
                _ = format
                _ = args

        self._server = ThreadingHTTPServer((host, port), Handler)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()
        if not wait_for_listener(self.url):
            raise RuntimeError(f"Pacing-sensitive OpenAI embedding server failed to start at {self.url}")

    def stats(self) -> dict[str, int]:
        with self._lock:
            return {
                "total_requests": self.total_requests,
                "rate_limited_requests": self.rate_limited_requests,
                "successful_requests": self.successful_requests,
            }

    def stop(self) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=5)


class TermiteEmbeddingServer:
    def __init__(self, host: str = "127.0.0.1"):
        port = find_free_port()
        self.base_url = f"http://{host}:{port}"
        self.url = termite_public_api_url(self.base_url)

        outer = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:  # noqa: N802
                content_length = int(self.headers.get("Content-Length", "0"))
                raw = self.rfile.read(content_length)
                payload = json.loads(raw.decode("utf-8") or "{}")

                if self.path in ("/chunk", "/api/chunk", f"{TERMITE_PUBLIC_API_ROOT}/chunk"):
                    model = payload.get("config", {}).get("model")
                    if model != "termite-chunker-v1":
                        self.send_error(400)
                        return
                    input_value = payload.get("input", "")
                    text = str(input_value)
                    if "beta body" in text:
                        chunks = [
                            {
                                "id": 0,
                                "mime_type": "text/plain",
                                "text": "beta body",
                                "start_char": 0,
                                "end_char": 9,
                            },
                            {
                                "id": 1,
                                "mime_type": "text/plain",
                                "text": "chunk tail",
                                "start_char": 10,
                                "end_char": 20,
                            },
                        ]
                    else:
                        chunks = [
                            {
                                "id": 0,
                                "mime_type": "text/plain",
                                "text": "alpha body",
                                "start_char": 0,
                                "end_char": 10,
                            },
                            {
                                "id": 1,
                                "mime_type": "text/plain",
                                "text": "chunk tail",
                                "start_char": 11,
                                "end_char": 21,
                            },
                        ]

                    body = json.dumps({"object": "list", "data": chunks}).encode("utf-8")
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                    return

                if self.path in (
                    "/embed",
                    "/embeddings",
                    "/api/embed",
                    f"{TERMITE_PUBLIC_API_ROOT}/embed",
                    f"{TERMITE_PUBLIC_API_ROOT}/embeddings",
                ):
                    model = payload.get("model", "")
                    input_value = payload.get("input", [])
                    if isinstance(input_value, list):
                        values = [outer._vector_for_text(json.dumps(item)) for item in input_value]
                    else:
                        values = [outer._vector_for_text(str(input_value))]

                    if model == "termite-sparse-v1":
                        data = [
                            {
                                "object": "embedding",
                                "index": i,
                                "embedding": (
                                    {"indices": [7, 42], "values": [1.5, 0.5]}
                                    if vector[0] > vector[1]
                                    else {"indices": [7, 42], "values": [0.25, 1.0]}
                                ),
                            }
                            for i, vector in enumerate(values)
                        ]
                    else:
                        data = [
                            {
                                "object": "embedding",
                                "index": i,
                                "embedding": vector,
                            }
                            for i, vector in enumerate(values)
                        ]
                    body = json.dumps(
                        {
                            "object": "list",
                            "data": data,
                            "model": model or "termite-embed-v1",
                            "usage": {
                                "prompt_tokens": max(1, len(values)),
                                "total_tokens": max(1, len(values)),
                            },
                        }
                    ).encode("utf-8")

                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                    return

                self.send_error(404)

            def log_message(self, format: str, *args: object) -> None:
                _ = outer
                _ = format
                _ = args

        self._server = ThreadingHTTPServer((host, port), Handler)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()
        if not wait_for_listener(self.url):
            raise RuntimeError(f"Termite embedding server failed to start at {self.url}")

    @staticmethod
    def _vector_for_text(text: str) -> list[float]:
        lowered = text.lower()
        if "alpha concept" in lowered or "alpha body" in lowered:
            return [1.0, 0.0, 0.0]
        if "beta body" in lowered:
            return [0.0, 1.0, 0.0]
        if '"mime_type": "image/png"' in lowered or '"type": "media"' in lowered:
            return [1.0, 0.0, 0.0]
        return [0.0, 0.0, 1.0]

    def stop(self) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=5)


def _models_dir() -> Path:
    home = os.environ.get("HOME")
    return Path(home).expanduser() / ".termite" / "models" if home else Path("./models")


def _clipclap_model_dir() -> Path:
    return _models_dir() / CLIPCLAP_MODEL


def _clipclap_gguf_available(model_dir: Path) -> bool:
    return model_dir.exists() and all((model_dir / name).exists() for name in CLIPCLAP_GGUF_FILES)


def _env_truthy(name: str) -> bool:
    return os.environ.get(name, "").lower() in {"1", "true", "yes", "on"}


@pytest.fixture(scope="session")
def serverless_runtime():
    url = os.environ.get("ANTFLY_SERVERLESS_URL")
    if url:
        url = url.rstrip("/")
        if not wait_for_server(url, timeout=10):
            pytest.skip(f"Server at {url} is not reachable")
        yield url, None
        return

    binary = resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN)))
    if not Path(binary).exists():
        pytest.skip("Set ANTFLY_SERVERLESS_URL or ANTFLY_BIN to run E2E tests")

    port = find_free_port()
    server = AntflyServer(binary, "127.0.0.1", port)
    yield server.url, server
    server.stop()


@pytest.fixture(scope="session")
def serverless_api(serverless_runtime):
    session = requests.Session()
    session.headers["Content-Type"] = "application/json"
    session.headers["Connection"] = "close"
    base_url, server = serverless_runtime

    class Api:
        def __init__(self, session: requests.Session, base_url: str, server_ref: AntflyServer | None):
            self.s = session
            self.url = base_url.rstrip("/")
            self._server = server_ref

        def _check(self, response: requests.Response) -> dict:
            if response.status_code >= 400:
                body = response.text.strip()
                logs = ""
                if self._server is not None:
                    logs = self._server.debug_logs().strip()
                message = f"{response.status_code} {response.reason} for {response.request.method} {response.url}"
                if body:
                    message += f"\n[body]\n{body}"
                if logs:
                    message += f"\n[logs]\n{logs}"
                raise requests.HTTPError(message, response=response)
            return response.json()

        def get(self, path: str) -> dict:
            return self._check(self.s.get(f"{self.url}{path}", timeout=10))

        def post(self, path: str, payload: dict) -> dict:
            return self._check(self.s.post(f"{self.url}{path}", json=payload, timeout=10))

        def put(self, path: str, payload: dict) -> dict:
            return self._check(self.s.put(f"{self.url}{path}", json=payload, timeout=10))

        def status(self) -> dict:
            return self.get("/status")

        def ensure_table(self, table_name: str, created_at_ns: int = 100) -> dict:
            return self.put(f"/tables/{table_name}", {"created_at_ns": created_at_ns})

        def update_table(self, table_name: str, payload: dict) -> dict:
            return self.put(f"/tables/{table_name}", payload)

        def ingest_table(self, table_name: str, timestamp_ns: int, mutations: list[dict]) -> dict:
            return self.put(
                f"/tables/{table_name}/ingest-batch",
                {"timestamp_ns": timestamp_ns, "mutations": mutations},
            )

        def list_indexes(self, table_name: str) -> dict:
            return self.get(f"/tables/{table_name}/indexes")

        def get_index(self, table_name: str, index_name: str) -> dict:
            return self.get(f"/tables/{table_name}/indexes/{index_name}")

        def create_index(self, table_name: str, index_name: str, payload: dict) -> dict:
            created = self.post(f"/tables/{table_name}/indexes/{index_name}", payload)
            deadline = time.monotonic() + 10.0
            while True:
                try:
                    current = self.get(f"/tables/{table_name}/indexes/{index_name}")
                    config = current.get("config", {})
                    if config.get("name") == index_name:
                        return created
                except requests.RequestException:
                    pass
                if time.monotonic() >= deadline:
                    return created
                time.sleep(0.1)

        def wait_index_ready(self, table_name: str, index_name: str, *, timeout_s: float = 30.0, interval_s: float = 0.5) -> dict | None:
            deadline = time.monotonic() + timeout_s
            while True:
                try:
                    ready = ready_index_status(self.get(f"/tables/{table_name}/indexes/{index_name}"))
                    if ready is not None:
                        return ready
                except requests.RequestException:
                    pass
                if time.monotonic() >= deadline:
                    return None
                time.sleep(interval_s)

        def delete_index(self, table_name: str, index_name: str) -> dict:
            return self._check(self.s.delete(f"{self.url}/tables/{table_name}/indexes/{index_name}", timeout=10))

        def build_table(self, table_name: str, *, timeout_s: float = 10.0, interval_s: float = 0.1) -> dict:
            deadline = time.monotonic() + timeout_s
            while True:
                try:
                    return self.post(antfly_internal_api_path(f"/tables/{table_name}/build"), {})
                except requests.HTTPError as exc:
                    if exc.response is None or exc.response.status_code != 409 or time.monotonic() >= deadline:
                        raise
                    time.sleep(interval_s)

        def table_build_status(self, table_name: str) -> dict:
            return self.get(antfly_internal_api_path(f"/tables/{table_name}/build-status"))

        def batch_table(
            self,
            table_name: str,
            *,
            inserts: dict[str, dict] | None = None,
            deletes: list[str] | None = None,
            transforms: list[dict[str, object]] | None = None,
            sync_level: str | None = None,
        ) -> dict:
            payload: dict[str, object] = {}
            if inserts:
                payload["inserts"] = inserts
            if deletes:
                payload["deletes"] = deletes
            if transforms:
                payload["transforms"] = transforms
            if sync_level is not None:
                payload["sync_level"] = sync_level
            timeout = 60 if sync_level in {"full_text", "enrichments", "aknn", "full_index"} else 10
            return self._check(self.s.post(f"{self.url}/tables/{table_name}/batch", json=payload, timeout=timeout))

        def query_published(self, table_name: str) -> dict:
            return self.get(f"/tables/{table_name}/query/published")

        def query_latest(self, table_name: str) -> dict:
            return self.get(f"/tables/{table_name}/query/latest")

        def query_table(self, table_name: str, payload: dict) -> dict:
            return self.post(f"/tables/{table_name}/query", payload)

        def search_table(self, table_name: str, payload: dict) -> dict:
            return self.post(f"/tables/{table_name}/query/search", payload)

        def graph_neighbors(self, table_name: str, payload: dict) -> dict:
            return self.post(f"/tables/{table_name}/query/graph/neighbors", payload)

        def graph_traverse(self, table_name: str, payload: dict) -> dict:
            return self.post(f"/tables/{table_name}/query/graph/traverse", payload)

        def graph_shortest_path(self, table_name: str, payload: dict) -> dict:
            return self.post(f"/tables/{table_name}/query/graph/shortest-path", payload)

        def query_head_artifact(self, namespace: str, artifact_index: int) -> dict:
            return self.get(antfly_internal_api_path(f"/namespaces/{namespace}/query/head/artifacts/{artifact_index}"))

    yield Api(session, base_url, server)
    session.close()


@pytest.fixture(scope="function")
def termite_reranker():
    server = TermiteRerankerServer()
    yield server.url
    server.stop()


@pytest.fixture(scope="function")
def termite_generator():
    server = TermiteGeneratorServer()
    yield server.url
    server.stop()


@pytest.fixture(scope="function")
def openai_embedder():
    server = OpenAiEmbeddingServer()
    yield server.url
    server.stop()


@pytest.fixture(scope="function")
def rate_limited_openai_embedder():
    server = RateLimitedOpenAiEmbeddingServer()
    yield server
    server.stop()


@pytest.fixture(scope="function")
def single_item_enrichment_batches(monkeypatch):
    monkeypatch.setenv("ANTFLY_ENRICHMENT_EMBED_BATCH_ITEMS", "1")


@pytest.fixture(scope="function")
def pacing_sensitive_openai_embedder():
    server = PacingSensitiveOpenAiEmbeddingServer()
    yield server
    server.stop()


@pytest.fixture(scope="function")
def strict_pacing_sensitive_openai_embedder():
    server = PacingSensitiveOpenAiEmbeddingServer(min_interval_s=0.2)
    yield server
    server.stop()


@pytest.fixture(scope="function")
def termite_embedder():
    server = TermiteEmbeddingServer()
    yield server.url
    server.stop()


@pytest.fixture(scope="session")
def clipclap_model_available():
    model_dir = _clipclap_model_dir()
    if _clipclap_gguf_available(model_dir):
        return model_dir
    if not _env_truthy(ALLOW_REAL_MODEL_DOWNLOAD_ENV):
        pytest.skip(
            f"ClipClap GGUF files are not present at {model_dir}; set {ALLOW_REAL_MODEL_DOWNLOAD_ENV}=1 to download them"
        )

    binary = resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN)))
    if not Path(binary).exists():
        pytest.skip(f"Antfly binary not found for model download: {binary}")

    subprocess.run(
        [binary, "termite", "pull", "hf:antflydb/clipclap:gguf:Q4_K", "--tasks", "embed"],
        cwd=REPO_ROOT,
        check=True,
    )
    if not model_dir.exists():
        raise RuntimeError(f"termite pull finished but did not create {model_dir}")
    if not _clipclap_gguf_available(model_dir):
        raise RuntimeError(f"ClipClap GGUF files are missing from {model_dir}")
    return model_dir


@pytest.fixture(scope="function")
def real_clipclap_backup_api(request, clipclap_model_available):
    _ = clipclap_model_available
    return request.getfixturevalue("backup_api")


@pytest.fixture(scope="function")
def stateful_api():
    base_url = os.environ.get("ANTFLY_STATEFUL_URL")
    server: PublicAntflyServer | SwarmAntflyServer | StatefulAntflyServer | None = None
    default_root = os.environ.get("ANTFLY_STATEFUL_API_ROOT")
    if not base_url:
        binary = resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN)))
        if not Path(binary).exists():
            pytest.skip(f"Public API binary not found: {binary}")
        port = find_free_port()
        server = _start_stateful_server_with_retry(binary, port)
        base_url = server.url
        if default_root is None and Path(binary).name == "antfly":
            default_root = ANTFLY_PUBLIC_API_ROOT

    base = antfly_public_api_url(base_url, root=default_root if default_root is not None else "")

    if not wait_for_server(base, timeout=10):
        pytest.skip(f"Public API at {base} is not reachable")

    session = requests.Session()
    session.headers["Content-Type"] = "application/json"
    session.headers["Connection"] = "close"

    class PublicApi:
        def __init__(
            self,
            session: requests.Session,
            base_url: str,
            server_ref: PublicAntflyServer | SwarmAntflyServer | StatefulAntflyServer | None,
        ):
            self.s = session
            self.url = base_url.rstrip("/")
            self._server = server_ref
            self._request_lock = threading.Lock()

        def _raise_request_error(self, err: requests.RequestException) -> None:
            raise_request_error_with_logs(err, self._server)

        def _check(self, response: requests.Response) -> Any:
            if response.status_code >= 400:
                body = response.text.strip()
                logs = ""
                if self._server is not None:
                    logs = self._server.debug_logs().strip()
                if body:
                    if logs:
                        raise requests.HTTPError(
                            f"{response.status_code} {response.reason} for url: {response.url} body={body}\nserver logs:\n{logs}",
                            response=response,
                        )
                    raise requests.HTTPError(
                        f"{response.status_code} {response.reason} for url: {response.url} body={body}",
                        response=response,
                    )
                if logs:
                    raise requests.HTTPError(
                        f"{response.status_code} {response.reason} for url: {response.url}\nserver logs:\n{logs}",
                        response=response,
                    )
                response.raise_for_status()
            if not response.content:
                return {}
            return response.json()

        def _decode(self, response: requests.Response) -> Any:
            if not response.content:
                return {}
            return response.json()

        def restart_server(self) -> None:
            server = self._server
            if server is None or not hasattr(server, "restart"):
                raise AssertionError("restart is only available for locally managed stateful servers")
            with self._request_lock:
                self.s.close()
                server.restart()
                if not wait_for_server(self.url, timeout=20):
                    logs = server.debug_logs().strip()
                    raise AssertionError(f"stateful server failed to restart at {self.url}\n{logs}")
                new_session = requests.Session()
                new_session.headers["Content-Type"] = "application/json"
                new_session.headers["Connection"] = "close"
                self.s = new_session

        def corrupt_embedding_artifact(self, table_name: str, doc_key: str, index_name: str) -> None:
            server = self._server
            if server is None:
                raise AssertionError("artifact corruption is only available for locally managed stateful servers")
            internal_url = f"{server.url}{antfly_internal_api_path(f'/tables/{table_name}/corrupt-embedding-artifact')}"
            try:
                with self._request_lock:
                    self._check(
                        self.s.post(
                            internal_url,
                            json={
                                "doc_key": doc_key,
                                "index_name": index_name,
                            },
                            timeout=30,
                        )
                    )
            except requests.RequestException as err:
                self._raise_request_error(err)
            self.restart_server()

        def get(self, path: str) -> Any:
            try:
                with self._request_lock:
                    return self._check(self.s.get(f"{self.url}{path}", timeout=30))
            except requests.RequestException as err:
                self._raise_request_error(err)

        def post(self, path: str, payload: dict) -> Any:
            try:
                with self._request_lock:
                    return self._check(self.s.post(f"{self.url}{path}", json=payload, timeout=30))
            except requests.RequestException as err:
                self._raise_request_error(err)

        def put(self, path: str, payload: dict) -> Any:
            try:
                with self._request_lock:
                    return self._check(self.s.put(f"{self.url}{path}", json=payload, timeout=30))
            except requests.RequestException as err:
                self._raise_request_error(err)

        def _request(self, method: str, path: str, payload: dict | None = None) -> requests.Response:
            try:
                with self._request_lock:
                    return self.s.request(method, f"{self.url}{path}", json=payload, timeout=30)
            except requests.RequestException as err:
                self._raise_request_error(err)

        def delete(self, path: str) -> Any:
            try:
                with self._request_lock:
                    return self._check(self.s.delete(f"{self.url}{path}", timeout=30))
            except requests.RequestException as err:
                self._raise_request_error(err)

        def create_table(self, table_name: str, *, num_shards: int = 1, description: str | None = None) -> dict:
            payload: dict[str, object] = {"num_shards": num_shards}
            if description is not None:
                payload["description"] = description
            deadline = time.monotonic() + 5.0
            while True:
                try:
                    with self._request_lock:
                        response = self.s.post(f"{self.url}/tables/{table_name}", json=payload, timeout=30)
                except requests.RequestException as err:
                    if time.monotonic() >= deadline:
                        self._raise_request_error(err)
                    time.sleep(0.1)
                    continue
                if response.status_code not in (404, 500):
                    return self._check(response)
                if time.monotonic() >= deadline:
                    return self._check(response)
                time.sleep(0.1)

        def get_table(self, table_name: str) -> dict:
            return self.get(f"/tables/{table_name}")

        def delete_table(self, table_name: str) -> dict:
            return self.delete(f"/tables/{table_name}")

        def update_schema(self, table_name: str, schema: dict) -> dict:
            return self.put(f"/tables/{table_name}/schema", schema)

        def query_table(self, table_name: str, payload: dict) -> dict:
            return self.post(f"/tables/{table_name}/query", payload)

        def termite_embed(self, model: str, text: str, *, timeout_s: float = 120.0) -> dict:
            base_url = self.url.removesuffix(ANTFLY_PUBLIC_API_ROOT)
            try:
                with self._request_lock:
                    response = self.s.post(
                        f"{base_url}{TERMITE_PUBLIC_API_ROOT}/embed",
                        json={"model": model, "input": text},
                        timeout=timeout_s,
                    )
                return self._check(response)
            except requests.RequestException as err:
                self._raise_request_error(err)

        def batch_write(
            self,
            table_name: str,
            *,
            inserts: dict[str, dict] | None = None,
            deletes: list[str] | None = None,
            transforms: list[dict[str, object]] | None = None,
            sync_level: str | None = None,
        ) -> dict:
            payload: dict[str, object] = {}
            if inserts:
                payload["inserts"] = inserts
            if deletes:
                payload["deletes"] = deletes
            if transforms:
                payload["transforms"] = transforms
            if sync_level is not None:
                payload["sync_level"] = sync_level
            return self.post(f"/tables/{table_name}/batch", payload)

        def linear_merge(
            self,
            table_name: str,
            *,
            records: dict[str, object],
            last_merged_id: str = "",
            dry_run: bool = False,
            sync_level: str | None = None,
        ) -> dict:
            payload: dict[str, object] = {
                "records": records,
                "last_merged_id": last_merged_id,
                "dry_run": dry_run,
            }
            if sync_level is not None:
                payload["sync_level"] = sync_level
            return self.post(f"/tables/{table_name}/merge", payload)

        def backup_table(self, table_name: str, *, backup_id: str, location: str) -> dict:
            try:
                with self._request_lock:
                    response = self.s.post(
                        f"{self.url}/tables/{table_name}/backup",
                        json={
                            "backup_id": backup_id,
                            "location": location,
                        },
                        timeout=120,
                    )
                return self._check(response)
            except requests.RequestException as err:
                self._raise_request_error(err)

        def restore_table(self, table_name: str, *, backup_id: str, location: str) -> dict:
            try:
                with self._request_lock:
                    response = self.s.post(
                        f"{self.url}/tables/{table_name}/restore",
                        json={
                            "backup_id": backup_id,
                            "location": location,
                        },
                        timeout=120,
                    )
                return self._check(response)
            except requests.RequestException as err:
                self._raise_request_error(err)

        def cluster_backup(self, *, backup_id: str, location: str, table_names: list[str] | None = None) -> dict:
            payload: dict[str, object] = {
                "backup_id": backup_id,
                "location": location,
            }
            if table_names is not None:
                payload["table_names"] = table_names
            try:
                with self._request_lock:
                    return self._check(self.s.post(f"{self.url}/backup", json=payload, timeout=120))
            except requests.RequestException as err:
                self._raise_request_error(err)

        def cluster_restore(
            self,
            *,
            backup_id: str,
            location: str,
            table_names: list[str] | None = None,
            restore_mode: str | None = None,
        ) -> dict:
            payload: dict[str, object] = {
                "backup_id": backup_id,
                "location": location,
            }
            if table_names is not None:
                payload["table_names"] = table_names
            if restore_mode is not None:
                payload["restore_mode"] = restore_mode
            try:
                with self._request_lock:
                    return self._check(self.s.post(f"{self.url}/restore", json=payload, timeout=120))
            except requests.RequestException as err:
                self._raise_request_error(err)

        def list_backups(self, *, location: str) -> dict:
            response = self.s.get(f"{self.url}/backups?location={location}", timeout=30)
            return self._check(response)

        def batch_write_with_timeout(
            self,
            table_name: str,
            *,
            inserts: dict[str, dict] | None = None,
            deletes: list[str] | None = None,
            transforms: list[dict[str, object]] | None = None,
            sync_level: str | None = None,
            timeout_s: float,
        ) -> dict:
            payload: dict[str, object] = {}
            if inserts:
                payload["inserts"] = inserts
            if deletes:
                payload["deletes"] = deletes
            if transforms:
                payload["transforms"] = transforms
            if sync_level is not None:
                payload["sync_level"] = sync_level
            with self._request_lock:
                response = self.s.post(
                    f"{self.url}/tables/{table_name}/batch",
                    json=payload,
                    timeout=timeout_s,
                )
            return self._check(response)

        def multi_batch(self, tables: dict[str, dict], *, sync_level: str | None = None) -> dict:
            payload: dict[str, object] = {
                "read_set": [],
                "tables": tables,
            }
            if sync_level is not None:
                payload["sync_level"] = sync_level
            response = self._request("POST", "/transactions/commit", payload)
            if response.status_code != 200:
                return self._check(response)
            return self._decode(response)

        def lookup_key(self, table_name: str, key: str) -> dict:
            return self.get(lookup_key_path(table_name, key))

        def scan_keys(self, table_name: str, payload: dict) -> list[dict]:
            response = self._request("POST", f"/tables/{table_name}/lookup", payload)
            if response.status_code >= 400:
                return self._check(response)
            if not response.content:
                return []
            return [
                json.loads(line)
                for line in response.text.splitlines()
                if line.strip()
            ]

        def lookup_key_with_version(self, table_name: str, key: str) -> tuple[dict, str | None]:
            response = self._request("GET", lookup_key_path(table_name, key))
            body = self._check(response)
            return body, response.headers.get("X-Antfly-Version")

        def commit_transaction(
            self,
            *,
            read_set: list[dict[str, object]],
            tables: dict[str, dict],
            sync_level: str | None = None,
        ) -> tuple[int, dict]:
            payload: dict[str, object] = {
                "read_set": read_set,
                "tables": tables,
            }
            if sync_level is not None:
                payload["sync_level"] = sync_level
            response = self._request("POST", "/transactions/commit", payload)
            if response.status_code not in (200, 409):
                return response.status_code, self._check(response)
            return response.status_code, self._decode(response)

        def begin_transaction_session(self, *, sync_level: str | None = None) -> dict:
            payload: dict[str, object] = {}
            if sync_level is not None:
                payload["sync_level"] = sync_level
            return self.post("/transactions/begin", payload)

        def stage_transaction_session(
            self,
            transaction_id: str,
            *,
            read_set: list[dict[str, object]] | None = None,
            tables: dict[str, dict] | None = None,
            sync_level: str | None = None,
        ) -> dict:
            payload: dict[str, object] = {}
            if read_set:
                payload["read_set"] = read_set
            else:
                payload["read_set"] = []
            if tables:
                payload["tables"] = tables
            if sync_level is not None:
                payload["sync_level"] = sync_level
            return self.post(f"/transactions/{transaction_id}/stage", payload)

        def stage_transaction_read(
            self,
            transaction_id: str,
            *,
            table_name: str,
            key: str,
            version: str | int,
        ) -> tuple[int, dict]:
            payload = {
                "table": table_name,
                "key": key,
                "version": str(version),
            }
            response = self._request("POST", f"/transactions/{transaction_id}/read", payload)
            if response.status_code not in (200, 409):
                response.raise_for_status()
            return response.status_code, self._decode(response)

        def stage_transaction_write(
            self,
            transaction_id: str,
            *,
            table_name: str,
            key: str,
            document: dict[str, object],
        ) -> dict:
            payload = {
                "table": table_name,
                "key": key,
                "document": document,
            }
            return self.post(f"/transactions/{transaction_id}/write", payload)

        def stage_transaction_delete(self, transaction_id: str, *, table_name: str, key: str) -> dict:
            payload = {
                "table": table_name,
                "key": key,
            }
            return self.post(f"/transactions/{transaction_id}/delete", payload)

        def create_transaction_savepoint(self, transaction_id: str) -> dict:
            return self.post(f"/transactions/{transaction_id}/savepoints", {})

        def rollback_transaction_savepoint(self, transaction_id: str, savepoint_id: int) -> dict:
            return self.post(f"/transactions/{transaction_id}/savepoints/{savepoint_id}/rollback", {})

        def commit_transaction_session(
            self,
            transaction_id: str,
            *,
            read_set: list[dict[str, object]] | None = None,
            tables: dict[str, dict] | None = None,
            sync_level: str | None = None,
        ) -> tuple[int, dict]:
            payload: dict[str, object] = {}
            if read_set:
                payload["read_set"] = read_set
            if tables:
                payload["tables"] = tables
            if sync_level is not None:
                payload["sync_level"] = sync_level
            response = self._request("POST", f"/transactions/{transaction_id}/commit", payload or None)
            if response.status_code not in (200, 409):
                response.raise_for_status()
            return response.status_code, self._decode(response)

        def abort_transaction_session(self, transaction_id: str) -> dict:
            return self.post(f"/transactions/{transaction_id}/abort", {})

        def list_indexes(self, table_name: str) -> dict:
            return self.get(f"/tables/{table_name}/indexes")

        def get_index(self, table_name: str, index_name: str) -> dict:
            return self.get(f"/tables/{table_name}/indexes/{index_name}")

        def create_index(self, table_name: str, index_name: str, payload: dict) -> dict:
            created = self.post(f"/tables/{table_name}/indexes/{index_name}", payload)
            deadline = time.monotonic() + 10.0
            while True:
                try:
                    current = self.get(f"/tables/{table_name}/indexes/{index_name}")
                    config = current.get("config", {})
                    if config.get("name") == index_name:
                        return created
                except requests.RequestException:
                    pass
                if time.monotonic() >= deadline:
                    return created
                time.sleep(0.1)

        def delete_index(self, table_name: str, index_name: str) -> dict:
            return self.delete(f"/tables/{table_name}/indexes/{index_name}")

    yield PublicApi(session, base, server)
    session.close()
    if server is not None:
        server.stop()


@pytest.fixture(scope="function")
def backup_api():
    binary = resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN)))
    if not Path(binary).exists():
        pytest.skip(f"Public API binary not found: {binary}")

    port = find_free_port()
    if Path(binary).name == "antfly":
        server = SwarmAntflyServer(binary, "127.0.0.1", port)
    else:
        server = PublicAntflyServer(binary, "127.0.0.1", port)

    base = antfly_public_api_url(server.url, binary=binary)

    if not wait_for_server(base, timeout=10):
        server.stop()
        pytest.skip(f"Public API at {base} is not reachable")

    session = requests.Session()
    session.headers["Content-Type"] = "application/json"
    session.headers["Connection"] = "close"

    class PublicApi:
        def __init__(
            self,
            session: requests.Session,
            base_url: str,
            server_ref: PublicAntflyServer | SwarmAntflyServer | StatefulAntflyServer | None,
        ):
            self.s = session
            self.url = base_url.rstrip("/")
            self._server = server_ref
            self._request_lock = threading.Lock()

        def _raise_request_error(self, err: requests.RequestException) -> None:
            raise_request_error_with_logs(err, self._server)

        def _check(self, response: requests.Response) -> Any:
            if response.status_code >= 400:
                body = response.text.strip()
                logs = ""
                if self._server is not None:
                    logs = self._server.debug_logs().strip()
                if body:
                    if logs:
                        raise requests.HTTPError(
                            f"{response.status_code} {response.reason} for url: {response.url} body={body}\nserver logs:\n{logs}",
                            response=response,
                        )
                    raise requests.HTTPError(
                        f"{response.status_code} {response.reason} for url: {response.url} body={body}",
                        response=response,
                    )
                if logs:
                    raise requests.HTTPError(
                        f"{response.status_code} {response.reason} for url: {response.url}\nserver logs:\n{logs}",
                        response=response,
                    )
                response.raise_for_status()
            if not response.content:
                return {}
            return response.json()

        def get(self, path: str) -> Any:
            try:
                with self._request_lock:
                    return self._check(self.s.get(f"{self.url}{path}", timeout=30))
            except requests.RequestException as err:
                self._raise_request_error(err)

        def post(self, path: str, payload: dict) -> Any:
            try:
                with self._request_lock:
                    return self._check(self.s.post(f"{self.url}{path}", json=payload, timeout=30))
            except requests.RequestException as err:
                self._raise_request_error(err)

        def put(self, path: str, payload: dict) -> Any:
            try:
                with self._request_lock:
                    return self._check(self.s.put(f"{self.url}{path}", json=payload, timeout=30))
            except requests.RequestException as err:
                self._raise_request_error(err)

        def _request(self, method: str, path: str, payload: dict | None = None) -> requests.Response:
            try:
                with self._request_lock:
                    return self.s.request(method, f"{self.url}{path}", json=payload, timeout=30)
            except requests.RequestException as err:
                self._raise_request_error(err)

        def delete(self, path: str) -> Any:
            try:
                with self._request_lock:
                    return self._check(self.s.delete(f"{self.url}{path}", timeout=30))
            except requests.RequestException as err:
                self._raise_request_error(err)

        def create_table(self, table_name: str, *, num_shards: int = 1, description: str | None = None) -> dict:
            payload: dict[str, object] = {"num_shards": num_shards}
            if description is not None:
                payload["description"] = description
            deadline = time.monotonic() + 5.0
            while True:
                try:
                    with self._request_lock:
                        response = self.s.post(f"{self.url}/tables/{table_name}", json=payload, timeout=30)
                except requests.RequestException as err:
                    if time.monotonic() >= deadline:
                        self._raise_request_error(err)
                    time.sleep(0.1)
                    continue
                if response.status_code not in (404, 500):
                    return self._check(response)
                if time.monotonic() >= deadline:
                    return self._check(response)
                time.sleep(0.1)

        def get_table(self, table_name: str) -> dict:
            return self.get(f"/tables/{table_name}")

        def delete_table(self, table_name: str) -> dict:
            return self.delete(f"/tables/{table_name}")

        def update_schema(self, table_name: str, schema: dict) -> dict:
            return self.put(f"/tables/{table_name}/schema", schema)

        def query_table(self, table_name: str, payload: dict) -> dict:
            return self.post(f"/tables/{table_name}/query", payload)

        def termite_embed(self, model: str, text: str, *, timeout_s: float = 120.0) -> dict:
            base_url = self.url.removesuffix(ANTFLY_PUBLIC_API_ROOT)
            try:
                with self._request_lock:
                    response = self.s.post(
                        f"{base_url}{TERMITE_PUBLIC_API_ROOT}/embed",
                        json={"model": model, "input": text},
                        timeout=timeout_s,
                    )
                return self._check(response)
            except requests.RequestException as err:
                self._raise_request_error(err)

        def batch_write(
            self,
            table_name: str,
            *,
            inserts: dict[str, dict] | None = None,
            deletes: list[str] | None = None,
            transforms: list[dict[str, object]] | None = None,
            sync_level: str | None = None,
        ) -> dict:
            payload: dict[str, object] = {}
            if inserts:
                payload["inserts"] = inserts
            if deletes:
                payload["deletes"] = deletes
            if transforms:
                payload["transforms"] = transforms
            if sync_level is not None:
                payload["sync_level"] = sync_level
            return self.post(f"/tables/{table_name}/batch", payload)

        def lookup_key(self, table_name: str, key: str) -> dict:
            return self.get(lookup_key_path(table_name, key))

        def scan_keys(self, table_name: str, payload: dict) -> list[dict]:
            response = self._request("POST", f"/tables/{table_name}/lookup", payload)
            if response.status_code >= 400:
                return self._check(response)
            if not response.content:
                return []
            return [
                json.loads(line)
                for line in response.text.splitlines()
                if line.strip()
            ]

        def list_indexes(self, table_name: str) -> dict:
            return self.get(f"/tables/{table_name}/indexes")

        def get_index(self, table_name: str, index_name: str) -> dict:
            return self.get(f"/tables/{table_name}/indexes/{index_name}")

        def create_index(self, table_name: str, index_name: str, payload: dict) -> dict:
            created = self.post(f"/tables/{table_name}/indexes/{index_name}", payload)
            deadline = time.monotonic() + 10.0
            while True:
                try:
                    current = self.get(f"/tables/{table_name}/indexes/{index_name}")
                    config = current.get("config", {})
                    if config.get("name") == index_name:
                        return created
                except requests.RequestException:
                    pass
                if time.monotonic() >= deadline:
                    return created
                time.sleep(0.1)

        def wait_index_ready(self, table_name: str, index_name: str, *, timeout_s: float = 30.0, interval_s: float = 0.5) -> dict | None:
            deadline = time.monotonic() + timeout_s
            while True:
                try:
                    ready = ready_index_status(self.get(f"/tables/{table_name}/indexes/{index_name}"))
                    if ready is not None:
                        return ready
                except requests.RequestException:
                    pass
                if time.monotonic() >= deadline:
                    return None
                time.sleep(interval_s)

        def delete_index(self, table_name: str, index_name: str) -> dict:
            return self.delete(f"/tables/{table_name}/indexes/{index_name}")

        def backup_table(self, table_name: str, *, backup_id: str, location: str) -> dict:
            try:
                with self._request_lock:
                    response = self.s.post(
                        f"{self.url}/tables/{table_name}/backup",
                        json={
                            "backup_id": backup_id,
                            "location": location,
                        },
                        timeout=120,
                    )
                return self._check(response)
            except requests.RequestException as err:
                self._raise_request_error(err)

        def restore_table(self, table_name: str, *, backup_id: str, location: str) -> dict:
            try:
                with self._request_lock:
                    response = self.s.post(
                        f"{self.url}/tables/{table_name}/restore",
                        json={
                            "backup_id": backup_id,
                            "location": location,
                        },
                        timeout=120,
                    )
                return self._check(response)
            except requests.RequestException as err:
                self._raise_request_error(err)

        def cluster_backup(self, *, backup_id: str, location: str, table_names: list[str] | None = None) -> dict:
            payload: dict[str, object] = {
                "backup_id": backup_id,
                "location": location,
            }
            if table_names is not None:
                payload["table_names"] = table_names
            try:
                with self._request_lock:
                    return self._check(self.s.post(f"{self.url}/backup", json=payload, timeout=120))
            except requests.RequestException as err:
                self._raise_request_error(err)

        def cluster_restore(
            self,
            *,
            backup_id: str,
            location: str,
            table_names: list[str] | None = None,
            restore_mode: str | None = None,
        ) -> dict:
            payload: dict[str, object] = {
                "backup_id": backup_id,
                "location": location,
            }
            if table_names is not None:
                payload["table_names"] = table_names
            if restore_mode is not None:
                payload["restore_mode"] = restore_mode
            try:
                with self._request_lock:
                    return self._check(self.s.post(f"{self.url}/restore", json=payload, timeout=120))
            except requests.RequestException as err:
                self._raise_request_error(err)

        def list_backups(self, *, location: str) -> dict:
            response = self.s.get(f"{self.url}/backups?location={location}", timeout=30)
            return self._check(response)

    yield PublicApi(session, base, server)
    session.close()
    server.stop()


@pytest.fixture(scope="function", params=["stateful", "serverless"], ids=["stateful", "serverless"])
def table_api(request):
    backend = request.param
    raw = request.getfixturevalue(f"{backend}_api")

    class TableApi:
        def __init__(self, backend_name: str, raw_api):
            self.backend = backend_name
            self.raw = raw_api

        def create_table(self, table_name: str) -> dict:
            if self.backend == "stateful":
                return self.raw.create_table(table_name, num_shards=1)
            created = self.raw.ensure_table(table_name, created_at_ns=100)
            indexes = self.raw.list_indexes(table_name)
            created["indexes"] = {
                entry["config"]["name"]: entry["config"]
                for entry in indexes
                if "config" in entry and "name" in entry["config"]
            }
            return created

        def batch_write(
            self,
            table_name: str,
            *,
            inserts: dict[str, dict] | None = None,
            deletes: list[str] | None = None,
            transforms: list[dict[str, object]] | None = None,
            sync_level: str | None = None,
        ) -> dict:
            if self.backend == "stateful":
                return self.raw.batch_write(
                    table_name,
                    inserts=inserts,
                    deletes=deletes,
                    transforms=transforms,
                    sync_level=sync_level,
                )
            return self.raw.batch_table(
                table_name,
                inserts=inserts,
                deletes=deletes,
                transforms=transforms,
                sync_level=sync_level,
            )

        def publish_table(self, table_name: str, *, timeout_s: float = 30.0, interval_s: float = 0.5) -> dict | None:
            if self.backend == "stateful":
                return None
            try:
                self.raw.build_table(table_name)
            except requests.HTTPError as exc:
                assert exc.response is not None
                if exc.response.status_code != 409:
                    raise
            deadline = time.monotonic() + timeout_s
            while True:
                status = self.raw.table_build_status(table_name)
                if status.get("head_version", 0) >= 1 and status.get("published_wal_end_lsn", 0) >= 1:
                    return status
                if time.monotonic() >= deadline:
                    return None
                time.sleep(interval_s)

        def query_table(self, table_name: str, payload: dict) -> dict:
            if self.backend == "serverless":
                try:
                    return self.raw.query_table(table_name, payload)
                except requests.ConnectionError as exc:
                    logs = ""
                    server = getattr(self.raw, "_server", None)
                    if server is not None:
                        logs = server.debug_logs().strip()
                    message = str(exc)
                    if logs:
                        message += f"\n[logs]\n{logs}"
                    raise requests.ConnectionError(message, request=getattr(exc, "request", None)) from exc
            return self.raw.query_table(table_name, payload)

        def list_indexes(self, table_name: str) -> dict:
            return self.raw.list_indexes(table_name)

        def get_index(self, table_name: str, index_name: str) -> dict:
            return self.raw.get_index(table_name, index_name)

        def create_index(self, table_name: str, index_name: str, payload: dict) -> dict:
            return self.raw.create_index(table_name, index_name, payload)

        def delete_index(self, table_name: str, index_name: str) -> dict:
            return self.raw.delete_index(table_name, index_name)

        def post(self, path: str, payload: dict) -> dict:
            return self.raw.post(path, payload)

    return TableApi(backend, raw)
