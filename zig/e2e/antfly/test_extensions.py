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

from __future__ import annotations

import os
import json
import signal
import subprocess
import tempfile
from pathlib import Path
from typing import Any

import pytest
import requests

from conftest import (
    DEFAULT_ANTFLY_BIN,
    LoopbackPortReservations,
    REPO_ROOT,
    _data_command,
    _metadata_command,
    _read_log_tail,
    _write_remote_content_e2e_config,
    antfly_public_api_url,
    maybe_preserve_tempdir,
    wait_for_server,
)
from helpers import wait_until

pytestmark = pytest.mark.slow

MEMORYAF_VERSION = "0.0.1"
MEMORYAF_PACKAGE_STORE = REPO_ROOT.parent / "extensions"
MEMORYAF_GRANTS = [
    {"name": "db:read", "scope": "memoryaf"},
    {"name": "db:write", "scope": "memoryaf"},
    {"name": "ai:embed", "scope": "memoryaf"},
    {"name": "mcp:tool", "scope": "memoryaf"},
]


class _ExtensionProcess:
    def __init__(self, binary: str, mode: str):
        self.binary = binary
        self.mode = mode
        self.host = "127.0.0.1"
        self.tempdir = tempfile.TemporaryDirectory(prefix=f"antfly-zig-extensions-{mode}-")
        self.root = Path(self.tempdir.name)
        self.package_store = MEMORYAF_PACKAGE_STORE
        if not (self.package_store / "memoryaf" / "extension.json").exists():
            raise RuntimeError(f"memoryaf extension package not found under {self.package_store}")

        self.port_reservations = LoopbackPortReservations(self.host)
        self.public_port = self.port_reservations.reserve()
        self.metadata_raft_port = self.port_reservations.reserve()
        self.metadata_admin_port = self.port_reservations.reserve()
        self.data_raft_port = self.port_reservations.reserve()
        self.url = f"http://{self.host}:{self.public_port}"
        self.api_url = antfly_public_api_url(self.url, binary=binary)
        self.metadata_admin_url = f"http://{self.host}:{self.metadata_admin_port}"

        self.standalone_log_path = self.root / "standalone.log"
        self.metadata_log_path = self.root / "metadata.log"
        self.data_log_path = self.root / "data.log"
        self.standalone_log_file = self.standalone_log_path.open("w")
        self.metadata_log_file = self.metadata_log_path.open("w")
        self.data_log_file = self.data_log_path.open("w")
        self.standalone_proc: subprocess.Popen[str] | None = None
        self.metadata_proc: subprocess.Popen[str] | None = None
        self.data_proc: subprocess.Popen[str] | None = None

        try:
            if mode == "standalone":
                self._start_standalone()
            elif mode == "distributed":
                self._start_distributed()
            else:
                raise ValueError(mode)
        except BaseException:
            self.stop()
            raise

    def _start_standalone(self) -> None:
        env = self._server_env()
        command = [
            self.binary,
            "standalone",
            "--config",
            str(_write_remote_content_e2e_config(self.root)),
            "--host",
            self.host,
            "--port",
            str(self.public_port),
            "--data-dir",
            str(self.root),
            "--control-tick-ms",
            "5",
            "--replica-root-dir",
            str(self.root / "replicas"),
            "--replica-catalog-path",
            str(self.root / "catalog.txt"),
            "--snapshot-root-dir",
            str(self.root / "snapshots"),
            "--extension-package-store",
            str(self.package_store),
        ]
        self.port_reservations.release(self.public_port)
        self.standalone_proc = subprocess.Popen(command, stdout=self.standalone_log_file, stderr=subprocess.STDOUT, cwd=self.root, env=env)
        if not wait_for_server(self.api_url):
            raise RuntimeError(f"standalone extension server failed to start\n{self.debug_logs()}")

    def _start_distributed(self) -> None:
        env = self._server_env()
        metadata_command = _metadata_command(
            self.binary,
            host=self.host,
            raft_port=self.metadata_raft_port,
            admin_port=self.metadata_admin_port,
            root=self.root,
        )
        metadata_command.extend(["--extension-package-store", str(self.package_store)])
        self.port_reservations.release(self.metadata_raft_port, self.metadata_admin_port)
        self.metadata_proc = subprocess.Popen(
            metadata_command,
            stdout=self.metadata_log_file,
            stderr=subprocess.STDOUT,
            cwd=self.root,
            env=env,
        )
        if not wait_for_server(self.metadata_admin_url, path="/metadata/v1/status"):
            raise RuntimeError(f"metadata extension server failed to start\n{self.debug_logs()}")

        data_command = _data_command(
            self.binary,
            host=self.host,
            port=self.public_port,
            raft_port=self.data_raft_port,
            metadata_admin_base_uri=self.metadata_admin_url,
            root=self.root,
        )
        self.port_reservations.release(self.public_port, self.data_raft_port)
        self.data_proc = subprocess.Popen(
            data_command,
            stdout=self.data_log_file,
            stderr=subprocess.STDOUT,
            cwd=self.root,
            env=env,
        )
        if not wait_for_server(self.api_url):
            raise RuntimeError(f"data extension server failed to start\n{self.debug_logs()}")

    def _server_env(self) -> dict[str, str]:
        env = os.environ.copy()
        env["ANTFLY_EXTENSION_PACKAGE_STORE"] = str(self.package_store)
        return env

    def debug_logs(self) -> str:
        for handle in (self.standalone_log_file, self.metadata_log_file, self.data_log_file):
            handle.flush()
        return (
            f"[standalone]\n{_read_log_tail(self.standalone_log_path)}\n"
            f"[metadata]\n{_read_log_tail(self.metadata_log_path)}\n"
            f"[data]\n{_read_log_tail(self.data_log_path)}"
        )

    def stop(self) -> None:
        self.port_reservations.close()
        for proc in (self.data_proc, self.metadata_proc, self.standalone_proc):
            if proc is not None and proc.poll() is None:
                proc.send_signal(signal.SIGTERM)
                try:
                    proc.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait()
        self.data_proc = None
        self.metadata_proc = None
        self.standalone_proc = None
        for handle in (self.standalone_log_file, self.metadata_log_file, self.data_log_file):
            if not handle.closed:
                handle.close()
        if not maybe_preserve_tempdir(self.tempdir):
            self.tempdir.cleanup()


@pytest.fixture(params=["standalone", "distributed"])
def extension_server(request) -> _ExtensionProcess:
    binary = Path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN))).expanduser().resolve()
    if binary.name != "antfly":
        pytest.skip("extension e2e requires the unified antfly binary")
    if not binary.exists():
        pytest.skip(f"antfly binary not built: {binary}")

    server = _ExtensionProcess(str(binary), request.param)
    try:
        yield server
    finally:
        server.stop()


def _check_response(response: requests.Response) -> Any:
    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        raise AssertionError(f"{response.request.method} {response.url} failed: {response.text}") from exc
    if not response.content:
        return {}
    return response.json()


def test_extension_package_routes_match_standalone_and_distributed(extension_server: _ExtensionProcess) -> None:
    _assert_extension_package_routes(extension_server)


def test_extension_memoryaf_wasm_runtime_required() -> None:
    if not os.environ.get("ANTFLY_WASMTIME_LIB"):
        pytest.skip("set ANTFLY_WASMTIME_LIB to run the required Wasmtime extension runtime e2e")
    binary = Path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN))).expanduser().resolve()
    if binary.name != "antfly":
        pytest.skip("extension e2e requires the unified antfly binary")
    if not binary.exists():
        pytest.skip(f"antfly binary not built: {binary}")

    server = _ExtensionProcess(str(binary), "standalone")
    try:
        _assert_extension_package_routes(server)
    finally:
        server.stop()


def _assert_extension_package_routes(extension_server: _ExtensionProcess) -> None:
    session = requests.Session()
    base_url = extension_server.url

    root = _check_response(session.get(f"{base_url}/extensions/v1", timeout=10))
    assert root == {
        "packages": "/extensions/v1/packages",
        "installed": "/extensions/v1/installed",
    }

    def projected_packages() -> list[dict[str, Any]] | None:
        packages = _check_response(session.get(f"{base_url}/extensions/v1/packages", timeout=10))
        if any(package.get("name") == "memoryaf" for package in packages):
            return packages
        return None

    packages = wait_until(projected_packages, timeout_s=10.0, interval_s=0.25)
    assert packages is not None, f"memoryaf package was not projected\n{extension_server.debug_logs()}"
    memoryaf_package = next(package for package in packages if package["name"] == "memoryaf")
    assert memoryaf_package["version"] == MEMORYAF_VERSION
    assert memoryaf_package["artifacts"][0]["kind"] == "wasm"

    dry_run = _check_response(
        session.post(
            f"{base_url}/extensions/v1/installed/memoryaf",
            json={"version": MEMORYAF_VERSION, "scope": {"kind": "cluster"}, "dry_run": True},
            timeout=10,
        )
    )
    assert dry_run["name"] == "memoryaf"
    assert dry_run["package_version"] == MEMORYAF_VERSION
    assert dry_run["scope"]["kind"] == "cluster"

    installed_after_dry_run = _check_response(session.get(f"{base_url}/extensions/v1/installed", timeout=10))
    assert all(extension.get("name") != "memoryaf" for extension in installed_after_dry_run)

    table_name = f"memoryaf_memories_{extension_server.mode}"
    created_table = _check_response(
        session.post(f"{extension_server.api_url}/tables/{table_name}", json={"num_shards": 1}, timeout=30)
    )
    assert created_table["name"] == table_name

    installed = _check_response(
        session.post(
            f"{base_url}/extensions/v1/installed/memoryaf",
            json={
                "version": MEMORYAF_VERSION,
                "scope": {"kind": "table", "table_name": table_name},
                "grants": MEMORYAF_GRANTS,
            },
            timeout=10,
        )
    )
    assert installed["name"] == "memoryaf"
    assert installed["package_version"] == MEMORYAF_VERSION
    assert installed["scope"] == {"kind": "table", "table_name": table_name}
    assert isinstance(installed["installed_at_epoch_ms"], int)
    assert installed["installed_at_epoch_ms"] > 1_700_000_000_000

    objects = _check_response(session.get(f"{base_url}/extensions/v1/installed/memoryaf/objects", timeout=10))
    object_kinds = {(obj["object_kind"], obj["object_name"]) for obj in objects}
    assert ("data_shape", "memory_record") in object_kinds
    assert ("generated_artifact", "memory_embedding") in object_kinds
    assert ("skill", "memory") in object_kinds
    assert ("mcp_tool", "store_memory") in object_kinds
    assert ("mcp_tool", "search_memories") in object_kinds
    assert ("mcp_tool", "list_memories") in object_kinds

    old_mcp_route = session.get(f"{base_url}/mcp/extensions/memoryaf", timeout=10)
    assert old_mcp_route.status_code == 404

    mcp_endpoint = f"{base_url}/mcp/v1/extensions/memoryaf"
    initialize = session.post(
        mcp_endpoint,
        json={"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}},
        timeout=10,
    )
    init_response = _check_response(initialize)
    assert init_response["jsonrpc"] == "2.0"
    mcp_session_id = initialize.headers["Mcp-Session-Id"]

    tools = _check_response(
        session.post(
            mcp_endpoint,
            headers={"Mcp-Session-Id": mcp_session_id},
            json={"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}},
            timeout=10,
        )
    )
    tool_names = {tool["name"] for tool in tools["result"]["tools"]}
    assert {"store_memory", "search_memories", "list_memories"}.issubset(tool_names)
    assert "create_table" not in tool_names

    if not os.environ.get("ANTFLY_WASMTIME_LIB"):
        return

    store = _check_response(
        session.post(
            mcp_endpoint,
            headers={"Mcp-Session-Id": mcp_session_id},
            json={
                "jsonrpc": "2.0",
                "id": 3,
                "method": "tools/call",
                "params": {
                    "name": "store_memory",
                    "arguments": {
                        "content": "Extension runtimes should be executable",
                        "project": "antfly",
                        "visibility": "team",
                    },
                },
            },
            timeout=10,
        )
    )
    assert store["result"]["isError"] is False
    assert store["result"]["structuredContent"]["ok"] is True
    assert store["result"]["structuredContent"]["tool"] == "store_memory"
    assert store["result"]["structuredContent"]["status"] == "stored"
    assert "db.write(memory_record)" in store["result"]["structuredContent"]["host_calls"]
    assert "ai.embed(content)" in store["result"]["structuredContent"]["host_calls"]
    assert store["result"]["structuredContent"]["host_results"]["embedding_dimensions"] == 8
    assert store["result"]["structuredContent"]["host_results"]["write"]["inserted"] == 1

    search = _check_response(
        session.post(
            mcp_endpoint,
            headers={"Mcp-Session-Id": mcp_session_id},
            json={
                "jsonrpc": "2.0",
                "id": 4,
                "method": "tools/call",
                "params": {"name": "search_memories", "arguments": {"query": "extension runtime", "limit": 5}},
            },
            timeout=10,
        )
    )
    assert search["result"]["isError"] is False
    assert search["result"]["structuredContent"]["tool"] == "search_memories"
    assert search["result"]["structuredContent"]["query"] == "extension runtime"
    assert search["result"]["structuredContent"]["limit"] == 5
    assert "Extension runtimes should be executable" in json.dumps(
        search["result"]["structuredContent"]["host_results"]["rows"]
    )

    listed = _check_response(
        session.post(
            mcp_endpoint,
            headers={"Mcp-Session-Id": mcp_session_id},
            json={
                "jsonrpc": "2.0",
                "id": 6,
                "method": "tools/call",
                "params": {"name": "list_memories", "arguments": {"limit": 5}},
            },
            timeout=10,
        )
    )
    assert listed["result"]["isError"] is False
    assert "Extension runtimes should be executable" in json.dumps(
        listed["result"]["structuredContent"]["host_results"]["rows"]
    )

    missing_query = _check_response(
        session.post(
            mcp_endpoint,
            headers={"Mcp-Session-Id": mcp_session_id},
            json={
                "jsonrpc": "2.0",
                "id": 7,
                "method": "tools/call",
                "params": {"name": "search_memories", "arguments": {}},
            },
            timeout=10,
        )
    )
    assert missing_query["result"]["isError"] is True
    assert missing_query["result"]["structuredContent"]["error"]["code"] == "invalid_request"
