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

import json
import os
import signal
import subprocess
import tempfile
from pathlib import Path
from typing import Any

import pytest
import requests

from conftest import (
    DEFAULT_ANTFLY_BIN,
    REPO_ROOT,
    _data_command,
    _metadata_command,
    _read_log_tail,
    _write_remote_content_e2e_config,
    antfly_public_api_url,
    find_free_port,
    maybe_preserve_tempdir,
    wait_for_server,
)
from helpers import wait_until


def _write_memoryaf_package(store_root: Path) -> None:
    manifest_dir = store_root / "memoryaf" / "1.0.0"
    manifest_dir.mkdir(parents=True, exist_ok=True)
    (manifest_dir / "extension.json").write_text(
        json.dumps(
            {
                "manifest_api_version": "extensions/v1",
                "name": "memoryaf",
                "version": "1.0.0",
                "kind": "extension",
                "description": "E2E memory extension package",
                "digest": "sha256:e2e",
                "install": {
                    "scopes_supported": ["cluster", "table"],
                    "shapes": [
                        {
                            "name": "recall_request",
                            "kind": "tool_schema",
                            "version": "1",
                            "schema_json": '{"type":"object","properties":{"query":{"type":"string"}}}',
                        }
                    ],
                    "objects": [
                        {
                            "kind": "mcp_tool",
                            "name": "recall",
                            "shape": "recall_request",
                            "config_json": '{"handler":"antfly_api_template"}',
                        }
                    ],
                },
            }
        ),
        encoding="utf-8",
    )


class _ExtensionProcess:
    def __init__(self, binary: str, mode: str):
        self.binary = binary
        self.mode = mode
        self.host = "127.0.0.1"
        self.tempdir = tempfile.TemporaryDirectory(prefix=f"antfly-zig-extensions-{mode}-")
        self.root = Path(self.tempdir.name)
        self.package_store = self.root / "extensions"
        _write_memoryaf_package(self.package_store)

        self.public_port = find_free_port()
        self.metadata_raft_port = find_free_port()
        self.metadata_admin_port = find_free_port()
        self.data_raft_port = find_free_port()
        self.url = f"http://{self.host}:{self.public_port}"
        self.api_url = antfly_public_api_url(self.url, binary=binary)
        self.metadata_admin_url = f"http://{self.host}:{self.metadata_admin_port}"

        self.swarm_log_path = self.root / "swarm.log"
        self.metadata_log_path = self.root / "metadata.log"
        self.data_log_path = self.root / "data.log"
        self.swarm_log_file = self.swarm_log_path.open("w")
        self.metadata_log_file = self.metadata_log_path.open("w")
        self.data_log_file = self.data_log_path.open("w")
        self.swarm_proc: subprocess.Popen[str] | None = None
        self.metadata_proc: subprocess.Popen[str] | None = None
        self.data_proc: subprocess.Popen[str] | None = None

        try:
            if mode == "swarm":
                self._start_swarm()
            elif mode == "distributed":
                self._start_distributed()
            else:
                raise ValueError(mode)
        except BaseException:
            self.stop()
            raise

    def _start_swarm(self) -> None:
        command = [
            self.binary,
            "swarm",
            "--config",
            str(_write_remote_content_e2e_config(self.root)),
            "--host",
            self.host,
            "--port",
            str(self.public_port),
            "--data-dir",
            str(self.root),
            "--tick-ms",
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
        self.swarm_proc = subprocess.Popen(command, stdout=self.swarm_log_file, stderr=subprocess.STDOUT, cwd=self.root)
        if not wait_for_server(self.api_url):
            raise RuntimeError(f"swarm extension server failed to start\n{self.debug_logs()}")

    def _start_distributed(self) -> None:
        metadata_command = _metadata_command(
            self.binary,
            host=self.host,
            raft_port=self.metadata_raft_port,
            admin_port=self.metadata_admin_port,
            root=self.root,
        )
        metadata_command.extend(["--extension-package-store", str(self.package_store)])
        self.metadata_proc = subprocess.Popen(
            metadata_command,
            stdout=self.metadata_log_file,
            stderr=subprocess.STDOUT,
            cwd=self.root,
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
        self.data_proc = subprocess.Popen(
            data_command,
            stdout=self.data_log_file,
            stderr=subprocess.STDOUT,
            cwd=self.root,
        )
        if not wait_for_server(self.api_url):
            raise RuntimeError(f"data extension server failed to start\n{self.debug_logs()}")

    def debug_logs(self) -> str:
        for handle in (self.swarm_log_file, self.metadata_log_file, self.data_log_file):
            handle.flush()
        return (
            f"[swarm]\n{_read_log_tail(self.swarm_log_path)}\n"
            f"[metadata]\n{_read_log_tail(self.metadata_log_path)}\n"
            f"[data]\n{_read_log_tail(self.data_log_path)}"
        )

    def stop(self) -> None:
        for proc in (self.data_proc, self.metadata_proc, self.swarm_proc):
            if proc is not None and proc.poll() is None:
                proc.send_signal(signal.SIGTERM)
                try:
                    proc.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait()
        self.data_proc = None
        self.metadata_proc = None
        self.swarm_proc = None
        for handle in (self.swarm_log_file, self.metadata_log_file, self.data_log_file):
            if not handle.closed:
                handle.close()
        if not maybe_preserve_tempdir(self.tempdir):
            self.tempdir.cleanup()


@pytest.fixture(params=["swarm", "distributed"])
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


def test_extension_package_routes_match_swarm_and_distributed(extension_server: _ExtensionProcess) -> None:
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
    assert [package["name"] for package in packages] == ["memoryaf"]
    assert packages[0]["version"] == "1.0.0"

    if extension_server.mode == "distributed":
        return

    installed = _check_response(
        session.post(
            f"{base_url}/extensions/v1/installed/memoryaf",
            json={"version": "1.0.0", "scope": {"kind": "cluster"}},
            timeout=10,
        )
    )
    assert installed["name"] == "memoryaf"
    assert installed["package_version"] == "1.0.0"
    assert installed["scope"]["kind"] == "cluster"
    assert isinstance(installed["installed_at_epoch_ms"], int)
    assert installed["installed_at_epoch_ms"] > 1_700_000_000_000

    objects = _check_response(session.get(f"{base_url}/extensions/v1/installed/memoryaf/objects", timeout=10))
    object_kinds = {(obj["object_kind"], obj["object_name"]) for obj in objects}
    assert ("data_shape", "recall_request") in object_kinds
    assert ("mcp_tool", "recall") in object_kinds

    old_mcp_route = session.get(f"{base_url}/mcp/extensions/memoryaf", timeout=10)
    assert old_mcp_route.status_code == 404
