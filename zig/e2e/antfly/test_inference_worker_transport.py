# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
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

"""Model-free worker qualification for full and focused standalone executables.

Build antfly, then set ANTFLY_WORKER_TRANSPORT_TESTS=1. Standalone's live
content-security source also requires worker isolation on CPU. ANTFLY_BIN and ANTFLY_STANDALONE_BIN can
select the full and focused artifacts; defaults are under zig/zig-out/bin.
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import time
from pathlib import Path

import pytest
import requests
from port_reservations import LoopbackPortReservations
from test_standalone import _inference_worker_pid


@pytest.mark.parametrize("focused", [False, True], ids=["full", "focused"])
def test_worker_launch_body_limits_and_replacement(tmp_path, focused):
    if os.environ.get("ANTFLY_WORKER_TRANSPORT_TESTS") != "1":
        pytest.skip("Set ANTFLY_WORKER_TRANSPORT_TESTS=1 to qualify worker transport")
    if os.name != "posix":
        pytest.skip("Worker fault injection requires POSIX signals")
    zig_root = Path(__file__).resolve().parents[2]
    binary_name = "antfly-standalone" if focused else "antfly"
    binary_env = "ANTFLY_STANDALONE_BIN" if focused else "ANTFLY_BIN"
    binary = Path(os.environ.get(binary_env, zig_root / "zig-out/bin" / binary_name))
    assert binary.is_file(), f"Build the {binary_name} artifact first"
    # A renamed executable must retain the private launch contract.
    renamed = tmp_path / "renamed-runtime"
    os.link(binary.resolve(), renamed)
    log_path = tmp_path / "server.log"
    process = None
    with LoopbackPortReservations() as ports, log_path.open("w") as log:
        port = ports.reserve()
        config_path = tmp_path / "config.json"
        config_path.write_text(json.dumps({"public_inference": {"enabled": True}}))
        command = [
            str(renamed),
            *([] if focused else ["standalone"]),
            "--config",
            str(config_path),
        ]
        command.extend(
            [
                "--host",
                "127.0.0.1",
                "--port",
                str(port),
                "--health=false",
                "--models-dir",
                str(tmp_path / "models"),
                "--data-dir",
                str(tmp_path / "data"),
                "--replica-root-dir",
                str(tmp_path / "replicas"),
                "--replica-catalog-path",
                str(tmp_path / "catalog.txt"),
                "--snapshot-root-dir",
                str(tmp_path / "snapshots"),
            ]
        )
        base = f"http://127.0.0.1:{port}"

        def logs():
            log.flush()
            return log_path.read_text()[-16000:]

        def ready():
            assert process.poll() is None, logs()
            try:
                return requests.get(f"{base}/readyz", timeout=1).ok
            except requests.RequestException:
                return False

        try:
            process = ports.handoff_to(
                (port,),
                lambda: subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT),
            )
            deadline = time.monotonic() + 60
            while not ready():
                assert time.monotonic() < deadline, logs()
                time.sleep(0.05)
            worker = _inference_worker_pid(process.pid)
            assert worker is not None, (
                "Standalone inference must launch its supervised worker"
            )
            # Missing embedding fields intentionally produce a stable 400 from
            # the worker's parser, without downloading or running a model.
            endpoint = f"{base}/ai/v1/embed"
            baseline = requests.post(endpoint, data=b"{}", timeout=10)
            assert baseline.status_code == 400, baseline.text + logs()
            for size in (48 * 1024 * 1024, 64 * 1024 * 1024):
                body = b" " * (size - 2) + b"{}"
                response = requests.post(
                    endpoint,
                    data=body,
                    headers={"Content-Type": "application/json"},
                    timeout=60,
                )
                assert response.status_code == 400, response.text + logs()
                assert response.json() == baseline.json()
                assert _inference_worker_pid(process.pid) == worker
            response = requests.post(endpoint, data=body + b" ", timeout=60)
            assert response.status_code == 413, response.text + logs()
            assert ready()
            assert _inference_worker_pid(process.pid) == worker
            os.kill(worker, signal.SIGKILL)
            deadline = time.monotonic() + 30
            while True:
                response = requests.post(endpoint, data=b"{}", timeout=10)
                if response.status_code == 400:
                    replacement = _inference_worker_pid(process.pid)
                    if replacement is not None and replacement != worker:
                        break
                assert time.monotonic() < deadline, response.text + logs()
                time.sleep(0.05)
            assert response.json() == baseline.json()
            assert ready()
        finally:
            if process is not None and process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=10)
                    pytest.fail("Standalone shutdown required SIGKILL\n" + logs())
        assert process.returncode == 0, logs()


@pytest.mark.e2e_resource("antfly_process")
def test_worker_live_local_policy_large_snapshots_and_replacement(tmp_path):
    """Exercise the production callback, CPU worker launch, reload and restart."""
    if os.environ.get("ANTFLY_WORKER_TRANSPORT_TESTS") != "1":
        pytest.skip("Set ANTFLY_WORKER_TRANSPORT_TESTS=1 to qualify worker transport")
    if os.name != "posix":
        pytest.skip("Worker fault injection requires POSIX signals")
    zig_root = Path(__file__).resolve().parents[2]
    binary = Path(os.environ.get("ANTFLY_BIN", zig_root / "zig-out/bin/antfly"))
    assert binary.is_file(), "Build antfly first"
    media = tmp_path / "media.wav"
    media.write_bytes(b"0123456789")
    config_path = tmp_path / "config.json"
    # This snapshot fits the policy cap, but its former doubly JSON-encoded
    # worker metadata exceeds 1 MiB. Host entries are irrelevant to local-file
    # authorization, so the test measures transport rather than path searching.
    padding = ["media.io"] * 65000

    def publish(allow):
        paths = [str(tmp_path)] if allow else []
        config = {
            "public_inference": {"enabled": True},
            "remote_content": {
                "security": {"allowed_paths": paths, "allowed_hosts": padding}
            },
        }
        encoded = json.dumps(config, separators=(",", ":"))
        assert len(encoded) < 1024 * 1024
        pending = config_path.with_suffix(".next")
        pending.write_text(encoded)
        pending.replace(config_path)

    publish(False)
    request = {
        "input": {
            "type": "source",
            "uri": media.as_uri(),
            "mime_type": "audio/wav",
            "version_kind": "file_sha256",
            "version": "0" * 64,
            "length": media.stat().st_size,
        },
        "embedding": {"model": "fixture-policy-model"},
        "max_chunks": 1,
    }
    log_path = tmp_path / "policy-worker.log"
    process = None
    with LoopbackPortReservations() as ports, log_path.open("w") as log:
        port = ports.reserve()
        base = f"http://127.0.0.1:{port}"
        command = [
            str(binary.resolve()),
            "standalone",
            "--config",
            str(config_path),
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "--health=false",
            "--models-dir",
            str(tmp_path / "models"),
            "--data-dir",
            str(tmp_path / "data"),
            "--replica-root-dir",
            str(tmp_path / "replicas"),
            "--replica-catalog-path",
            str(tmp_path / "catalog.txt"),
            "--snapshot-root-dir",
            str(tmp_path / "snapshots"),
        ]

        def logs():
            log.flush()
            return log_path.read_text()[-16000:]

        def expect_policy(error):
            deadline = time.monotonic() + 30
            while True:
                assert process.poll() is None, logs()
                response = requests.post(
                    f"{base}/ai/v1/chunk", json=request, timeout=10
                )
                if (
                    response.status_code in (400, 409)
                    and response.json().get("error") == error
                ):
                    return
                assert time.monotonic() < deadline, response.text + logs()
                time.sleep(0.1)

        try:
            process = ports.handoff_to(
                (port,),
                lambda: subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT),
            )
            deadline = time.monotonic() + 60
            while True:
                assert process.poll() is None, logs()
                try:
                    if requests.get(f"{base}/readyz", timeout=1).ok:
                        break
                except requests.RequestException:
                    pass
                assert time.monotonic() < deadline, logs()
                time.sleep(0.05)
            worker = _inference_worker_pid(process.pid)
            assert worker is not None, logs()
            expect_policy("PathNotAllowed")
            # Ordinary text requests retain their independent 64 MiB body cap
            # even while the full large policy crosses each worker request.
            embed = f"{base}/ai/v1/embed"
            baseline = requests.post(embed, data=b"{}", timeout=10)
            assert baseline.status_code == 400, baseline.text + logs()
            body = b" " * (64 * 1024 * 1024 - 2) + b"{}"
            large = requests.post(embed, data=body, timeout=60)
            assert large.status_code == 400, large.text + logs()
            assert large.json() == baseline.json()
            assert requests.post(embed, data=body + b" ", timeout=60).status_code == 413
            publish(True)
            # A wrong digest proves the worker reached actual local verification
            # without requiring a model. Revocation must stop it before that.
            expect_policy("SourceVersionChanged")
            assert _inference_worker_pid(process.pid) == worker
            publish(False)
            expect_policy("PathNotAllowed")
            publish(True)
            expect_policy("SourceVersionChanged")
            os.kill(worker, signal.SIGKILL)
            expect_policy("SourceVersionChanged")
            replacement = _inference_worker_pid(process.pid)
            assert replacement is not None and replacement != worker, logs()
            publish(False)
            expect_policy("PathNotAllowed")
            assert _inference_worker_pid(process.pid) == replacement
        finally:
            if process is not None and process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=10)
                    pytest.fail("Standalone shutdown required SIGKILL\n" + logs())
        assert process.returncode == 0, logs()
