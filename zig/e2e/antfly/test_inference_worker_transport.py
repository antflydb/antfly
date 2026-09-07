# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Elastic-2.0

"""Model-free worker qualification for full and focused standalone executables.

Build with -Dmetal=true (or another isolated backend), then set
ANTFLY_WORKER_TRANSPORT_TESTS=1. ANTFLY_BIN and ANTFLY_STANDALONE_BIN can
select the full and focused artifacts; defaults are under zig/zig-out/bin.
"""

from __future__ import annotations

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
        command = [str(renamed), *([] if focused else ["standalone"])]
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
                "Build with an isolated backend, e.g. -Dmetal=true"
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
