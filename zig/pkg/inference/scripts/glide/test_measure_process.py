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

"""GPU memory attribution regressions without optional packages or hardware."""

import io
import json
import os
import sys
import tempfile
import time
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from types import SimpleNamespace
from typing import ClassVar
from unittest.mock import patch

from measure_process import (
    gpu_inventory,
    main,
    measure,
    observed_gpu_identity,
    process_gpu_bytes,
    sample_process_tree,
)


class GpuMemoryTests(unittest.TestCase):
    devices: ClassVar = [
        (0, {"uuid": "GPU-first", "name": "first"}),
        (1, {"uuid": "GPU-second", "name": "second"}),
    ]

    def nvml(self, processes):
        return SimpleNamespace(
            nvmlDeviceGetCount=lambda: 2,
            nvmlDeviceGetHandleByIndex=lambda index: index,
            nvmlDeviceGetUUID=lambda handle: self.devices[handle][1]["uuid"],
            nvmlDeviceGetName=lambda handle: self.devices[handle][1]["name"],
            nvmlDeviceGetComputeRunningProcesses=lambda handle: processes[handle],
        )

    def test_visible_device_remapping_and_foreign_process(self):
        nvml = self.nvml(
            [
                [SimpleNamespace(pid=999, usedGpuMemory=10000)],
                [SimpleNamespace(pid=42, usedGpuMemory=300)],
            ]
        )
        with patch.dict("os.environ", {"CUDA_VISIBLE_DEVICES": "1"}):
            devices = gpu_inventory(nvml)
            usage = process_gpu_bytes(nvml, devices, 42)
        self.assertEqual(usage, {"GPU-second": 300})
        self.assertEqual(
            observed_gpu_identity(devices, set(usage)),
            {"gpu": "second", "gpu_devices": [self.devices[1][1]]},
        )

    def test_process_on_multiple_devices(self):
        nvml = self.nvml(
            [
                [SimpleNamespace(pid=42, usedGpuMemory=100)],
                [SimpleNamespace(pid=42, usedGpuMemory=200)],
            ]
        )
        usage = process_gpu_bytes(nvml, self.devices, 42)
        self.assertEqual(sum(usage.values()), 300)
        self.assertEqual(
            len(observed_gpu_identity(self.devices, set(usage))["gpu_devices"]), 2
        )

    def test_unavailable_memory_is_rejected(self):
        for value in (None, 2**64 - 1, -1):
            with self.subTest(value=value):
                nvml = self.nvml([[], [SimpleNamespace(pid=42, usedGpuMemory=value)]])
                with self.assertRaisesRegex(RuntimeError, "GPU memory unavailable"):
                    process_gpu_bytes(nvml, self.devices, 42)

    def test_descendant_host_and_gpu_memory_are_counted(self):
        worker = SimpleNamespace(
            pid=43, memory_full_info=lambda: SimpleNamespace(rss=20, pss=10)
        )
        root = SimpleNamespace(
            pid=42,
            children=lambda recursive: [worker],
            memory_full_info=lambda: SimpleNamespace(rss=100, pss=80),
        )
        psutil = SimpleNamespace(
            Process=lambda pid: root, NoSuchProcess=ProcessLookupError
        )
        nvml = self.nvml([[SimpleNamespace(pid=43, usedGpuMemory=64)], []])
        self.assertEqual(
            sample_process_tree(psutil, nvml, self.devices, 42),
            (120, 90, {"GPU-first": 64}),
        )

    def test_missing_observations_are_rejected(self):
        with self.assertRaisesRegex(RuntimeError, "No process GPU memory observed"):
            observed_gpu_identity(self.devices, set())


class LifecycleTests(unittest.TestCase):
    def run_measure(self, code, sample, **kwargs):
        nvml = GpuMemoryTests().nvml([[], []])
        nvml.nvmlInit = lambda: None
        nvml.nvmlShutdown = lambda: None
        nvml.nvmlSystemGetDriverVersion = lambda: "test-driver"
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "receipt"
            result = measure(
                [sys.executable, "-c", code],
                output,
                nvml,
                sample,
                interval=0.005,
                grace=0.1,
                **kwargs,
            )
            self.assertEqual(
                result, json.loads(output.with_suffix(".json").read_text())
            )
            return result, output.with_suffix(".log").read_text()

    @staticmethod
    def gpu_sample(_devices, _pid):
        return 100, 80, {"GPU-first": 64}

    def test_success_and_actual_cadence(self):
        result, log = self.run_measure(
            'import time; print("loaded backend=cuda", flush=True); time.sleep(0.1)',
            self.gpu_sample,
        )
        self.assertTrue(result["passed"])
        self.assertGreater(result["peaks_bytes"]["inference"]["gpu_samples"], 0)
        self.assertGreater(result["sampling_actual_interval_s"]["mean"], 0)
        self.assertIn("loaded backend=cuda", log)

    def test_startup_gpu_does_not_validate_missing_inference_gpu(self):
        def sample(_devices, _pid):
            return 100, 80, {"GPU-first": 64} if not marker.exists() else {}

        with tempfile.TemporaryDirectory() as directory:
            marker = Path(directory) / "loaded"
            code = (
                "import time,pathlib; time.sleep(0.08); "
                f"pathlib.Path({str(marker)!r}).touch(); "
                'print("loaded backend=cuda",flush=True); time.sleep(0.08)'
            )
            result, _ = self.run_measure(code, sample)
        self.assertFalse(result["passed"])
        self.assertEqual(result["exit_code"], 0)
        self.assertGreater(result["peaks_bytes"]["startup"]["gpu_samples"], 0)
        self.assertIsNone(result["peaks_bytes"]["inference"]["gpu"])
        self.assertIn("No inference GPU memory", result["measurement_error"])

    def test_missing_marker_and_child_failure_are_invalid(self):
        for code in (
            "import time; time.sleep(0.05)",
            'import sys; print("loaded backend=cuda",flush=True); sys.exit(7)',
        ):
            with self.subTest(code=code):
                result, _ = self.run_measure(code, self.gpu_sample)
                self.assertFalse(result["passed"])

    def test_empty_inference_sample_window_is_invalid(self):
        def slow_startup(_devices, _pid):
            time.sleep(0.08)
            return 100, 80, {"GPU-first": 64}

        result, _ = self.run_measure(
            'import time; time.sleep(0.02); print("loaded backend=cuda",flush=True)',
            slow_startup,
        )
        self.assertFalse(result["passed"])
        self.assertEqual(result["peaks_bytes"]["inference"]["gpu_samples"], 0)

    def test_sampling_failure_kills_worker_and_retains_receipt(self):
        with tempfile.TemporaryDirectory() as directory:
            worker = Path(directory) / "worker.pid"
            child = (
                "import os,signal,time,pathlib; "
                "signal.signal(signal.SIGTERM,signal.SIG_IGN); "
                f"pathlib.Path({str(worker)!r}).write_text(str(os.getpid())); "
                "time.sleep(30)"
            )
            code = (
                f'import subprocess,sys,time; subprocess.Popen([sys.executable,"-c",{child!r}]); '
                'print("loaded backend=cuda",flush=True); time.sleep(30)'
            )

            def sample(_devices, _pid):
                if worker.exists():
                    raise RuntimeError("injected NVML failure")
                return self.gpu_sample(_devices, _pid)

            started = time.monotonic()
            result, _ = self.run_measure(code, sample)
            self.assertLess(time.monotonic() - started, 3)
            self.assertFalse(result["passed"])
            self.assertIn("injected NVML failure", result["measurement_error"])
            pid = int(worker.read_text())
            # SIGKILL delivery is asynchronous; init may also retain a zombie.
            deadline = time.monotonic() + 1
            while True:
                stat = Path(f"/proc/{pid}/stat")
                try:
                    if stat.exists() and stat.read_text().split()[2] == "Z":
                        break
                    os.kill(pid, 0)
                except (ProcessLookupError, FileNotFoundError):
                    break
                self.assertLess(
                    time.monotonic(), deadline, "worker survived group cleanup"
                )
                time.sleep(0.005)

    def test_initialization_and_launch_failures_persist_receipts(self):
        nvml = GpuMemoryTests().nvml([[], []])

        def fail():
            raise RuntimeError("injected initialization failure")

        nvml.nvmlInit = fail
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "failed-init"
            result = measure(
                [sys.executable, "-c", "pass"], output, nvml, self.gpu_sample
            )
            self.assertFalse(result["passed"])
            self.assertIn(
                "injected initialization failure", result["measurement_error"]
            )
            self.assertEqual(
                result, json.loads(output.with_suffix(".json").read_text())
            )
            nvml.nvmlInit = lambda: None
            nvml.nvmlShutdown = lambda: None
            nvml.nvmlSystemGetDriverVersion = lambda: "test"
            result = measure(
                [str(Path(directory) / "missing-executable")],
                output,
                nvml,
                self.gpu_sample,
            )
            self.assertFalse(result["passed"])
            self.assertIsNone(result["exit_code"])

    def test_cli_exits_nonzero_for_successful_child_with_invalid_measurement(self):
        nvml = GpuMemoryTests().nvml([[], []])
        nvml.nvmlInit = lambda: None
        nvml.nvmlShutdown = lambda: None
        nvml.nvmlSystemGetDriverVersion = lambda: "test"
        psutil = SimpleNamespace(NoSuchProcess=ProcessLookupError)
        code = 'import time; print("loaded backend=cuda",flush=True); time.sleep(0.05)'
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "cli"
            with (
                patch.dict(sys.modules, {"psutil": psutil, "pynvml": nvml}),
                patch(
                    "measure_process.sample_process_tree", return_value=(100, 80, {})
                ),
                patch.object(
                    sys,
                    "argv",
                    [
                        "measure_process",
                        "--out",
                        str(output),
                        "--",
                        sys.executable,
                        "-c",
                        code,
                    ],
                ),
                redirect_stdout(io.StringIO()),
                self.assertRaises(SystemExit) as raised,
            ):
                main()
            self.assertEqual(raised.exception.code, 1)
            receipt = json.loads(output.with_suffix(".json").read_text())
            self.assertEqual(receipt["exit_code"], 0)
            self.assertFalse(receipt["passed"])

    def test_orphan_output_pipe_is_bounded(self):
        child = "import signal,time; signal.signal(signal.SIGTERM,signal.SIG_IGN); time.sleep(30)"
        code = f'import subprocess,sys; subprocess.Popen([sys.executable,"-c",{child!r}]); print("loaded backend=cuda",flush=True)'
        started = time.monotonic()
        result, _ = self.run_measure(code, self.gpu_sample)
        self.assertLess(time.monotonic() - started, 3)
        self.assertFalse(result["passed"])
        self.assertIn("output pipe remained open", result["measurement_error"])


if __name__ == "__main__":
    unittest.main()
