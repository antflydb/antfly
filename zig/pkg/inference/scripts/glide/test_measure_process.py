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
import subprocess
import sys
import tempfile
import time
import unittest
from contextlib import redirect_stdout
from functools import partial
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
    def run_measure(self, code=None, sample=None, *, prepare=None, **kwargs):
        nvml = GpuMemoryTests().nvml([[], []])
        nvml.nvmlInit = lambda: None
        nvml.nvmlShutdown = lambda: None
        nvml.nvmlSystemGetDriverVersion = lambda: "test-driver"
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "receipt"
            child = {}
            if prepare is not None:
                code, sample = prepare(output, child)
            real_popen = subprocess.Popen

            def launch(*args, **launch_kwargs):
                child["process"] = real_popen(*args, **launch_kwargs)
                return child["process"]

            with patch("measure_process.subprocess.Popen", side_effect=launch):
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

    @staticmethod
    def phase_case(
        output,
        child,
        *,
        inference_gpu=True,
        empty_inference=False,
        marker=True,
        exit_code=0,
        sample_delay=0,
    ):
        """A child advances only after the sampler acknowledges each phase.

        The flushed log proves that measure() has consumed each marker before
        the sample callback releases the corresponding acknowledgement.
        """
        startup_ack = output.parent / "startup.ack"
        inference_ack = output.parent / "inference.ack"
        log = output.with_suffix(".log")
        load_marker = "loaded backend=cuda" if marker else "inference-ready"
        code = f"""
import pathlib, sys, time

def wait_for(path):
    deadline = time.monotonic() + 10
    while not pathlib.Path(path).exists():
        if time.monotonic() >= deadline:
            raise TimeoutError("sampler acknowledgement missing: " + path)
        time.sleep(0.001)

print("startup-ready", flush=True)
wait_for({str(startup_ack)!r})
print({load_marker!r}, flush=True)
"""
        if not empty_inference:
            code += f"wait_for({str(inference_ack)!r})\n"
        code += f"sys.exit({exit_code})\n"
        inference_samples = 0
        delayed = False

        def sample(_devices, _pid):
            nonlocal inference_samples, delayed
            # Deliberately pause the parent beyond the old timing windows.
            # Correctness comes from acknowledgements, not the pause duration.
            if sample_delay and not delayed:
                delayed = True
                time.sleep(sample_delay)
            text = log.read_text()
            usage = {"GPU-first": 64}
            if load_marker in text:
                inference_samples += 1
                if not inference_gpu:
                    usage = {}
                if inference_samples >= 2:
                    inference_ack.touch()
            elif "startup-ready" in text:
                startup_ack.touch()
                if empty_inference:
                    # The child exits before this startup sample returns.
                    # measure() must then drain the marker without inventing
                    # an inference sample; wait() observes exit without a race.
                    child["process"].wait(timeout=10)
            return 100, 80, usage

        return code, sample

    def test_success_and_actual_cadence(self):
        for delay in (0, 0.2):
            with self.subTest(parent_delay=delay):
                result, log = self.run_measure(
                    prepare=partial(self.phase_case, sample_delay=delay)
                )
                self.assertTrue(result["passed"])
                self.assertGreaterEqual(
                    result["peaks_bytes"]["inference"]["gpu_samples"], 2
                )
                self.assertGreater(result["sampling_actual_interval_s"]["mean"], 0)
                self.assertIn("loaded backend=cuda", log)

    def test_startup_gpu_does_not_validate_missing_inference_gpu(self):
        for delay in (0, 0.2):
            with self.subTest(parent_delay=delay):
                result, _ = self.run_measure(
                    prepare=partial(
                        self.phase_case, inference_gpu=False, sample_delay=delay
                    )
                )
                self.assertFalse(result["passed"])
                self.assertEqual(result["exit_code"], 0)
                self.assertGreater(result["peaks_bytes"]["startup"]["gpu_samples"], 0)
                self.assertGreaterEqual(
                    result["peaks_bytes"]["inference"]["samples"], 2
                )
                self.assertIsNone(result["peaks_bytes"]["inference"]["gpu"])
                self.assertIn("No inference GPU memory", result["measurement_error"])

    def test_missing_marker_and_child_failure_are_invalid(self):
        for marker, exit_code in ((False, 0), (True, 7)):
            with self.subTest(marker=marker, exit_code=exit_code):
                result, _ = self.run_measure(
                    prepare=partial(self.phase_case, marker=marker, exit_code=exit_code)
                )
                self.assertFalse(result["passed"])
                self.assertEqual(result["exit_code"], exit_code)
                if not marker:
                    self.assertIn("Missing load marker", result["measurement_error"])
                else:
                    self.assertNotIn("measurement_error", result)

    def test_empty_inference_sample_window_is_invalid(self):
        for delay in (0, 0.2):
            with self.subTest(parent_delay=delay):
                result, _ = self.run_measure(
                    prepare=partial(
                        self.phase_case, empty_inference=True, sample_delay=delay
                    )
                )
                self.assertFalse(result["passed"])
                self.assertEqual(result["exit_code"], 0)
                self.assertGreater(result["peaks_bytes"]["startup"]["gpu_samples"], 0)
                self.assertIsNotNone(result["load_s"])
                self.assertEqual(result["peaks_bytes"]["inference"]["samples"], 0)
                self.assertIn("No inference GPU memory", result["measurement_error"])

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
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "cli"
            code, sample = self.phase_case(output, {}, inference_gpu=False)
            with (
                patch.dict(sys.modules, {"psutil": psutil, "pynvml": nvml}),
                patch(
                    "measure_process.sample_process_tree",
                    side_effect=lambda _psutil, _nvml, devices, pid: sample(
                        devices, pid
                    ),
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
