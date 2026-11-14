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

import unittest
from types import SimpleNamespace
from typing import ClassVar
from unittest.mock import patch

from measure_process import gpu_inventory, observed_gpu_identity, process_gpu_bytes


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

    def test_missing_observations_are_rejected(self):
        with self.assertRaisesRegex(RuntimeError, "No process GPU memory observed"):
            observed_gpu_identity(self.devices, set())


if __name__ == "__main__":
    unittest.main()
