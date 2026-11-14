#!/usr/bin/env python3
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

"""GPU attribution regressions; no CUDA hardware or optional packages needed."""

from types import SimpleNamespace
import unittest

from benchmark_decide_hotel_cuda import process_gpu_bytes


class GpuMemoryTests(unittest.TestCase):
    devices = [(0, {"uuid": "GPU-first"}), (1, {"uuid": "GPU-second"})]

    def nvml(self, processes):
        return SimpleNamespace(
            NVML_VALUE_NOT_AVAILABLE=2**64 - 1,
            nvmlDeviceGetComputeRunningProcesses=lambda handle: processes[handle],
        )

    def test_visible_device_remapping_and_foreign_process(self):
        nvml = self.nvml(
            [
                [SimpleNamespace(pid=999, usedGpuMemory=10000)],
                [SimpleNamespace(pid=42, usedGpuMemory=300)],
            ]
        )
        self.assertEqual(
            process_gpu_bytes(nvml, self.devices, {42}), {"GPU-second": 300}
        )

    def test_parent_and_children_on_multiple_devices(self):
        nvml = self.nvml(
            [
                [SimpleNamespace(pid=42, usedGpuMemory=100)],
                [
                    SimpleNamespace(pid=43, usedGpuMemory=200),
                    SimpleNamespace(pid=44, usedGpuMemory=300),
                ],
            ]
        )
        self.assertEqual(
            process_gpu_bytes(nvml, self.devices, {42, 43, 44}),
            {"GPU-first": 100, "GPU-second": 500},
        )

    def test_unavailable_memory_is_not_a_valid_measurement(self):
        for value in (None, 2**64 - 1, -1):
            nvml = self.nvml([[], [SimpleNamespace(pid=42, usedGpuMemory=value)]])
            with self.assertRaisesRegex(RuntimeError, "GPU memory unavailable"):
                process_gpu_bytes(nvml, self.devices, {42})


if __name__ == "__main__":
    unittest.main()
