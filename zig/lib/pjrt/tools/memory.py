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

"""Process-tree PSS plus per-process NVML sampling for isolated Linux runs."""

import json
import threading
import time
from pathlib import Path


class MemorySampler:
    def __init__(self, process, path):
        import psutil
        import pynvml

        pynvml.nvmlInit()
        self.psutil, self.nvml = psutil, pynvml
        self.handles = [
            pynvml.nvmlDeviceGetHandleByIndex(i)
            for i in range(pynvml.nvmlDeviceGetCount())
        ]
        self.process, self.path = process, path
        self.stop_event = threading.Event()
        self.peak_rss_bytes = self.peak_gpu_bytes = self.peak_pss_bytes = (
            self.peak_footprint_bytes
        ) = self.samples = 0
        self.errors = []
        self.thread = threading.Thread(target=self.run, daemon=True)

    def run(self):
        with self.path.open("w") as out:
            while not self.stop_event.is_set():
                pids, rss, pss = set(), 0, 0
                try:
                    root = self.psutil.Process(self.process.pid)
                    processes = [root, *root.children(recursive=True)]
                except self.psutil.NoSuchProcess:
                    processes = []
                for process in processes:
                    try:
                        pids.add(process.pid)
                        for line in (
                            Path(f"/proc/{process.pid}/smaps_rollup")
                            .read_text()
                            .splitlines()
                        ):
                            if line.startswith("Pss:"):
                                pss += int(line.split()[1]) * 1024
                            elif line.startswith("Rss:"):
                                rss += int(line.split()[1]) * 1024
                    except (FileNotFoundError, ProcessLookupError):
                        continue
                    except OSError as error:
                        self.errors.append(repr(error))
                try:
                    gpu = 0
                    for handle in self.handles:
                        for process in self.nvml.nvmlDeviceGetComputeRunningProcesses(
                            handle
                        ):
                            if process.pid in pids:
                                used = process.usedGpuMemory
                                if not isinstance(used, int) or not 0 <= used < (
                                    1 << 60
                                ):
                                    raise RuntimeError("NVML memory unavailable")
                                gpu += used
                    self.peak_gpu_bytes = max(self.peak_gpu_bytes, gpu)
                    self.peak_rss_bytes = max(self.peak_rss_bytes, rss)
                    self.peak_pss_bytes = max(self.peak_pss_bytes, pss)
                    self.peak_footprint_bytes = max(
                        self.peak_footprint_bytes, pss + gpu
                    )
                    self.samples += 1
                    out.write(
                        json.dumps(
                            {
                                "time": time.time(),
                                "rss_bytes": rss,
                                "pss_bytes": pss,
                                "gpu_bytes": gpu,
                                "footprint_bytes": pss + gpu,
                            }
                        )
                        + "\n"
                    )
                except Exception as error:  # noqa: BLE001 - invalidate every sampler failure
                    self.errors.append(repr(error))
                self.stop_event.wait(0.2)

    def stop(self):
        self.stop_event.set()
        self.thread.join()
        self.nvml.nvmlShutdown()
        return {
            "peak_rss_bytes": self.peak_rss_bytes,
            "peak_gpu_bytes": self.peak_gpu_bytes,
            "peak_pss_bytes": self.peak_pss_bytes,
            "peak_footprint_bytes": self.peak_footprint_bytes,
            "samples": self.samples,
            "errors": self.errors,
            "valid": self.samples > 0
            and self.peak_pss_bytes > 0
            and self.peak_gpu_bytes > 0
            and not self.errors,
            "method": "process-tree smaps_rollup PSS plus NVML; 0.2s polling",
        }
