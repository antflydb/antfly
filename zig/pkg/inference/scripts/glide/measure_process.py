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

"""Run one fresh benchmark process and sample host PSS/RSS and NVML GPU bytes.

Usage: measure_process.py --out evidence/run -- command [args...]
The child output is preserved in .log and lifecycle/memory metadata in .json.
The sampler covers loading and inference separately (including warmup).
"""

import argparse
import json
import os
import subprocess
import threading
import time
from pathlib import Path


def gpu_inventory(nvml):
    """Enumerate physical GPUs independently of CUDA_VISIBLE_DEVICES."""
    return [
        (
            handle,
            {
                "uuid": nvml.nvmlDeviceGetUUID(handle),
                "name": nvml.nvmlDeviceGetName(handle),
            },
        )
        for handle in (
            nvml.nvmlDeviceGetHandleByIndex(i) for i in range(nvml.nvmlDeviceGetCount())
        )
    ]


def process_gpu_bytes(nvml, devices, pid):
    usage = {}
    for handle, info in devices:
        for process in nvml.nvmlDeviceGetComputeRunningProcesses(handle):
            if process.pid != pid:
                continue
            value = process.usedGpuMemory
            if value is None or value == 2**64 - 1 or value < 0:
                raise RuntimeError(
                    f"GPU memory unavailable for PID {pid} on {info['uuid']}"
                )
            usage[info["uuid"]] = usage.get(info["uuid"], 0) + value
    return usage


def observed_gpu_identity(devices, observed):
    used = [info for _, info in devices if info["uuid"] in observed]
    if not used:
        raise RuntimeError(
            "No process GPU memory observed; refusing an unverified CUDA measurement"
        )
    return {"gpu": ", ".join(info["name"] for info in used), "gpu_devices": used}


def main():
    import psutil
    import pynvml

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    if not command:
        parser.error("a command is required")
    args.out.parent.mkdir(parents=True, exist_ok=True)
    pynvml.nvmlInit()
    devices = gpu_inventory(pynvml)
    observed = set()
    sampling_error = None
    started = time.monotonic()
    phase = {"loaded": False, "load_s": None}
    peaks = {
        name: {"rss": 0, "pss": 0, "gpu": 0, "samples": 0}
        for name in ("startup", "inference")
    }
    process = subprocess.Popen(
        command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1
    )
    proc = psutil.Process(process.pid)
    with args.out.with_suffix(".log").open("w") as log:

        def drain():
            for line in process.stdout:
                log.write(line)
                log.flush()
                if "loaded backend=" in line or '"event": "load"' in line:
                    phase["loaded"] = True
                    phase["load_s"] = time.monotonic() - started

        reader = threading.Thread(target=drain)
        reader.start()
        while process.poll() is None:
            peak = peaks["inference" if phase["loaded"] else "startup"]
            try:
                mem = proc.memory_full_info()
                peak["rss"] = max(peak["rss"], mem.rss)
                peak["pss"] = max(peak["pss"], mem.pss)
                usage = process_gpu_bytes(pynvml, devices, process.pid)
                observed.update(uuid for uuid, value in usage.items() if value > 0)
                peak["gpu"] = max(peak["gpu"], sum(usage.values()))
                peak["samples"] += 1
            except psutil.NoSuchProcess:
                break
            except (RuntimeError, pynvml.NVMLError, psutil.Error, OSError) as exc:
                sampling_error = str(exc)
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
                break
            time.sleep(0.02)
        reader.join()
    result = {
        "command": command,
        "pid": process.pid,
        "exit_code": process.wait(),
        "elapsed_s": time.monotonic() - started,
        "load_s": phase["load_s"],
        "peaks_bytes": peaks,
        "gpu_inventory": [info for _, info in devices],
        "cuda_visible_devices": os.environ.get("CUDA_VISIBLE_DEVICES"),
        "driver": pynvml.nvmlSystemGetDriverVersion(),
        "sampling_interval_s": 0.02,
        "environment": {
            k: v
            for k, v in os.environ.items()
            if k.startswith(
                (
                    "ANTFLY_GLIDE",
                    "ANTFLY_INFERENCE_CUDA_",
                    "ANTFLY_CUDA_ENABLE_",
                    "OMP_",
                    "MKL_",
                    "TRITON_",
                    "TOKENIZERS_",
                )
            )
        },
    }
    try:
        if sampling_error:
            raise RuntimeError(sampling_error)
        result.update(observed_gpu_identity(devices, observed))
    except RuntimeError as exc:
        result["measurement_error"] = str(exc)
    pynvml.nvmlShutdown()
    args.out.with_suffix(".json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result), flush=True)
    if result.get("measurement_error"):
        raise RuntimeError(result["measurement_error"])
    raise SystemExit(result["exit_code"])


if __name__ == "__main__":
    main()
