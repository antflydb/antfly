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
import selectors
import signal
import subprocess
import time
from pathlib import Path


def gpu_inventory(nvml):
    """Enumerate physical GPUs independently of CUDA_VISIBLE_DEVICES."""
    devices = []
    for index in range(nvml.nvmlDeviceGetCount()):
        handle = nvml.nvmlDeviceGetHandleByIndex(index)
        devices.append(
            (
                handle,
                {
                    "uuid": nvml.nvmlDeviceGetUUID(handle),
                    "name": nvml.nvmlDeviceGetName(handle),
                },
            )
        )
    return devices


def process_gpu_bytes(nvml, devices, pids):
    if isinstance(pids, int):
        pids = {pids}
    usage = {}
    for handle, info in devices:
        for process in nvml.nvmlDeviceGetComputeRunningProcesses(handle):
            if process.pid not in pids:
                continue
            value = process.usedGpuMemory
            if value is None or value == 2**64 - 1 or value < 0:
                raise RuntimeError(
                    f"GPU memory unavailable for PID {process.pid} on {info['uuid']}"
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


def sample_process_tree(psutil, nvml, devices, pid):
    root = psutil.Process(pid)
    processes = [root, *root.children(recursive=True)]
    rss = pss = 0
    pids = set()
    for process in processes:
        try:
            memory = process.memory_full_info()
        except psutil.NoSuchProcess:
            continue
        pids.add(process.pid)
        rss += memory.rss
        pss += memory.pss
    return rss, pss, process_gpu_bytes(nvml, devices, pids)


def stop_process_group(process, grace):
    """Kill descendants even after the direct child has already exited."""

    def send(sig):
        try:
            os.killpg(process.pid, sig)
        except ProcessLookupError:
            pass

    send(signal.SIGTERM)
    deadline = time.monotonic() + grace
    while time.monotonic() < deadline:
        process.poll()  # Reap the group leader while waiting for its workers.
        try:
            os.killpg(process.pid, 0)
        except ProcessLookupError:
            break
        time.sleep(min(0.01, max(0, deadline - time.monotonic())))
    send(signal.SIGKILL)
    process.wait(timeout=grace)


def measure(command, output, nvml, sample, *, interval=0.02, grace=1.0):
    """Return and persist a receipt, including failures; no monitoring thread."""
    output = Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    result = {
        "command": command,
        "passed": False,
        "exit_code": None,
        "load_s": None,
        "sampling_target_interval_s": interval,
        "cuda_visible_devices": os.environ.get("CUDA_VISIBLE_DEVICES"),
        "environment": {
            name: value
            for name, value in os.environ.items()
            if name.startswith(
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
    peaks = {
        phase: {"rss": 0, "pss": 0, "gpu": None, "samples": 0, "gpu_samples": 0}
        for phase in ("startup", "inference")
    }
    result["peaks_bytes"] = peaks
    sample_times = []
    observed = set()
    process = None
    initialized = False
    error = None
    try:
        nvml.nvmlInit()
        initialized = True
        devices = gpu_inventory(nvml)
        result["gpu_inventory"] = [info for _, info in devices]
        result["driver"] = nvml.nvmlSystemGetDriverVersion()
        with (
            output.with_suffix(".log").open("wb") as log,
            selectors.DefaultSelector() as selector,
        ):
            process = subprocess.Popen(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
            result["pid"] = process.pid
            os.set_blocking(process.stdout.fileno(), False)
            selector.register(process.stdout, selectors.EVENT_READ)
            loaded = False
            tail = b""
            next_sample = time.monotonic()
            exit_deadline = None
            while True:
                now = time.monotonic()
                exited = process.poll() is not None
                if exited and exit_deadline is None:
                    exit_deadline = now + grace
                if exited and not selector.get_map():
                    break
                if exit_deadline is not None and now >= exit_deadline:
                    raise RuntimeError(
                        "Child exited but worker output pipe remained open"
                    )
                timeout = max(0, next_sample - now) if not exited else grace
                if exit_deadline is not None:
                    timeout = min(timeout, max(0, exit_deadline - now))
                for key, _ in selector.select(timeout):
                    data = os.read(key.fd, 65536)
                    if not data:
                        selector.unregister(key.fileobj)
                        continue
                    log.write(data)
                    log.flush()
                    text = tail + data
                    if not loaded and (
                        b"loaded backend=" in text or b'"event": "load"' in text
                    ):
                        loaded = True
                        result["load_s"] = time.monotonic() - started
                    tail = text[-64:]
                now = time.monotonic()
                if not exited and now >= next_sample:
                    phase = "inference" if loaded else "startup"
                    try:
                        rss, pss, usage = sample(devices, process.pid)
                    except ProcessLookupError:
                        if process.poll() is None:
                            raise
                    else:
                        peak = peaks[phase]
                        peak["rss"] = max(peak["rss"], rss)
                        peak["pss"] = max(peak["pss"], pss)
                        if any(value > 0 for value in usage.values()):
                            peak["gpu"] = max(peak["gpu"] or 0, sum(usage.values()))
                            peak["gpu_samples"] += 1
                            observed.update(
                                uuid for uuid, value in usage.items() if value > 0
                            )
                        peak["samples"] += 1
                        sample_times.append(now - started)
                    # Skip missed ticks rather than burst-sampling to catch up.
                    next_sample = now + interval
            result["exit_code"] = process.wait(timeout=grace)
            result.update(observed_gpu_identity(devices, observed))
            if result["load_s"] is None:
                raise RuntimeError(
                    "Missing load marker; inference phase was not observed"
                )
            if peaks["inference"]["gpu_samples"] == 0:
                raise RuntimeError(
                    "No inference GPU memory observed; inference peak is unavailable"
                )
    except Exception as exc:  # noqa: BLE001 - persist failures from injected or optional monitoring APIs
        error = str(exc)
    finally:
        if process is not None:
            try:
                stop_process_group(process, grace)
            except (OSError, subprocess.TimeoutExpired) as exc:
                error = error or str(exc)
            result["exit_code"] = process.poll()
            if process.stdout is not None:
                process.stdout.close()
        if initialized:
            try:
                nvml.nvmlShutdown()
            except Exception as exc:  # noqa: BLE001 - retain the receipt even if NVML teardown fails
                error = error or str(exc)
    gaps = [sample_times[i] - sample_times[i - 1] for i in range(1, len(sample_times))]
    result["sample_times_s"] = sample_times
    result["sampling_actual_interval_s"] = (
        {"min": min(gaps), "max": max(gaps), "mean": sum(gaps) / len(gaps)}
        if gaps
        else None
    )
    result["elapsed_s"] = time.monotonic() - started
    result["passed"] = error is None and result["exit_code"] == 0
    if error:
        result["measurement_error"] = error
    output.with_suffix(".json").write_text(json.dumps(result, indent=2) + "\n")
    return result


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

    def sample(devices, pid):
        try:
            return sample_process_tree(psutil, pynvml, devices, pid)
        except psutil.NoSuchProcess as exc:
            raise ProcessLookupError(str(exc)) from exc

    result = measure(command, args.out, pynvml, sample)
    print(json.dumps(result), flush=True)
    raise SystemExit(0 if result["passed"] else result["exit_code"] or 1)


if __name__ == "__main__":
    main()
