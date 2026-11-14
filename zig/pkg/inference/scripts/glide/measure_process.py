#!/usr/bin/env python3
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
import psutil
import pynvml


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    if not command:
        parser.error("a command is required")
    args.out.parent.mkdir(parents=True, exist_ok=True)
    pynvml.nvmlInit()
    handle = pynvml.nvmlDeviceGetHandleByIndex(0)
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
                for gpu_proc in pynvml.nvmlDeviceGetComputeRunningProcesses(handle):
                    if gpu_proc.pid == process.pid:
                        peak["gpu"] = max(peak["gpu"], gpu_proc.usedGpuMemory)
                peak["samples"] += 1
            except psutil.NoSuchProcess:
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
        "gpu": pynvml.nvmlDeviceGetName(handle),
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
    args.out.with_suffix(".json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result), flush=True)
    raise SystemExit(result["exit_code"])


if __name__ == "__main__":
    main()
