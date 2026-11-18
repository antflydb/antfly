# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Raw-media CPU preprocessing + native PJRT CUDA towers/encoder/readback."""

import argparse
import json
import os
import select
import struct
import subprocess
import time
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import torch
from media_inputs import prepare, processor
from memory import MemorySampler


def read_exact(process, n, timeout=1200):
    result = bytearray()
    deadline = time.monotonic() + timeout
    while len(result) < n:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("PJRT media response timed out")
        if not select.select([process.stdout], [], [], min(remaining, 1))[0]:
            if process.poll() is not None:
                raise RuntimeError("PJRT media worker exited; see native.log")
            continue
        part = os.read(process.stdout.fileno(), n - len(result))
        if not part:
            raise EOFError("PJRT media response truncated")
        result.extend(part)
    return bytes(result)


def feed_weights(process, manifest, path):
    with path.open("rb") as f:
        length = struct.unpack("<Q", f.read(8))[0]
        header = json.loads(f.read(length))
        base = 8 + length
        for spec in manifest["parameters"]:
            if "data" in spec:
                if spec["dtype"] != "float32":
                    raise ValueError("Unsupported synthetic parameter dtype")
                process.stdin.write(
                    np.asarray(spec["data"], dtype=np.float32).tobytes()
                )
            else:
                t = header[spec["source"]]
                if t["dtype"] != "BF16" or t["shape"] != spec["shape"]:
                    raise ValueError("Checkpoint parameter dtype/shape mismatch")
                start, end = t["data_offsets"]
                f.seek(base + start)
                remaining = end - start
                while remaining:
                    chunk = f.read(min(remaining, 4 << 20))
                    if not chunk:
                        raise EOFError("Truncated checkpoint")
                    process.stdin.write(chunk)
                    remaining -= len(chunk)
        process.stdin.flush()


def main():
    p = argparse.ArgumentParser(description=__doc__)
    for arg in ("runner", "plugin", "model", "manifest", "suite", "out"):
        p.add_argument("--" + arg, type=Path, required=True)
    p.add_argument("--iterations", type=int, default=8)
    p.add_argument("--warmups", type=int, default=3)
    p.add_argument("--external-memory-sampler", action="store_true")
    p.add_argument("--executable-cache", type=Path)
    a = p.parse_args()
    if not 1 <= a.iterations <= 100 or not 0 <= a.warmups <= 100:
        p.error("iterations must be 1..100 and warmups 0..100")
    if not 1 <= a.iterations <= 100 or not 0 <= a.warmups <= 100:
        p.error("iterations must be 1..100 and warmups 0..100")
    a.out.mkdir(parents=True, exist_ok=False)
    torch.set_num_threads(2)
    sampler = None
    if not a.external_memory_sampler:
        sampler = MemorySampler(
            SimpleNamespace(pid=os.getpid()), a.out / "memory.jsonl"
        )
        sampler.thread.start()
    process = None
    try:
        proc = processor(a.model)
        manifest = json.loads(a.manifest.read_text())
        suite = json.loads(a.suite.read_text())
        with (a.out / "native.log").open("w") as log:
            process = subprocess.Popen(
                [
                    str(a.runner.resolve()),
                    str(a.plugin.resolve()),
                    str(a.manifest.resolve()),
                ]
                + ([str(a.executable_cache.resolve())] if a.executable_cache else []),
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=log,
                bufsize=0,
            )
            # Unbuffered pipe writes can be short: use the buffered writer for
            # bulk weights and request payloads; stdout remains descriptor-based.
            import io

            process.stdin = io.BufferedWriter(process.stdin, buffer_size=1 << 20)
            feed_weights(process, manifest, a.model / "model.safetensors")
            if read_exact(process, 6) != b"READY\n":
                raise RuntimeError("Invalid native readiness frame")
            by_name = {c["name"]: c for c in suite["cases"]}
            results = []
            for index, program in enumerate(manifest["cases"]):
                case = by_name[program["id"]]
                samples = []
                vectors = []
                preprocess = []
                transfer = []
                execute = []
                for rep in range(a.warmups + a.iterations):
                    started = time.perf_counter()
                    values = prepare(proc, case, a.suite.parent)
                    prepared = time.perf_counter()
                    payload = bytearray(struct.pack("<I", index))
                    for spec in program["inputs"]:
                        array = values[spec["name"]].numpy()
                        if (
                            list(array.shape) != spec["shape"]
                            or str(array.dtype) != spec["dtype"]
                        ):
                            raise ValueError("Runtime processor geometry changed")
                        payload.extend(array.tobytes())
                    process.stdin.write(payload)
                    process.stdin.flush()
                    response = read_exact(process, 16 + 768 * 4, timeout=300)
                    ended = time.perf_counter()
                    receive_ns, execute_ns = struct.unpack("<QQ", response[:16])
                    vector = np.frombuffer(response[16:], dtype="<f4").copy()
                    if (
                        not np.isfinite(vector).all()
                        or abs(np.linalg.norm(vector) - 1) > 0.001
                    ):
                        raise ValueError("Invalid media vector")
                    if rep >= a.warmups:
                        samples.append(ended - started)
                        preprocess.append(prepared - started)
                        transfer.append(receive_ns / 1e9)
                        execute.append(execute_ns / 1e9)
                        vectors.append(vector.tolist())
                    print(
                        case["name"],
                        rep,
                        round((ended - started) * 1000, 3),
                        flush=True,
                    )
                results.append(
                    {
                        "name": case["name"],
                        "samples_seconds": samples,
                        "preprocess_seconds": preprocess,
                        "native_receive_upload_seconds": transfer,
                        "native_execute_readback_seconds": execute,
                        "vectors": vectors,
                        "input_tokens": [len(case["token_ids"])] * len(samples),
                    }
                )
                (a.out / "results.json").write_text(
                    json.dumps(
                        {
                            "scope": "local encoded file read, official CPU decode/processor/tokenizer, IPC serialization/transfer, native PJRT CUDA towers, shared encoder and synchronized normalized vector readback; excludes startup and request cleanup",
                            "precision": "BF16 weights with official FP32 boundaries",
                            "torch_threads": 2,
                            "warmups": a.warmups,
                            "iterations": a.iterations,
                            "cases": results,
                        },
                        indent=2,
                    )
                )
            process.stdin.write(struct.pack("<I", 0xFFFFFFFF))
            process.stdin.flush()
            process.stdin.close()
            if process.wait(timeout=30):
                raise RuntimeError("PJRT media worker exited with failure")
    finally:
        if process is not None and process.poll() is None:
            process.kill()
            process.wait()
        if sampler is not None:
            (a.out / "memory.json").write_text(json.dumps(sampler.stop(), indent=2))


if __name__ == "__main__":
    main()
