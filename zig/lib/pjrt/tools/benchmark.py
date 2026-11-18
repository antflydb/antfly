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

"""Measure a native PJRT model runner, retaining identities, logs and memory.

Python streams weights before timing; no Python frontend participates in forwards.
Set CUDA library/tool paths in the environment. XLA_FLAGS must be unset for the
native bounded allocator. Do not overlap this command with another GPU workload.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
from memory import MemorySampler


def identity(path):
    with path.open("rb") as f:
        return {
            "path": str(path.resolve()),
            "bytes": path.stat().st_size,
            "sha256": hashlib.file_digest(f, "sha256").hexdigest(),
        }


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--runner", type=Path, required=True)
    p.add_argument("--plugin", type=Path, required=True)
    p.add_argument("--manifest", type=Path, required=True)
    p.add_argument("--weights", type=Path, required=True)
    p.add_argument("--format", choices=["gguf", "safetensors"], required=True)
    p.add_argument("--out", type=Path, required=True)
    p.add_argument("--timeout", type=float, default=3600)
    args = p.parse_args()
    args.out.mkdir(parents=True, exist_ok=False)
    manifest = json.loads(args.manifest.read_text())
    active = subprocess.check_output(
        ["nvidia-smi", "--query-compute-apps=pid", "--format=csv,noheader"], text=True
    ).strip()
    if active:
        raise RuntimeError(f"GPU already has compute processes: {active}")
    if os.environ.get("XLA_FLAGS"):
        raise RuntimeError(
            "Unset XLA_FLAGS; retain compiler options in the manifest instead"
        )
    paths = [args.runner, args.plugin, args.manifest, args.weights]
    base = args.manifest.parent
    paths += [
        base / c[k]
        for c in manifest["cases"]
        for k in ("prefill", "decode")
        if c.get(k)
    ]
    if manifest.get("compile_options"):
        paths.append(base / manifest["compile_options"])
    provenance = {
        "files": [identity(path) for path in dict.fromkeys(paths)],
        "timing": "native pretokenized forwards and output readback; warm calls exclude load/compile",
        "memory": "native process tree only; offline weight feeder excluded",
        "gpu": subprocess.check_output(
            [
                "nvidia-smi",
                "--query-gpu=name,memory.total,driver_version",
                "--format=csv,noheader",
            ],
            text=True,
        ).strip(),
    }
    (args.out / "provenance.json").write_text(json.dumps(provenance, indent=2))
    script = Path(__file__).with_name(
        "stream_gguf.py" if args.format == "gguf" else "stream_safetensors.py"
    )
    with (
        (args.out / "stream.log").open("w") as feedlog,
        (args.out / "native.log").open("w") as log,
    ):
        feed = subprocess.Popen(
            [sys.executable, str(script), str(args.manifest), str(args.weights)],
            stdout=subprocess.PIPE,
            stderr=feedlog,
        )
        process = None
        sampler = None
        try:
            process = subprocess.Popen(
                [
                    str(args.runner.resolve()),
                    str(args.plugin.resolve()),
                    str(args.manifest),
                    str(args.out / "results.json"),
                ],
                stdin=feed.stdout,
                stdout=log,
                stderr=subprocess.STDOUT,
            )
            feed.stdout.close()
            sampler = MemorySampler(process, args.out / "memory.jsonl")
            sampler.thread.start()
            rc = process.wait(timeout=args.timeout)
            if rc:
                raise RuntimeError(f"Native runner exited {rc}; see native.log")
            if feed.wait(timeout=30):
                raise RuntimeError("Weight feeder failed; see stream.log")
        finally:
            if process is not None and process.poll() is None:
                process.kill()
                process.wait()
            if feed.poll() is None:
                feed.terminate()
                feed.wait()
            if sampler is not None:
                (args.out / "memory.json").write_text(
                    json.dumps(sampler.stop(), indent=2)
                )


if __name__ == "__main__":
    main()
