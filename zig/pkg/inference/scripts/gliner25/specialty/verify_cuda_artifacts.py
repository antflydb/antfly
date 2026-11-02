#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Check specialty SASS bodies for GB10, L4 and A100 without GPU hardware.

Run artifact regeneration first to establish source freshness. This check proves
image contents, not numerical correctness or performance on an absent device.
"""
import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess
import time

from qualify_http import PACKAGE, digest_file
from qualify_cuda_sanitizers import PII_KERNELS

KERNELS = (*PII_KERNELS, "termite_add_bias_relu_rows_f32", "termite_linear_f32",
           "termite_linear_f32_tiled")
IMAGES = (("a100", "sm_80", "inference_cuda_kernels.fatbin"),
          ("l4", "sm_89", "inference_cuda_kernels.fatbin"),
          ("gb10", "sm_120", "inference_cuda_kernels.fatbin"),
          ("l4", "sm_89", "inference_cuda_kernels_sm89.cubin"))


def check_image(tool, artifact, arch):
    command = [str(tool), "--dump-sass", "--gpu-architecture", arch,
               "--function", ",".join(KERNELS), str(artifact)]
    result = subprocess.run(command, capture_output=True, text=True, check=True, timeout=120)
    if not re.search(r"(?:arch\s*=\s*|code for\s+)" + arch + r"\b", result.stdout):
        raise ValueError(f"{artifact.name}: missing {arch} image")
    bodies = dict(re.findall(r"Function\s*:\s*(\w+)\s*\n(.*?)(?=\n\s*Function\s*:|\Z)",
                             result.stdout, re.DOTALL))
    instructions = {}
    for kernel in KERNELS:
        # Require real compiled bodies, not just symbol names or RET-only stubs.
        body = bodies.get(kernel, "")
        count = len(re.findall(r"/\*[0-9a-fA-F]+\*/\s+\S", body))
        # nvcc can pad an empty entry with several NOPs. Every required kernel
        # writes global output, so instruction count alone cannot bless a stub.
        if count < 5 or not re.search(r"\bSTG(?:\.|\s)", body):
            raise ValueError(f"{artifact.name}/{arch}: missing or stub kernel {kernel} ({count} instructions)")
        instructions[kernel] = count
    return {"command": command, "instruction_counts": instructions,
            "sass_sha256": hashlib.sha256(result.stdout.encode()).hexdigest()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cuobjdump", type=Path, default=Path("/usr/local/cuda/bin/cuobjdump"))
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    report = {"status": "failed", "checked_unix_seconds": time.time(), "images": [],
              "scope": "Local SASS contents only; GB10 loads compatible SM120 code. L4/A100 hardware not executed."}
    try:
        for target, arch, filename in IMAGES:
            artifact = PACKAGE / "src/ops/cuda/artifacts" / filename
            receipt = check_image(args.cuobjdump, artifact, arch)
            report["images"].append({"target": target, "arch": arch, "artifact": filename,
                                     "sha256": digest_file(artifact), **receipt})
        report["status"] = "passed"
    except (ValueError, OSError, subprocess.SubprocessError) as error:
        report["error"] = str(error)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"status": report["status"], "images": len(report["images"]),
                      "error": report.get("error")}))
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
