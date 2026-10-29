#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Freeze the original TL1/TL2 attention functions for independent qualification.

Run from the worktree root with --output <worktree evidence directory>/baseline.h.
Compile check_metal_ternary_attention.m with clang -O3 -fobjc-arc and
-DANTFLY_TERNARY_ATTENTION_BASELINE='"<absolute output path>"', linking Foundation,
Metal, MetalPerformanceShaders and Accelerate. Run without arguments for parity;
--benchmark-gpu and --benchmark-host enable the separate paired timing assays.
The caller must declare its timing protocol and retain raw samples before judging
performance. This preparer does not launch or retry measurements.
"""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess

PREFIX = "termite_metal_decode_runtime_"
STEMS = ("encode_attention_quantized_residual", "apply_attention_residual",
         "apply_attention_span_residual")
SOURCE = "zig/pkg/inference/src/backends/metal_kernels.m"


def prepare(root, revision):
    revision = subprocess.check_output(
        ["git", "rev-parse", "--verify", revision + "^{commit}"], cwd=root, text=True).strip()
    original = subprocess.check_output(["git", "show", revision + ":" + SOURCE], cwd=root).decode()
    functions = {}
    for stem in STEMS:
        for codec in ("tl1", "tl2"):
            name = PREFIX + stem + "_" + codec
            start = original.index("int " + name + "(")
            start = original.rfind("\n", 0, start) + 1
            end = original.index("\n}\n", start) + 3
            functions[name] = original[start:end]
    header, unfixed_functions = [], []
    for name, body in functions.items():
        # Keep source labels and profile categories identical to production.
        body = body.replace("__func__", '"' + name + '"')
        if "_apply_" in name:
            body = body.replace("int " + name + "(", "static int av_before_" + name[len(PREFIX):] + "(")
        if "apply_attention_residual_" in name:
            unfixed = body.replace("av_before_apply_attention_residual_", "av_unfixed_apply_attention_residual_")
            unfixed_functions.append(unfixed)
            old_return = "return command_buffer.status == MTLCommandBufferStatusCompleted ? 0 : -4;"
            assert body.count(old_return) == 1
            # Only repair copyback for fair output and timing comparison. Keep an
            # unfixed copy above so the checker must reproduce the original bug.
            body = body.replace(old_return,
                "if (command_buffer.status != MTLCommandBufferStatusCompleted) return -4;\n"
                "        if (!output_borrowed) memcpy(output, output_buffer.contents, hidden_bytes);\n"
                "        return 0;")
        header.append(body)
    return "\n".join(header + unfixed_functions), {"baseline": revision,
        "source_sha256": hashlib.sha256(original.encode()).hexdigest(),
        "functions": {name: hashlib.sha256(body.encode()).hexdigest()
                      for name, body in functions.items()},
        "correction": "successful direct-host output copyback only"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--baseline", default="02e4f2709b")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    header, receipt = prepare(args.root, args.baseline)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(header)
    receipt["header_sha256"] = hashlib.sha256(header.encode()).hexdigest()
    args.output.with_suffix(".json").write_text(json.dumps(receipt, indent=2) + "\n")


if __name__ == "__main__":
    main()
