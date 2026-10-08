#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Snapshot the inspected Transformers EmbeddingGemma2 sampling policy.
Adapted from Hugging Face Transformers (Apache-2.0), observed 2026-10-08:
https://github.com/huggingface/transformers/blob/main/src/transformers/models/embedding_gemma2/video_processing_embedding_gemma2.py
Uses Python float/int operations and NumPy's documented linspace step/end-point
construction. --numpy verifies these fixtures with NumPy itself when installed.
No dependency download is required for the checked-in fixture tests.
"""

import argparse
import ast
from types import SimpleNamespace
import hashlib
import json
import random
from pathlib import Path


def reference(total, source_fps, duration, fps, cap, overflow):
    if fps is not None and (source_fps is None or duration is None):
        fps = None
    if fps is None:
        indexes = list(range(total))
    else:
        step = source_fps / fps
        count = max(1, int(duration * fps))
        indexes = [min(total - 1, int(i * step)) for i in range(count)]
    if overflow is not None and len(indexes) > cap:
        if overflow == "truncate":
            indexes = indexes[:cap]
        else:
            # np.linspace(0, n-1, cap, dtype=int) at nonnegative endpoints:
            # compute the float step, multiply, fix last endpoint, floor to int.
            positions = (
                [0]
                if cap == 1
                else [int(i * ((len(indexes) - 1) / (cap - 1))) for i in range(cap - 1)]
                + [len(indexes) - 1]
            )
            indexes = [indexes[i] for i in positions]
    return indexes


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--numpy", action="store_true")
    args = parser.parse_args()
    cases = [
        (60, 30.0, 2.0, 1.0, 32, "uniform"),
        (1800, 30.0, 60.0, 1.0, 32, "uniform"),
        (1800, 30.0, 60.0, 1.0, 32, "truncate"),
        (15, 30.0, 0.5, 1.0, 32, "uniform"),
        (1, 30.0, 1 / 30.0, 1.0, 32, "uniform"),
        (1001, 29.97002997002997, 33.4, 1.0, 7, "uniform"),
        (5, None, None, 1.0, 3, "uniform"),
        (7, None, None, None, 1, "uniform"),
        (7, None, None, None, 3, "truncate"),
        (7, 7.0, 1.0, 14.0, 32, "uniform"),
        (42, 10.0, 4.2, 0.7, 32, "uniform"),
        (1200, 30.0, 40.0, 1.0, 1, "uniform"),
        (100, 10.0, 10.0, 1.0, None, None),
    ]
    rng = random.Random(20261008)
    for _ in range(64):
        total = rng.randint(1, 30_000)
        source_fps = rng.choice([23.976, 29.97002997002997, 30.0, 60.0, 120.0])
        cases.append(
            (
                total,
                source_fps,
                total / source_fps,
                rng.choice([0.1, 0.7, 1.0, 2.0, 24.0]),
                rng.choice([1, 7, 32, 64]),
                rng.choice(["uniform", "truncate"]),
            )
        )
    output = []
    snapshot = (
        Path(__file__).resolve().parents[1] / "testdata/reference/hf_sample_frames.py"
    )
    if args.numpy:
        import numpy as np

        tree = ast.parse(snapshot.read_text())
        method = tree.body[0].body[0]
        method.returns = None
        for argument in method.args.args:
            argument.annotation = None
        logger = SimpleNamespace(warning_once=lambda *args: None)
        namespace = dict(np=np, logger=logger)
        exec(
            compile(ast.Module(body=[method], type_ignores=[]), str(snapshot), "exec"),
            namespace,
        )
        upstream_sample = namespace["sample_frames"]
    for total, source_fps, duration, fps, cap, overflow in cases:
        indexes = reference(total, source_fps, duration, fps, cap, overflow)
        if args.numpy:
            upstream = upstream_sample(
                SimpleNamespace(),
                SimpleNamespace(
                    total_num_frames=total, fps=source_fps, duration=duration
                ),
                fps=fps,
                max_frames=cap,
                overflow_strategy=overflow,
            )
            assert upstream.tolist() == indexes
        output.append(
            dict(
                total_frames=total,
                source_fps=source_fps,
                duration_seconds=duration,
                fps=fps,
                max_frames=cap,
                overflow=overflow,
                indexes=indexes,
            )
        )
    path = Path(__file__).resolve()
    manifest = dict(
        policy="embeddinggemma2-frame-index-v1",
        source_url="https://github.com/huggingface/transformers/blob/main/src/transformers/models/embedding_gemma2/video_processing_embedding_gemma2.py",
        observed="2026-10-08",
        reference_snapshot_sha256=hashlib.sha256(snapshot.read_bytes()).hexdigest(),
        generator_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
        numpy_version=np.__version__ if args.numpy else None,
        upstream_snapshot_executed=args.numpy,
        provenance="Observed upstream method pinned by source-snapshot SHA-256; remote commit lookup unavailable due DNS. NumPy verification executes the unmodified snapshot method body.",
        cases=output,
    )
    (path.parents[1] / "testdata/sampling-oracle.json").write_text(
        json.dumps(manifest, indent=2) + "\n"
    )
    print(f"Generated {len(output)} frame-selection reference cases")


if __name__ == "__main__":
    main()
