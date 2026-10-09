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

"""Run the standalone Decide-1B service benchmark with immutable provenance.

Build inference-bench-server separately with run_bounded_zig_build.py. This
supervisor never builds or relaxes admission guards and retains failed runs.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys
import time

import run_family_performance as perf


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--build-receipt", type=Path, required=True)
    parser.add_argument("--model-dir", type=Path, required=True)
    parser.add_argument("--backend", choices=("metal", "native"), required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--timeout", type=int, default=300)
    parser.add_argument(
        "--diagnostic-disable",
        action="append",
        default=[],
        choices=("qkv-rope", "add-norm"),
        help="same-binary fusion control; marks the receipt ineligible for acceptance",
    )
    args = parser.parse_args()
    binary, model, output = (
        p.resolve() for p in (args.binary, args.model_dir, args.output)
    )
    build = json.loads(args.build_receipt.read_text())
    if (
        not build.get("passed")
        or build["source"] != perf.source_identity()
        or build["binary_sha256"] != perf.sha256(binary)
    ):
        raise RuntimeError(
            "binary/source differs from successful build receipt; rebuild before measuring"
        )
    output.mkdir(parents=True, exist_ok=False)
    models = output / "models"
    models.mkdir()
    # Keep the model directory under the validated root; hard-link individual pinned files.
    linked = models / "model"
    linked.mkdir()
    for relative in (
        "model.safetensors",
        "config.json",
        "encoder_config/config.json",
        "tokenizer.json",
        "tokenizer_config.json",
        "special_tokens_map.json",
    ):
        src, dst = model / relative, linked / relative
        if src.exists():
            dst.parent.mkdir(parents=True, exist_ok=True)
            os.link(src, dst)
    source = perf.source_identity()
    digest = perf.sha256(binary)
    # Save the actual patch, not just a claim about HEAD, before launching.
    import subprocess

    (output / "source.patch").write_bytes(
        subprocess.check_output(
            ["git", "diff", "--binary", "HEAD", "--", *perf.SOURCE_PATHS], cwd=perf.ROOT
        )
    )
    for name in source["untracked_files_sha256"]:
        dest = output / "source-untracked" / name
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes((perf.ROOT / name).read_bytes())
    env = {
        k: v
        for k, v in os.environ.items()
        if not k.startswith(
            (
                "TERMITE_",
                "ANTFLY_TEST_",
                "ANTFLY_METAL_",
                "ANTFLY_GLINER_",
                "ANTFLY_GLINER25_",
                "ANTFLY_INFERENCE_",
            )
        )
    }
    env.update({key: "2" for key in perf.THREAD_VARIABLES})
    diagnostic_flags = {
        "qkv-rope": "TERMITE_METAL_DISABLE_GLINER_QKV_ROPE",
        "add-norm": "TERMITE_METAL_DISABLE_GLINER_ADD_NORM",
    }
    env.update({diagnostic_flags[name]: "1" for name in args.diagnostic_disable})
    env.update(
        ANTFLY_GLINER25_PERF_OUTPUT_DIR=str(output),
        ANTFLY_GLINER25_PERF_SOURCE_HEAD=source["head"],
        ANTFLY_GLINER25_PERF_SOURCE_DIFF_SHA256=source["tracked_diff_sha256"],
        ANTFLY_GLINER25_PERF_BINARY_SHA256=digest,
        ANTFLY_GLINER25_DECIDE_1B_HOLDOUT=str(
            perf.INFERENCE / "testdata/gliner25/family/decide_1b_short_holdout.json"
        ),
        ANTFLY_GLINER25_DECIDE_1B_CAPTURE=str(
            perf.INFERENCE / "testdata/gliner25/family/decide_1b_capture.json"
        ),
    )
    command = [
        "/usr/bin/time",
        "-l",
        "-o",
        str(output / "time.txt"),
        str(binary),
        "decide-bench",
        args.backend,
        str(models),
    ]
    receipt = dict(
        source=source,
        binary=str(binary),
        binary_sha256=digest,
        build_receipt=str(args.build_receipt.resolve()),
        command=command,
        started_utc=perf.utc_now(),
        host_before=perf.host_context(),
        status="running",
    )
    receipt.update(
        diagnostic_only=bool(args.diagnostic_disable),
        diagnostic_disabled_fusions=args.diagnostic_disable,
        performance_qualification=not bool(args.diagnostic_disable),
    )
    path = output / "receipt.json"
    path.write_text(json.dumps(receipt, indent=2) + "\n")
    started = time.monotonic()
    try:
        receipt["exit_code"] = perf.run_child(
            command, env, output / "process.log", args.timeout
        )
        receipt["resources"] = perf.parse_time((output / "time.txt").read_text())
        reports = [
            json.loads(p.read_text()) for p in sorted(output.glob("gliner25-*.json"))
        ]
        if receipt["exit_code"] != 0 or len(reports) != 24:
            raise RuntimeError("benchmark failed or incomplete; see process.log")
        expected = {
            (case, path)
            for case in (
                "described_prompt_choice",
                "choice_score_noul",
                "binary_short",
                "described_four_labels",
                "score_and_noul",
                "mixed_longer",
            )
            for path in ("direct", "http_handler", "loaded_pipeline", "encoder_head")
        }
        if {(r["case_id"], r["path"]) for r in reports} != expected:
            raise RuntimeError("unexpected benchmark cases")
        for report in reports:
            assert report["backend"] == args.backend
            assert report["binary_sha256"] == digest
            assert report["source_diff_sha256"] == source["tracked_diff_sha256"]
            assert report["measurement_scope"] == (
                "validated_production_allocator_service_latency"
                if report["path"] in ("direct", "http_handler")
                else "validated_production_allocator_pipeline_latency"
            )
            assert (
                report["fixture_allocator"]
                == "platform.processAllocator(smp_allocator)"
            )
            assert report["measured_samples"] == 20 and len(report["samples_ns"]) == 20
        receipt["reports"] = reports
        receipt["passed"] = True
    except Exception as exc:
        receipt.update(passed=False, error=repr(exc))
    receipt.update(
        status="finished",
        finished_utc=perf.utc_now(),
        wall_seconds=time.monotonic() - started,
        host_after=perf.host_context(),
        source_unchanged=source == perf.source_identity(),
    )
    receipt["binary_unchanged"] = digest == perf.sha256(binary)
    receipt["passed"] &= receipt["source_unchanged"] and receipt["binary_unchanged"]
    path.write_text(json.dumps(receipt, indent=2) + "\n")
    print(json.dumps({k: receipt[k] for k in ("passed", "wall_seconds", "status")}))
    return 0 if receipt["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
