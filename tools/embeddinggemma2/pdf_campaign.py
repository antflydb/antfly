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

"""Paired real-PDF renderer/resident-handoff campaign; no downloads or retries."""

import argparse
import hashlib
import json
import math
import os
import platform
import subprocess
from pathlib import Path


def sha256(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def compare_vectors(reference, candidate, threshold=0.9999):
    left = reference["output"]
    right = candidate["output"]
    if len(left) != len(right) or not left:
        raise ValueError("Missing or differently sized embedding outputs")
    results = []
    for x, y in zip(left, right):
        key = (x["document"], x["page"])
        if key != (y["document"], y["page"]):
            raise ValueError("Document/page order changed")
        u, v = x["values"], y["values"]
        if len(u) != len(v) or len(u) != 768:
            raise ValueError("Embedding shape changed")
        if not all(math.isfinite(value) for value in u + v):
            raise ValueError("Nonfinite embedding")
        uu = sum(z * z for z in u)
        vv = sum(z * z for z in v)
        if uu == 0 or vv == 0:
            raise ValueError("Zero embedding")
        cosine = sum(a * b for a, b in zip(u, v)) / math.sqrt(uu * vv)
        if not math.isfinite(cosine) or cosine < threshold:
            raise ValueError(f"Embedding parity failed: {key}: {cosine}")
        results.append({"document": key[0], "page": key[1], "cosine": cosine})
    return results


def validate_report(report, expected_pages=None):
    if expected_pages is not None and report["items"] != expected_pages:
        raise ValueError("Page selection changed")
    if not report["samples"]:
        raise ValueError("Missing measured samples")
    if report.get("scope") == "pipeline":
        identities = [
            {"document": v["document"], "page": v["page"]} for v in report["output"]
        ]
        if identities != report["items"]:
            raise ValueError("Missing, duplicated, or reordered page outputs")
    for sample in report["samples"]:
        if sample["gpu_pages"] + sample["cpu_pages"] != sample["pages"]:
            raise ValueError("Page execution accounting changed")
        if sample["pages"] > 4 or sample["retained_raster_bytes"] > 256 * 1024 * 1024:
            raise ValueError("Wave exceeds admission bounds")
        if not math.isfinite(sample["seconds"]) or sample["seconds"] <= 0:
            raise ValueError("Invalid timing sample")
    for page in report["pixel_parity"]:
        if page["max_error"] > 1:
            raise ValueError("GPU pixel parity failed")


def write_json(path, value):
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2) + "\n")
    temporary.replace(path)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--model", type=Path, required=True)
    parser.add_argument("--pdf", type=Path, action="append", required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--batches", default="1,2,4")
    parser.add_argument("--warmups", type=int, default=2)
    parser.add_argument("--iterations", type=int, default=10)
    parser.add_argument("--max-pages", type=int, default=12)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--dpi", type=int, default=150)
    parser.add_argument("--max-dimension", type=int, default=1536)
    parser.add_argument("--timeout", type=int, default=1200)
    parser.add_argument(
        "--machine-state", choices=["unknown", "idle", "loaded"], default="unknown"
    )
    parser.add_argument("--backend", choices=["metal", "native"], default="metal")
    parser.add_argument("--gpu", choices=["on", "off"], default="on")
    args = parser.parse_args()
    batches = [int(b) for b in args.batches.split(",")]
    if not batches or any(b not in (1, 2, 4) for b in batches):
        parser.error("batches must be selected from 1,2,4")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    metadata = {
        "version": 1,
        "binary": {"path": str(args.binary.resolve()), "sha256": sha256(args.binary)},
        "model_files": [
            {"path": str(p.relative_to(args.model)), "sha256": sha256(p)}
            for p in sorted(args.model.rglob("*"))
            if p.is_file() and p.suffix in (".safetensors", ".json")
        ],
        "inputs": [{"path": str(p.resolve()), "sha256": sha256(p)} for p in args.pdf],
        "platform": platform.platform(),
        "processor": platform.processor(),
        "logical_cpu_count": os.cpu_count(),
        "machine_state": args.machine_state,
        "configuration": {
            k: str(v) if isinstance(v, Path) else v
            for k, v in vars(args).items()
            if k != "pdf"
        },
    }
    write_json(args.output_dir / "manifest.json", metadata)

    def run_case(name, renderer, resident, scope, batch, warmups, iterations):
        key = f"{scope}-{name}-b{batch}"
        report_path = args.output_dir / f"{key}.json"
        command = [
            str(args.binary.resolve()),
            "--model",
            str(args.model.resolve()),
            "--output",
            str(report_path.resolve()),
            "--scope",
            scope,
            "--renderer",
            renderer,
            "--resident",
            resident,
            "--backend",
            args.backend,
            "--batch",
            str(batch),
            "--workers",
            str(args.workers),
            "--warmups",
            str(warmups),
            "--iterations",
            str(iterations),
            "--max-pages",
            str(args.max_pages),
            "--dpi",
            str(args.dpi),
            "--max-dimension",
            str(args.max_dimension),
        ]
        for pdf in args.pdf:
            command += ["--pdf", str(pdf.resolve())]
        print(f"Running {key}", flush=True)
        with (args.output_dir / f"{key}.log").open("w") as log:
            subprocess.run(
                command, stdout=log, stderr=log, check=True, timeout=args.timeout
            )
        report = json.loads(report_path.read_text())
        validate_report(report)
        return key, report

    if args.gpu == "on":
        # Pixel correctness preflight runs before any expensive model cases.
        # Its timing is not included in the measured campaign summary.
        run_case("preflight-gpu", "gpu", "on", "render", max(batches), 0, 1)
    cases = {}
    # All processes run sequentially. Alternate resident A/B order per batch;
    # external machine activity is explicitly separate from our scheduling.
    for batch_index, batch in enumerate(batches):
        variants = [("cpu-host", "cpu", "off"), ("cpu-resident", "cpu", "on")]
        if args.gpu == "on":
            variants.append(("gpu-resident", "gpu", "on"))
        if batch_index % 2:
            variants.reverse()
        for name, renderer, resident in variants:
            for scope in ["render", "pipeline"]:
                if scope == "render" and name == "cpu-host":
                    continue
                key, report = run_case(
                    name,
                    renderer,
                    resident,
                    scope,
                    batch,
                    args.warmups,
                    args.iterations,
                )
                cases[key] = report
    if sha256(args.binary) != metadata["binary"]["sha256"]:
        raise ValueError("Benchmark binary changed during the campaign")
    comparisons = []
    reference = cases[f"pipeline-cpu-host-b{batches[0]}"]
    for key, report in cases.items():
        validate_report(report, reference["items"])
        entry = {
            "case": key,
            "pages_per_second": report["pages_per_second"],
            "p50_seconds": report["p50_seconds"],
            "p95_seconds": report["p95_seconds"],
            "gpu_pages": sum(s["gpu_pages"] for s in report["samples"]),
            "cpu_pages": sum(s["cpu_pages"] for s in report["samples"]),
        }
        if report["scope"] == "pipeline":
            entry["parity"] = compare_vectors(reference, report)
            paired = cases[f"pipeline-cpu-host-b{report['batch']}"]
            entry["paired_throughput_ratio"] = (
                report["pages_per_second"] / paired["pages_per_second"]
            )
        comparisons.append(entry)
    write_json(
        args.output_dir / "summary.json",
        {
            "metadata": metadata,
            "cases": comparisons,
            "qualification": "Local development observations; GPU painter includes CPU parsing/codec decoding, host output copy, and explicit unsupported-page CPU fallback. Stage times enqueue work and are not isolated GPU kernel times.",
        },
    )
    print(f"Complete: {args.output_dir / 'summary.json'}", flush=True)


if __name__ == "__main__":
    main()
