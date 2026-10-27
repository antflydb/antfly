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

"""Compare installed OCR models through Antfly's encoded-image reader API.

Uses the existing opt-in checkpoint test as a common reader harness. Its
LIGHTONOCR3 log labels are historical; the model directory determines which
reader runs. No PyTorch dependency or execution is involved.
"""

import argparse
from datetime import datetime
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import signal
import statistics
import subprocess

from benchmark_corpus import cer, native_report, normalize, plain_text


def digest(path):
    hashed = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            hashed.update(chunk)
    return hashed.hexdigest()


def checkpoint(path):
    weights = sorted(list(path.glob("*.safetensors")) + list(path.glob("*.gguf")))
    return {
        "directory": str(path),
        "weight_bytes": sum(p.stat().st_size for p in weights),
        "weight_files": [{"name": p.name, "bytes": p.stat().st_size} for p in weights],
        "metadata": {
            name: json.loads((path / name).read_text())
            for name in (
                "config.json",
                "preprocessor_config.json",
                "model_manifest.json",
                "source_revision.json",
            )
            if (path / name).is_file()
        },
    }


def hardware():
    result = {
        "processor": platform.processor(),
        "system": platform.system(),
        "machine": platform.machine(),
    }
    if platform.system() == "Darwin":
        result.update(
            processor=subprocess.check_output(
                ["/usr/sbin/sysctl", "-n", "machdep.cpu.brand_string"], text=True
            ).strip(),
            physical_memory_bytes=int(
                subprocess.check_output(
                    ["/usr/sbin/sysctl", "-n", "hw.memsize"], text=True
                )
            ),
        )
    elif platform.system() == "Linux":
        if hasattr(os, "sched_getaffinity"):
            result["cpu_affinity"] = sorted(os.sched_getaffinity(0))
        capacities = {}
        for cpu in Path("/sys/devices/system/cpu").glob("cpu[0-9]*/cpu_capacity"):
            try:
                capacities[cpu.parent.name] = int(cpu.read_text())
            except (OSError, ValueError):
                pass
        if capacities:
            result["cpu_capacities"] = capacities
        memory = Path("/proc/meminfo")
        if memory.is_file():
            for line in memory.read_text().splitlines():
                if line.startswith("MemTotal:"):
                    result["physical_memory_bytes"] = int(line.split()[1]) * 1024
        try:
            result["nvidia_smi"] = subprocess.check_output(
                [
                    "nvidia-smi",
                    "--query-gpu=name,driver_version,compute_cap",
                    "--format=csv,noheader",
                ],
                text=True,
                timeout=15,
            ).strip()
        except (OSError, subprocess.SubprocessError) as error:
            result["nvidia_smi_error"] = str(error)
        for name, query in (
            (
                "gpu_telemetry",
                "--query-gpu=temperature.gpu,power.draw,clocks.sm,utilization.gpu",
            ),
            (
                "gpu_compute_processes",
                "--query-compute-apps=pid,process_name,used_gpu_memory",
            ),
        ):
            try:
                result[name] = (
                    subprocess.check_output(
                        ["nvidia-smi", query, "--format=csv,noheader"],
                        text=True,
                        timeout=15,
                    )
                    .strip()
                    .splitlines()
                )
            except (OSError, subprocess.SubprocessError) as error:
                result[name + "_error"] = str(error)
    return result


def run_case(args, label, model, case, image, round_index, output_dir):
    env = os.environ.copy()
    for key in list(env):
        if key.startswith("TERMITE_METAL_DISABLE_LIGHTONOCR3") or key in (
            "TERMITE_METAL_LIGHTONOCR3_BF16_ACTIVATIONS",
            "ANTFLY_INFERENCE_FLORENCE_DISABLE_KV_CACHE",
            "TERMITE_SERVER_GENERATE_TIMING",
        ):
            del env[key]
    env.update(
        ANTFLY_TEST_LIGHTONOCR3_MODEL_DIR=str(model),
        ANTFLY_TEST_LIGHTONOCR3_IMAGE_PATH=str(image),
        ANTFLY_TEST_LIGHTONOCR3_MAX_TOKENS=str(case["max_new_tokens"]),
        ANTFLY_TEST_LIGHTONOCR3_WARM_RUNS=str(args.warm_runs),
        ANTFLY_TEST_LIGHTONOCR3_MEMORY_LIMIT_GIB="13",
        ANTFLY_TEST_LIGHTONOCR3_SHARED_LIMIT_GIB="24",
        ANTFLY_INFERENCE_PREFERRED_BACKEND="metal",
        ANTFLY_INFERENCE_REQUIRED_BACKEND="metal",
        ANTFLY_BENCH_LIGHTONOCR3="1",
    )
    command = [str(args.native_test), "--test-filter", "LightOnOCR3 real checkpoint"]
    if platform.system() == "Darwin":
        command = ["/usr/bin/time", "-l", *command]
    log_path = output_dir / f"round{round_index}-{label}-{case['name']}.txt"
    timed_out = False
    with log_path.open("w") as log:
        process = subprocess.Popen(
            command,
            env=env,
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        try:
            process.wait(timeout=args.timeout_seconds)
        except subprocess.TimeoutExpired:
            timed_out = True
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
    raw = log_path.read_text()
    entry = {
        "round": round_index,
        "model": label,
        "case": case["name"],
        "image_sha256": digest(image),
        "original_image_bytes": image.stat().st_size,
        "max_new_tokens": case["max_new_tokens"],
        "exit_code": process.returncode,
        "timed_out": timed_out,
        "log": log_path.name,
    }
    if process.returncode != 0:
        entry["status"] = (
            "memory_admission_denied"
            if "admission denied" in raw and "ResourceTemporarilyUnavailable" in raw
            else "resource_limit_exceeded"
            if "FAIL (ResourceLimitExceeded)" in raw
            else "timeout"
            if timed_out
            else "failed"
        )
        entry["diagnostic_tail"] = raw[-6000:]
        return entry
    result = native_report(raw)
    if len(result["runs"]) != args.warm_runs:
        raise RuntimeError(f"Incomplete warm samples: {log_path}")
    for run in result["runs"]:
        run["required_fields"] = {
            field: normalize(field) in normalize(plain_text(run["text"]))
            for field in case["required"]
        }
        run["plain_text_cer"] = cer(case["expected"], plain_text(run["text"]))
    peak = re.search(r"(\d+)\s+maximum resident set size", raw)
    entry.update(
        status="ok",
        result=result,
        peak_process_rss_bytes=int(peak[1]) if peak else None,
    )
    return entry


def summarize(entries, labels, expected_cases=None, expected_samples=None):
    summary = {}
    for label in labels:
        good = [e for e in entries if e["model"] == label and e["status"] == "ok"]
        samples = [r["seconds"] for e in good for r in e["result"]["runs"]]
        if not samples:
            summary[label] = {"status": "unavailable"}
            continue
        cases = {}
        for name in dict.fromkeys(e["case"] for e in good):
            selected = [e for e in good if e["case"] == name]
            runs = [r for e in selected for r in e["result"]["runs"]]
            times = [r["seconds"] for r in runs]
            cases[name] = {
                "samples": len(times),
                "median_seconds": statistics.median(times),
                "range_seconds": [min(times), max(times)],
                "serial_pages_per_second": len(times) / sum(times),
                "required_fields": {
                    field: all(r["required_fields"][field] for r in runs)
                    for field in runs[0]["required_fields"]
                },
                "all_required_fields_every_run": all(
                    all(r["required_fields"].values()) for r in runs
                ),
                "plain_text_cer_median": statistics.median(
                    r["plain_text_cer"] for r in runs
                ),
                "texts": sorted({r["text"] for r in runs}),
                "cold_request_seconds": [
                    e["result"]["cold_request_seconds"] for e in selected
                ],
                "peak_process_rss_bytes": [
                    e["peak_process_rss_bytes"] for e in selected
                ],
            }
        summary[label] = {
            "status": "ok"
            if expected_cases is None
            or (
                len(cases) == expected_cases
                and all(c["samples"] == expected_samples for c in cases.values())
            )
            else "partial",
            "completed_cases": sorted(cases),
            "warm_samples": len(samples),
            "warm_total_seconds": sum(samples),
            "mean_seconds_per_page": statistics.mean(samples),
            "serial_pages_per_second": len(samples) / sum(samples),
            "cases": cases,
        }
    return summary


def comparable_workloads(summary):
    available = {name: data for name, data in summary.items() if data.get("cases")}
    workloads = {}
    for name, selected in (
        (
            "complete_four_page_models",
            {m: v for m, v in available.items() if v["status"] == "ok"},
        ),
        ("common_cases_all_available_models", available),
    ):
        if not selected:
            continue
        cases = sorted(set.intersection(*(set(v["cases"]) for v in selected.values())))
        comparison = {"cases": cases, "models": {}}
        for model, data in selected.items():
            count = sum(data["cases"][c]["samples"] for c in cases)
            seconds = sum(
                data["cases"][c]["samples"]
                / data["cases"][c]["serial_pages_per_second"]
                for c in cases
            )
            if count:
                comparison["models"][model] = {
                    "warm_samples": count,
                    "mean_seconds_per_page": seconds / count,
                    "serial_pages_per_second": count / seconds,
                }
        workloads[name] = comparison
    return workloads


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--native-test", type=Path, required=True)
    parser.add_argument("--florence-dir", type=Path, required=True)
    parser.add_argument("--lighton08-dir", type=Path, required=True)
    parser.add_argument("--lighton4-dir", type=Path)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--warm-runs", type=int, default=3)
    parser.add_argument("--rounds", type=int, default=2)
    parser.add_argument("--timeout-seconds", type=int, default=180)
    args = parser.parse_args()
    if min(args.warm_runs, args.rounds, args.timeout_seconds) < 1:
        parser.error("warm-runs, rounds, and timeout-seconds must be positive")
    args.native_test = args.native_test.resolve(strict=True)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    models = {
        "florence2-base-q4_k": args.florence_dir,
        "lightonocr3-0.8b-bf16": args.lighton08_dir,
    }
    if args.lighton4_dir:
        models["lightonocr3-4b-bf16"] = args.lighton4_dir
    models = {name: path.resolve(strict=True) for name, path in models.items()}
    here = Path(__file__).resolve().parent
    cases = json.loads((here / "corpus/cases.json").read_text())["cases"]
    report = {
        "created_at": datetime.now().astimezone().isoformat(),
        "platform": platform.platform(),
        "hardware": hardware(),
        "worktree_revision": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], text=True, cwd=here
        ).strip(),
        "native_executable_sha256": digest(args.native_test),
        "models": {name: checkpoint(path) for name, path in models.items()},
        "warm_runs_per_case_per_round": args.warm_runs,
        "rounds": args.rounds,
        "process_limit_gib": 13,
        "shared_limit_gib": 24,
        "notes": [
            "Antfly Node.readEncodedImagesReportedDirect: includes preprocessing, inference, output handling; excludes HTTP, PDF rendering, embeddings, indexing.",
            "Serial single-page requests with resident weights during warm samples; pages/s is requests divided by total warm seconds, not concurrent serving throughput.",
            "Each case/model/round gets a new process and a separately reported cold request; OS file caches are not flushed.",
            "Model order reverses each round; models never execute concurrently in this harness. Other machine workloads are uncontrolled.",
            "Identical original page PNGs and output budgets; model-native resize/tokenization differs. Florence Q4_K and LightOn BF16 are different precision/size configurations.",
            "Null prompt selects each model's default OCR behavior. The shared checkpoint test retains historical LIGHTONOCR3 log labels for all readers.",
            "Synthetic smoke pages and required-field/CER checks are not a representative OCR accuracy evaluation.",
            "Peak process RSS is a separate whole-process maximum, not a complete measure of GPU/unified-memory use.",
            "Aggregates exclude failed requests; comparable_workloads separates complete-corpus results from cases completed by all available models.",
        ],
        "runs": [],
    }
    denied = set()
    for round_index in range(args.rounds):
        order = list(models)
        if round_index % 2:
            order.reverse()
        for label in order:
            if label in denied:
                continue
            for case in cases:
                print(
                    f"round={round_index} model={label} case={case['name']}", flush=True
                )
                entry = run_case(
                    args,
                    label,
                    models[label],
                    case,
                    (here / "corpus" / case["image"]).resolve(strict=True),
                    round_index,
                    args.output_dir,
                )
                report["runs"].append(entry)
                report["summary"] = summarize(
                    report["runs"], models, len(cases), args.rounds * args.warm_runs
                )
                report["comparable_workloads"] = comparable_workloads(report["summary"])
                (args.output_dir / "report.json").write_text(
                    json.dumps(report, indent=2) + "\n"
                )
                print(f"  {entry['status']}", flush=True)
                if entry["status"] == "memory_admission_denied":
                    denied.add(label)
                    break


if __name__ == "__main__":
    main()
