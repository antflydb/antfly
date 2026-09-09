#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Check the complete Qwen latency campaign against QWEN_PERFORMANCE.md.

This checks performance evidence, not the separate CPU-reference qualification.
It consumes completed, unmodified paired reports and their resource reports.
"""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
import statistics
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent / "qwen3_embedding"))
from benchmark_qwen3_embedding_endpoint import (  # noqa: E402
    bootstrap_ratio_ci,
    percentile,
    sha256_file,
)

CASES = {
    "embedding": {
        "tokens_20", "tokens_256", "tokens_511", "tokens_2551", "tokens_4096",
        "tokens_8192", "tokens_20_511", "tokens_20_256_511_2551",
    },
    "ocr": {
        "sparse", "portrait", "landscape_table", "screenshot",
        "serial_2_pages", "serial_4_pages",
    },
}
PRIMARY = {"embedding": "tokens_8192", "ocr": "portrait"}
CONFIRMATION = {
    "embedding": "embedding-confirm-8192",
    "ocr": "ocr-confirm-portrait",
}
OCR_CONTROLS = {
    "q4": {"TERMITE_METAL_DISABLE_Q4_K_HIGH_ROW_MM": "1"},
    "q6": {"TERMITE_METAL_DISABLE_Q6_K_HIGH_ROW_MM": "1"},
    "decode_frame": {"TERMITE_METAL_ENABLE_QWEN3VL_FORWARD_DECODE_FRAME": "0"},
}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def identity(report):
    """Exclude PID/startup observations, but retain execution configuration."""
    return {
        key: report.get(key)
        for key in (
            "phase", "model", "model_sha256", "artifacts", "fixture_sha256",
            "runner_sha256", "order", "system",
        )
    } | {
        "servers": {
            label: {key: server[key] for key in (
                "executable_sha256", "argv", "environment",
            )}
            for label, server in report["servers"].items()
        }
    }


def check_resources(resource):
    limits = resource["limits"]
    require(0 < limits["max_rss_mib"] <= 8192
            and 15 <= limits["min_free_percent"] <= 100
            and limits["max_swap_growth_mib"] == 0
            and limits["min_disk_free_mib"] >= 1024,
            "resource guard limits were relaxed")
    require(resource["pass"] is True and resource["returncode"] == 0
            and resource["violation"] is None, "resource guard did not pass")
    require(resource["samples"] > 0, "resource guard has no samples")
    require(resource["peak_group_rss_mib"] <= 8192, "RSS exceeds 8 GiB")
    require(resource["min_free_percent"] >= 15, "free memory fell below 15%")
    require(resource["swapout_growth_mib"] == 0, "swapout growth is nonzero")
    require(resource["min_disk_free_mib"] >= 1024, "free disk fell below 1 GiB")
    require(resource["elapsed_seconds"] <= limits["timeout_seconds"],
            "resource deadline exceeded")


def check_case(case, phase, iterations):
    baseline = "reference" if phase == "ocr" else "baseline"
    pairs = case["pairs"]
    require(len(pairs) == iterations + 3, "incorrect total pair count")
    for i, pair in enumerate(pairs):
        require(pair["iteration"] == i and pair["warmup"] is (i < 3),
                "incorrect pair order or warmup marking")
        require(set(pair["ms"]) == {baseline, "candidate"}, "unpaired measurement")
        require(all(math.isfinite(ms) and ms > 0 for ms in pair["ms"].values()),
                "invalid latency sample")
    if phase == "embedding":
        require(all(pair["min_cosine"] >= 0.9999 for pair in pairs),
                "embedding parity failed")
        require(len({tuple(pair["case_ids"]) for pair in pairs}) == len(pairs),
                "embedding requests were recycled")
    samples = {
        label: [pair["ms"][label] for pair in pairs[3:]]
        for label in (baseline, "candidate")
    }
    require(case["samples_ms"] == samples, "samples differ from measured raw pairs")
    intervals = {
        statistic: bootstrap_ratio_ci(
            samples["candidate"], samples[baseline], 2000, 1729, statistic=statistic
        )
        for statistic in ("median", "p95")
    }
    for statistic, interval in intervals.items():
        require(case[statistic + "_speedup"] == interval,
                f"{statistic} interval does not reproduce")
    median = {label: statistics.median(values) for label, values in samples.items()}
    require(case["median_ms"] == median, "median does not reproduce")
    reduction = 100 * (1 - median["candidate"] / median[baseline])
    return {
        "id": case["id"], "pairs": iterations,
        "baseline_median_ms": median[baseline],
        "candidate_median_ms": median["candidate"],
        "median_reduction_percent": reduction,
        "baseline_p95_ms": percentile(samples[baseline], 0.95),
        "candidate_p95_ms": percentile(samples["candidate"], 0.95),
        "median_speedup": intervals["median"],
        "p95_speedup": intervals["p95"],
        "primary_win": reduction >= 5 and intervals["median"]["lower_95"] > 1,
        "confirmed_regression": (
            intervals["median"]["upper_95"] < 1 / 1.03
            or intervals["p95"]["upper_95"] < 1 / 1.05
        ),
    }


def check_campaign(directory):
    result = {"schema": "antfly.qwen_performance_gates.v1", "pass": False,
              "runs": {}, "errors": []}
    campaign_builds = None
    campaign_system = None
    for phase in CASES:
        expected_identity = None
        expected_outputs = {}
        server_pids = set()
        for label in (phase + "-run1", phase + "-run2", CONFIRMATION[phase]):
            try:
                path = directory / label / "report.json"
                resource_path = directory / (label + "-resources.json")
                report = json.loads(path.read_text())
                resource = json.loads(resource_path.read_text())
                check_resources(resource)
                require(report["schema"] == "antfly.qwen_paired_benchmark.v1"
                        and report["pass"] is True, "paired run is incomplete or failed")
                require("current_case" not in report, "paired run is unfinished")
                confirmation = label == CONFIRMATION[phase]
                iterations = report["iters"]
                require(report["phase"] == phase and report["warmup"] == 3
                        and isinstance(iterations, int)
                        and iterations >= (100 if confirmation else 20),
                        "wrong sampling protocol")
                expected_cases = {PRIMARY[phase]} if confirmation else CASES[phase]
                require({case["id"] for case in report["cases"]} == expected_cases
                        and len(report["cases"]) == len(expected_cases),
                        "missing, unexpected, or duplicate workloads")
                observed_identity = identity(report)
                if expected_identity is None:
                    expected_identity = observed_identity
                require(observed_identity == expected_identity,
                        "build, model, fixture, hardware, or configuration differs")
                require(set(report["servers"]) == {"baseline", "candidate"},
                        "missing paired server provenance")
                builds = {key: value["executable_sha256"]
                          for key, value in report["servers"].items()}
                if campaign_builds is None:
                    campaign_builds, campaign_system = builds, report["system"]
                require(builds == campaign_builds and report["system"] == campaign_system,
                        "builds or hardware changed between model families")
                for server in report["servers"].values():
                    require(server["environment"] == {
                        "TERMITE_EMBED_RESIDENT_FAIL_CLOSED": "1"
                    }, "non-default runtime or instrumentation enabled")
                    require(server["pid"] not in server_pids, "server process reused")
                    server_pids.add(server["pid"])
                cells = []
                for case in report["cases"]:
                    if phase == "ocr":
                        output = {key: case[key] for key in ("fixture", "golden")}
                        expected_outputs.setdefault(case["id"], output)
                        require(output == expected_outputs[case["id"]],
                                "OCR golden or image geometry changed between runs")
                    cells.append(check_case(case, phase, iterations))
                result["runs"][label] = {
                    "report_sha256": sha256_file(path),
                    "resource_sha256": sha256_file(resource_path), "cases": cells,
                }
                require(not any(cell["confirmed_regression"] for cell in cells),
                        "confirmed median or p95 regression")
                require(next(cell for cell in cells if cell["id"] == PRIMARY[phase])
                        ["primary_win"], "primary median win does not meet retention gate")
            except (OSError, ValueError, KeyError, TypeError, StopIteration) as exc:
                result["errors"].append(f"{label}: {type(exc).__name__}: {exc}")
    result["pass"] = not result["errors"]
    return result


def check_ocr_ablations(directory):
    """Require each OCR change to earn retention against the same executable."""
    result = {"schema": "antfly.qwen_ocr_ablation_gates.v1", "pass": False,
              "runs": {}, "errors": []}
    try:
        anchor = json.loads((directory / "ocr-run2/report.json").read_text())
        require(anchor["pass"] is True, "main OCR run did not pass")
        anchor_identity = identity(anchor)
        anchor_identity.pop("servers")
        golden = next(case for case in anchor["cases"] if case["id"] == "portrait")
        executable = anchor["servers"]["candidate"]["executable_sha256"]
    except (OSError, ValueError, KeyError, TypeError, StopIteration) as exc:
        result["errors"].append(f"main OCR evidence: {type(exc).__name__}: {exc}")
        return result
    pids = set()
    for control, setting in OCR_CONTROLS.items():
        for run in (1, 2):
            label = f"ocr-ablation-{control}-run{run}"
            try:
                path = directory / label / "report.json"
                resource_path = directory / (label + "-resources.json")
                report = json.loads(path.read_text())
                check_resources(json.loads(resource_path.read_text()))
                require(report["schema"] == "antfly.qwen_paired_benchmark.v1"
                        and report["pass"] is True and "current_case" not in report,
                        "ablation is incomplete or failed")
                require(report["phase"] == "ocr" and report["warmup"] == 3
                        and isinstance(report["iters"], int) and report["iters"] >= 20,
                        "wrong ablation sampling protocol")
                observed = identity(report)
                observed.pop("servers")
                require(observed == anchor_identity,
                        "ablation model, fixture, hardware, or runner differs")
                require(set(report["servers"]) == {"baseline", "candidate"},
                        "missing paired server provenance")
                for side, server in report["servers"].items():
                    expected_env = anchor["servers"][side]["environment"].copy()
                    if side == "baseline":
                        expected_env.update(setting)
                    require(server["environment"] == expected_env,
                            "ablation did not isolate the intended control")
                    require(server["executable_sha256"] == executable
                            and server["argv"] == anchor["servers"][side]["argv"],
                            "ablation executable or server arguments differ")
                    require(server["pid"] not in pids, "ablation server reused")
                    pids.add(server["pid"])
                require(len(report["cases"]) == 1
                        and report["cases"][0]["id"] == "portrait",
                        "ablation must contain the portrait workload")
                case = report["cases"][0]
                require(all(case[key] == golden[key] for key in ("fixture", "golden")),
                        "ablation golden or image geometry differs")
                cell = check_case(case, "ocr", report["iters"])
                result["runs"][label] = {
                    "report_sha256": sha256_file(path),
                    "resource_sha256": sha256_file(resource_path),
                    "disabled_control": setting, "cases": [cell],
                }
                require(cell["primary_win"], "individual change missed retention gate")
                require(not cell["confirmed_regression"],
                        "individual change has a confirmed p95 regression")
            except (OSError, ValueError, KeyError, TypeError, StopIteration) as exc:
                result["errors"].append(f"{label}: {type(exc).__name__}: {exc}")
    result["pass"] = not result["errors"]
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--campaign-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--ocr-ablations", action="store_true",
                        help="check the six individual OCR ablation runs")
    args = parser.parse_args()
    check = check_ocr_ablations if args.ocr_ablations else check_campaign
    report = check(args.campaign_dir)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"pass": report["pass"], "errors": report["errors"]}, indent=2))
    return 0 if report["pass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
