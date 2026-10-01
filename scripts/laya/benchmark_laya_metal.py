#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Paired Laya legacy/resident Metal qualification using the same test harness.

The binary must contain `laya finetuned export` and its benchmark mode. The
baseline defaults to the same binary with residency disabled, isolating the
execution-path change. Raw samples, process logs, hashes, and paging counters
are retained. A failed process or paging-contaminated window fails qualification.
"""

import argparse
import hashlib
import json
import math
import os
import re
import signal
import subprocess
import time
from pathlib import Path


def run_window(command, env, log, timeout):
    # /usr/bin/time launches the actual GPU process. Killing only the wrapper
    # would leave that process running and contaminate subsequent measurements.
    with subprocess.Popen(
        command,
        env=env,
        stdout=log,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    ) as process:
        try:
            return process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait()
            raise


def run_pair(modes, measure, paging_retries, rejected):
    """Retry both sides of a paging-contaminated pair; never retry bad outputs."""
    for attempt in range(paging_retries + 1):
        records = []
        for mode in modes:
            record = measure(mode, attempt)
            records.append(record)
            if not record["passed"]:
                break
        if len(records) == len(modes) and all(r["passed"] for r in records):
            return records
        rejected.append({"attempt": attempt, "runs": records})
        failed = records[-1]
        deltas = failed.get("paging_delta", {})
        paging_only = (
            failed.get("measurement_valid") is True
            and set(deltas) == {"Pageouts", "Swapouts"}
            and all(v >= 0 for v in deltas.values())
            and any(v > 0 for v in deltas.values())
        )
        if not paging_only or attempt == paging_retries:
            raise RuntimeError(f"benchmark failed: {failed.get('artifact_prefix')}")
    raise AssertionError("unreachable")


def digest(path):
    h = hashlib.sha256()
    with open(path, "rb") as stream:
        for chunk in iter(lambda: stream.read(4 * 1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def paging():
    text = subprocess.check_output(["vm_stat"], text=True)
    return {
        key: int(re.search(rf"^{key}:\s+(\d+)", text, re.MULTILINE)[1])
        for key in ("Pageouts", "Swapouts")
    }


def percentile(samples, fraction):
    ordered = sorted(samples)
    return ordered[max(0, math.ceil(len(ordered) * fraction) - 1)]


def validate_measurement(payload, mode, batch, profile, expected_samples):
    """Fail closed on an absent route, malformed timings, or transfer violations."""
    if not isinstance(payload, dict):
        return False
    samples = payload.get("samples_ns")
    if (
        payload.get("batch") != batch
        or payload.get("mixed") is not (profile == "mixed")
        or not isinstance(samples, list)
        or len(samples) != expected_samples
        or not all(
            type(n) in (int, float) and math.isfinite(n) and n > 0 for n in samples
        )
    ):
        return False
    resident = payload.get("resident")
    if mode == "legacy":
        return resident is None
    if not isinstance(resident, dict) or resident.get("prepared") is not True:
        return False
    if any(
        resident.get(k) != 0
        for k in (
            "activation_host_accesses",
            "intermediate_readbacks",
            "host_fallbacks",
            "cached_activation_bytes",
        )
    ):
        return False
    return (
        resident.get("requests", 0) >= expected_samples + 10
        and resident.get("physical_download_calls") == resident.get("requests")
        and resident.get("physical_download_bytes", 0) > 0
        and resident.get("physical_download_bytes")
        == resident.get("output_readback_bytes")
        and resident.get("physical_upload_bytes", 0) > 0
        and resident.get("physical_upload_bytes") == resident.get("input_upload_bytes")
    )


def reference_hashes(root):
    files = [root / "reference.json"]
    files.extend(sorted((root / "model").glob("*.safetensors")))
    files.extend(sorted((root / "model").glob("*.json")))
    if not any(p.suffix == ".safetensors" for p in files):
        raise ValueError("reference has no safetensors model")
    return {str(p.relative_to(root)): digest(p) for p in files}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--baseline-binary", type=Path)
    parser.add_argument("--reference", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--pairs", type=int, default=3)
    parser.add_argument(
        "--batches", type=int, nargs="+", default=[1, 2, 4, 8, 16, 64, 128]
    )
    parser.add_argument(
        "--profiles", nargs="+", choices=("fixed", "mixed"), default=["fixed", "mixed"]
    )
    parser.add_argument("--timeout", type=float, default=1800)
    parser.add_argument("--paging-retries", type=int, default=2)
    args = parser.parse_args()
    if (
        args.pairs < 1
        or any(b < 1 or b > 512 for b in args.batches)
        or args.timeout <= 0
        or not 0 <= args.paging_retries <= 10
    ):
        parser.error("invalid run geometry or timeout")
    args.output.mkdir(parents=True, exist_ok=False)
    binaries = {
        "legacy": (args.baseline_binary or args.binary).resolve(),
        "resident": args.binary.resolve(),
    }
    hashes = {name: digest(path) for name, path in binaries.items()}
    input_hashes = reference_hashes(args.reference)
    harness_hash = digest(Path(__file__))
    results = []
    rejected = []
    try:
        for batch in args.batches:
            for profile in args.profiles:
                for pair in range(args.pairs):
                    modes = (
                        ("legacy", "resident")
                        if pair % 2 == 0
                        else ("resident", "legacy")
                    )

                    def measure(mode, attempt, batch=batch, profile=profile, pair=pair):
                        suffix = f"-retry{attempt}" if attempt else ""
                        prefix = (
                            args.output
                            / f"{profile}-b{batch}-pair{pair}-{mode}{suffix}"
                        )
                        env = os.environ.copy()
                        env.update(
                            {
                                "ANTFLY_LAYA_METAL": "1",
                                "ANTFLY_LAYA_EXPORT_REFERENCE": str(
                                    args.reference.resolve()
                                ),
                                "ANTFLY_LAYA_BENCH_BATCH": str(batch),
                                "ANTFLY_LAYA_BENCH_PROFILE": profile,
                                "ANTFLY_LAYA_BENCH_SAMPLES": "100"
                                if batch <= 8
                                else "25",
                                "ANTFLY_LAYA_METAL_RESIDENT": "1"
                                if mode == "resident"
                                else "0",
                                "TERMITE_METAL_DISABLE_LAYA_RESIDENT": "0"
                                if mode == "resident"
                                else "1",
                            }
                        )
                        before = paging()
                        began = time.monotonic()
                        command = [
                            "/usr/bin/time",
                            "-l",
                            str(binaries[mode]),
                            "--test-filter",
                            "laya finetuned export",
                        ]
                        record = {
                            "batch": batch,
                            "profile": profile,
                            "pair": pair,
                            "mode": mode,
                            "attempt": attempt,
                            "artifact_prefix": str(prefix),
                            "command": command,
                            "binary_sha256": hashes[mode],
                            "paging_before": before,
                            "environment": {
                                k: v
                                for k, v in env.items()
                                if k.startswith(("ANTFLY_LAYA_", "TERMITE_METAL_"))
                            },
                        }
                        try:
                            with prefix.with_suffix(".log").open("w") as log:
                                record["exit_code"] = run_window(
                                    command, env, log, args.timeout
                                )
                        except subprocess.TimeoutExpired:
                            record["timeout"] = True
                        record["elapsed_seconds"] = time.monotonic() - began
                        after = paging()
                        record["paging_delta"] = {
                            k: after[k] - before[k] for k in before
                        }
                        log_text = prefix.with_suffix(".log").read_text()
                        lines = log_text.splitlines()
                        for label, key in (
                            ("maximum resident set size", "peak_rss_bytes"),
                            ("peak memory footprint", "peak_footprint_bytes"),
                        ):
                            match = re.search(
                                rf"^\s*(\d+)\s+{label}$", log_text, re.MULTILINE
                            )
                            if match:
                                record[key] = int(match[1])
                        payloads = [
                            json.loads(line.split("LAYA_BENCH_JSON ", 1)[1])
                            for line in lines
                            if "LAYA_BENCH_JSON " in line
                        ]
                        record["measurements"] = payloads
                        record["measurement_valid"] = (
                            record.get("exit_code") == 0
                            and len(payloads) == 1
                            and validate_measurement(
                                payloads[0],
                                mode,
                                batch,
                                profile,
                                100 if batch <= 8 else 25,
                            )
                            and digest(binaries[mode]) == hashes[mode]
                        )
                        record["passed"] = record["measurement_valid"] and all(
                            v == 0 for v in record["paging_delta"].values()
                        )
                        if record["passed"]:
                            samples = payloads[0]["samples_ns"]
                            record["p50_ns"] = percentile(samples, 0.5)
                            record["p95_ns"] = percentile(samples, 0.95)
                            record["decisions_per_second"] = (
                                batch * len(samples) * 1e9 / sum(samples)
                            )
                        prefix.with_suffix(".json").write_text(
                            json.dumps(record, indent=2) + "\n"
                        )
                        return record

                    results.extend(
                        run_pair(modes, measure, args.paging_retries, rejected)
                    )
        comparisons = []
        for batch in args.batches:
            for profile in args.profiles:
                grouped = {
                    mode: [
                        r
                        for r in results
                        if r["batch"] == batch
                        and r["profile"] == profile
                        and r["mode"] == mode
                    ]
                    for mode in binaries
                }
                medians = {
                    mode: {
                        metric: percentile([r[metric] for r in records], 0.5)
                        for metric in ("p50_ns", "p95_ns", "decisions_per_second")
                    }
                    for mode, records in grouped.items()
                }
                ratios = {
                    metric: medians["resident"][metric] / medians["legacy"][metric]
                    for metric in medians["legacy"]
                }
                comparisons.append(
                    {
                        "batch": batch,
                        "profile": profile,
                        "ratios": ratios,
                        "passed": ratios["p50_ns"] <= 1.05
                        and ratios["p95_ns"] <= 1.10
                        and (batch <= 8 or ratios["decisions_per_second"] >= 0.95),
                    }
                )
        interactive = [c["ratios"]["p50_ns"] for c in comparisons if c["batch"] <= 8]
        geometric_ratio = (
            math.exp(sum(map(math.log, interactive)) / len(interactive))
            if interactive
            else None
        )
        # Check identities even for throughput-only invocations, whose overall
        # `passed` is false because they lack interactive profile coverage.
        identities_valid = input_hashes == reference_hashes(
            args.reference
        ) and harness_hash == digest(Path(__file__))
        summary = {
            "format": "antfly-laya-metal-benchmark/v1",
            "passed": bool(
                args.pairs >= 3
                and {1, 2, 4, 8}.issubset(args.batches)
                and {"fixed", "mixed"}.issubset(args.profiles)
                and identities_valid
                and geometric_ratio is not None
                and geometric_ratio <= 0.9
                and all(r["passed"] for r in results)
                and all(c["passed"] for c in comparisons)
            ),
            "measurement_runs_valid": identities_valid
            and all(r["passed"] for r in results),
            "identities_valid": identities_valid,
            "profile_regression_gates_passed": all(c["passed"] for c in comparisons),
            "interactive_p50_ratio": geometric_ratio,
            "comparisons": comparisons,
            "runs": results,
            "input_sha256": input_hashes,
            "binary_sha256": hashes,
            "harness_sha256": harness_hash,
            "rejected_attempts": rejected,
        }
    except (
        OSError,
        ValueError,
        RuntimeError,
        KeyError,
        TypeError,
        subprocess.SubprocessError,
    ) as exc:
        summary = {
            "format": "antfly-laya-metal-benchmark/v1",
            "passed": False,
            "error": str(exc),
            "runs": results,
            "input_sha256": input_hashes,
            "binary_sha256": hashes,
            "harness_sha256": harness_hash,
            "rejected_attempts": rejected,
        }
    (args.output / "result.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(
        json.dumps(
            {
                k: v
                for k, v in summary.items()
                if k not in ("runs", "comparisons", "rejected_attempts")
            },
            indent=2,
        )
    )
    return 0 if summary["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
