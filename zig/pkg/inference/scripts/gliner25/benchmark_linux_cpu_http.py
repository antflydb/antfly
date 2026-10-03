#!/usr/bin/env python3
"""Paired warm HTTP comparisons for pinned GLiNER extraction/decision cases.

The case file contains {"cases": [{"id", "path", "request", "expected",
"confidence_tolerance"}], "artifacts": [local file paths]}.
Run on each Linux qualification host with CPU-only servers already started.
The baseline, candidate, and optional same-precision reference receive identical
payloads. All public output fields are checked, except confidence/logit rounding.
This produces HTTP evidence, not automatic hardware/release qualification.
"""
import argparse
import json
import math
import sys
import time
from pathlib import Path
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from linux_cpu import host_provenance, sha256
import paired_benchmark


def strict_json(raw):
    def invalid(value):
        raise ValueError(f"non-finite JSON: {value}")
    def unique(pairs):
        out = {}
        for key, value in pairs:
            if key in out:
                raise ValueError(f"duplicate key: {key}")
            out[key] = value
        return out
    return json.loads(raw, parse_constant=invalid, object_pairs_hook=unique)


def require_equal(expected, actual, tolerance, path="output"):
    if isinstance(expected, dict):
        if not isinstance(actual, dict) or expected.keys() != actual.keys():
            raise ValueError(f"{path}: keys differ")
        for key, value in expected.items():
            require_equal(value, actual[key], tolerance, f"{path}.{key}")
    elif isinstance(expected, list):
        if not isinstance(actual, list) or len(expected) != len(actual):
            raise ValueError(f"{path}: lengths differ")
        for i, (left, right) in enumerate(zip(expected, actual)):
            require_equal(left, right, tolerance, f"{path}[{i}]")
    elif path.endswith((".confidence", ".logit", ".score", ".probability")):
        if (type(expected) not in (int, float) or type(actual) not in (int, float)
                or not math.isfinite(expected) or not math.isfinite(actual)
                or abs(expected - actual) > tolerance):
            raise ValueError(f"{path}: numerical mismatch")
    elif type(expected) is not type(actual) or expected != actual:
        raise ValueError(f"{path}: value differs")


def request(base, case, timeout):
    path = case["path"]
    if not path.startswith("/") or path.startswith("//"):
        raise ValueError("case path must be an absolute API path")
    payload = json.dumps(case["request"], allow_nan=False).encode()
    req = Request(base.rstrip("/") + path, data=payload,
                  headers={"Content-Type": "application/json"}, method="POST")
    start = time.perf_counter_ns()
    with urlopen(req, timeout=timeout) as response:
        raw = response.read(16 * 1024 * 1024 + 1)
        if response.status != 200 or len(raw) > 16 * 1024 * 1024:
            raise ValueError("unsuccessful or oversized response")
    duration = time.perf_counter_ns() - start
    output = strict_json(raw)
    require_equal(case["expected"], output, case["confidence_tolerance"])
    return {"duration_ns": duration, "output": output}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--cases", type=Path, required=True)
    p.add_argument("--baseline", required=True)
    p.add_argument("--candidate", required=True)
    p.add_argument("--reference")
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--warmup", type=int, default=3)
    p.add_argument("--pairs", type=int, default=20)
    p.add_argument("--timeout", type=float, default=300)
    args = p.parse_args()
    if not 0 <= args.warmup <= 10 or not 2 <= args.pairs <= 100 or not 0 < args.timeout <= 300:
        p.error("invalid sampling or timeout limits")
    arms = {"baseline": args.baseline, "candidate": args.candidate}
    if args.reference:
        arms["reference"] = args.reference
    for url in arms.values():
        parsed = urlsplit(url)
        if parsed.scheme != "http" or parsed.hostname not in ("localhost", "127.0.0.1", "::1"):
            p.error("use loopback HTTP servers on the same qualification host")
    manifest = strict_json(args.cases.read_bytes())
    cases = manifest["cases"]
    if not cases or len(cases) > 100 or len({c["id"] for c in cases}) != len(cases):
        p.error("need 1..100 cases with unique IDs")
    for case in cases:
        tolerance = case["confidence_tolerance"]
        if type(tolerance) not in (int, float) or not math.isfinite(tolerance) or not 0 <= tolerance <= 0.002:
            p.error("confidence_tolerance must preserve a qualified tolerance in 0..0.002")
    artifacts = {str(Path(f).resolve()): sha256(f) for f in manifest["artifacts"]}
    if not artifacts:
        p.error("pin server binaries and model files in artifacts")
    args.output.mkdir(parents=True, exist_ok=False)
    report = {"format_version": 1, "status": "running", "scope": "warm_http",
              "host": host_provenance(), "cases_sha256": sha256(args.cases),
              "artifacts": artifacts, "arms": arms, "warmup": args.warmup,
              "pairs": args.pairs, "performance_release_qualified": False,
              "samples": [], "comparisons": {}}
    try:
        for case in cases:
            for _ in range(args.warmup):
                for url in arms.values():
                    request(url, case, args.timeout)
            pairs = []
            reference_pairs = []
            for pair in range(args.pairs):
                order = list(arms) if pair % 2 == 0 else list(reversed(arms))
                row = {"case_id": case["id"], "pair": pair, "order": order}
                for arm in order:
                    row[arm] = request(arms[arm], case, args.timeout)
                report["samples"].append(row)
                pairs.append((row["candidate"]["duration_ns"], row["baseline"]["duration_ns"]))
                if args.reference:
                    reference_pairs.append((row["candidate"]["duration_ns"], row["reference"]["duration_ns"]))
            summary = {
                "candidate_ns": paired_benchmark.distribution(c for c, _ in pairs),
                "baseline_ns": paired_benchmark.distribution(b for _, b in pairs),
                "candidate_over_baseline": paired_benchmark.paired_log_ratio_ci(pairs, samples=2000),
            }
            if reference_pairs:
                summary["candidate_over_reference"] = paired_benchmark.paired_log_ratio_ci(reference_pairs, samples=2000)
            report["comparisons"][case["id"]] = summary
        if artifacts != {path: sha256(path) for path in artifacts} or sha256(args.cases) != report["cases_sha256"]:
            raise ValueError("artifacts changed during measurement")
        report["status"] = "complete"
    except BaseException as error:
        report["status"] = "failed"
        report["error"] = f"{type(error).__name__}: {error}"
        raise
    finally:
        (args.output / "report.json").write_text(json.dumps(report, indent=2, allow_nan=False) + "\n")
        paired_benchmark.write_evidence_manifest(args.output)
    print(args.output / "report.json")


if __name__ == "__main__":
    main()
