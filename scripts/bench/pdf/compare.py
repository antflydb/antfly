"""Run paired remote-PDF benchmarks without treating failed work as a speedup."""

import argparse
import json
import math
import re
import statistics
import subprocess
import sys
from pathlib import Path


def read_json(path, default=None):
    return json.loads(path.read_text()) if path.exists() else default


def save(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def output_signature(result):
    fields = (
        "unit_count",
        "chunk_count",
        "ocr_attempted_count",
        "ocr_selected_count",
        "ocr_failed_count",
    )
    indexes = result.get("indexes", [])
    if isinstance(indexes, dict):
        indexes = indexes.get("indexes") or indexes.get("items") or []
    vectors = [
        (row.get("status") or {}).get("searchable_vectors")
        for row in indexes
        if ((row.get("config") or {}).get("name") or row.get("name"))
        == "document_vectors"
    ]
    manifests = result.get("manifests", {})
    if not manifests or len(vectors) != 1 or vectors[0] is None:
        raise ValueError("missing artifact/vector coverage")
    documents = {}
    for key, manifest in sorted(manifests.items()):
        if any(manifest.get(field) is None for field in fields):
            raise ValueError(f"missing artifact counts: {key}")
        documents[key] = {field: manifest[field] for field in fields}
    return {
        "documents": documents,
        "pages": result["pages"],
        "document_count": result["documents"],
        "vectors": vectors[0],
        "unit_text_sha256": result["unit_text_sha256"],
        "unit_render_geometry": result["unit_render_geometry"],
    }


def comparable_pair(before, after, trials):
    errors = []
    for label, run in (("main", before), ("pr", after)):
        if run["returncode"] != 0 or len(run["results"]) != trials:
            errors.append(f"{label}: run failed or incomplete")
        if any(not row.get("passed") for row in run["results"]):
            errors.append(f"{label}: structural gate failed")
        if not run.get("metal_confirmed"):
            errors.append(f"{label}: Metal selection not confirmed for invoked models")
    for field in ("models", "table_config", "server_config"):
        if before.get(field) is None or before.get(field) != after.get(field):
            errors.append(f"mismatched or missing {field}")
    for field in (
        "selected",
        "mode",
        "suite",
        "batch",
        "circus_revision",
        "read_profile",
        "reader_batch_size",
    ):
        if field not in before.get("provenance", {}) or before["provenance"].get(
            field
        ) != after.get("provenance", {}).get(field):
            errors.append(f"mismatched or missing provenance.{field}")
    if before.get("provenance", {}).get("read_profile"):
        errors.append("profiling is enabled")
    if not errors:
        for i, (left, right) in enumerate(zip(before["results"], after["results"])):
            try:
                if output_signature(left) != output_signature(right):
                    errors.append(
                        f"trial {i}: output counts, text hashes or geometry differ"
                    )
                if (
                    left.get("unit_text_sha256") is None
                    or right.get("unit_text_sha256") is None
                    or left.get("unit_render_geometry") is None
                    or right.get("unit_render_geometry") is None
                ):
                    errors.append(f"trial {i}: missing retained-text verification")
                for row in (left, right):
                    if not math.isfinite(row["seconds"]) or row["seconds"] <= 0:
                        errors.append(f"trial {i}: invalid elapsed time")
            except (KeyError, ValueError) as exc:
                errors.append(f"trial {i}: {exc}")
    return errors


def summarize(pairs, trials):
    report = {"pairs": [], "timing_comparable": True, "timings": None}
    for pair in pairs:
        errors = comparable_pair(pair["main"], pair["pr"], trials)
        for subject in ("main", "pr"):
            for field in ("binary_sha256", "revision"):
                actual = pair[subject].get("provenance", {}).get(field)
                if actual is None or actual != pairs[0][subject].get(
                    "provenance", {}
                ).get(field):
                    errors.append(f"{subject}: missing or changed {field} across pairs")
            if pair[subject].get("models") != pairs[0][subject].get("models"):
                errors.append(f"{subject}: model files changed across pairs")
        report["pairs"].append({"order": pair["order"], "errors": errors})
        report["timing_comparable"] &= not errors
    if not pairs:
        report["timing_comparable"] = False
    if report["timing_comparable"]:
        timings = {}
        for phase in ("first_process_trial", "warm_process_trials"):
            samples = {}
            for subject in ("main", "pr"):
                values = []
                for pair in pairs:
                    rows = pair[subject]["results"]
                    selected = rows[:1] if phase == "first_process_trial" else rows[1:]
                    if selected:
                        values.append(
                            statistics.median(row["seconds"] for row in selected)
                        )
                samples[subject] = values
            if samples["main"] and samples["pr"]:
                before = statistics.median(samples["main"])
                after = statistics.median(samples["pr"])
                timings[phase] = {
                    "per_process_median_seconds": samples,
                    "main_seconds": before,
                    "pr_seconds": after,
                    "speedup_main_over_pr": before / after,
                    "elapsed_change_percent": (after / before - 1) * 100,
                    "paired_speedup_ratios": [
                        left / right
                        for left, right in zip(samples["main"], samples["pr"])
                    ],
                }
        report["timings"] = timings
    report["limitations"] = [
        "Structural counts do not prove OCR or retrieval accuracy.",
        "Filesystem caches and other host activity are not controlled.",
        "First-process trials are not cold-filesystem trials; setup is reported separately.",
        "Do not discard failed or unequal-output pairs to claim a speedup.",
    ]
    return report


def run_subject(args, out, index, subject):
    binary = getattr(args, f"{subject}_binary").resolve(strict=True)
    revision = getattr(args, f"{subject}_revision")
    name = f"{args.name}-{index:02d}-{subject}"
    command = [
        sys.executable,
        str(Path(__file__).with_name("benchmark.py")),
        "run",
        "--work-dir",
        str(args.work_dir),
        "--circus-dir",
        str(args.circus_dir),
        "--binary",
        str(binary),
        "--revision",
        revision,
        "--name",
        name,
        "--suite",
        args.suite,
        "--mode",
        args.mode,
        "--batch",
        "--verify-unit-text",
        "--trials",
        str(args.trials),
        "--timeout",
        str(args.timeout),
        "--port",
        str(args.port),
    ]
    if args.reader_batch_size is not None:
        command.extend(["--reader-batch-size", str(args.reader_batch_size)])
    print(f"Running {name}", flush=True)
    with (out / f"{name}.log").open("w") as log:
        child = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT)
        try:
            returncode = child.wait()
        except BaseException:
            child.terminate()
            child.wait()
            raise
    folder = args.work_dir / name
    log_path = folder / "antfly.log"
    log = log_path.read_text() if log_path.exists() else ""
    selected = [
        line for line in log.splitlines() if "selected backend metal for" in line
    ]
    results = read_json(folder / "results.json", [])
    ocr_invoked = any(
        manifest.get("ocr_attempted_count", 0) > 0
        for row in results
        for manifest in row.get("manifests", {}).values()
    )
    run = {
        "name": name,
        "command": command,
        "returncode": returncode,
        "results": results,
        "failure": read_json(folder / "failure.json"),
        "provenance": read_json(folder / "provenance.json", {}),
        "models": read_json(folder / "models.json"),
        "table_config": read_json(folder / "table-config.json"),
        "server_config": read_json(folder / "config.json"),
        "metal_confirmed": any("bge-small-en-v1.5" in line for line in selected)
        and (not ocr_invoked or any("Florence-2-base" in line for line in selected)),
    }
    print(
        f"Finished {name}: exit={returncode}, trials={len(run['results'])}", flush=True
    )
    return run


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work-dir", type=Path, required=True)
    parser.add_argument("--circus-dir", type=Path, required=True)
    for subject in ("main", "pr"):
        parser.add_argument(f"--{subject}-binary", type=Path, required=True)
        parser.add_argument(f"--{subject}-revision", required=True)
    parser.add_argument("--name", required=True)
    parser.add_argument(
        "--suite",
        choices=["scan", "embedded", "text", "small", "qualification"],
        default="text",
    )
    parser.add_argument("--mode", choices=["auto", "always"], default="always")
    parser.add_argument("--pairs", type=int, default=2)
    parser.add_argument("--reader-batch-size", type=int, choices=[1, 2, 4, 8, 16])
    parser.add_argument("--trials", type=int, default=3)
    parser.add_argument("--timeout", type=int, default=300)
    parser.add_argument("--port", type=int, default=29700)
    args = parser.parse_args()
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", args.name):
        parser.error("name must be a single safe directory name")
    if min(args.pairs, args.trials, args.timeout) < 1:
        parser.error("pairs, trials and timeout must be positive")
    for subject in ("main", "pr"):
        if not re.fullmatch(r"[0-9a-f]{40}", getattr(args, f"{subject}_revision")):
            parser.error("revisions must be full commit SHAs")
        getattr(args, f"{subject}_binary").resolve(strict=True)
    args.work_dir = args.work_dir.resolve(strict=True)
    args.circus_dir = args.circus_dir.resolve(strict=True)
    out = args.work_dir / args.name
    out.mkdir()  # Never overwrite an existing experiment.
    save(
        out / "experiment.json",
        {k: str(v) if isinstance(v, Path) else v for k, v in vars(args).items()},
    )
    pairs = []
    for i in range(args.pairs):
        order = ["main", "pr"] if i % 2 == 0 else ["pr", "main"]
        pair = {"order": order}
        for subject in order:
            pair[subject] = run_subject(args, out, i, subject)
            save(out / f"pair-{i:02d}.json", pair)
        pairs.append(pair)
        save(out / "summary.json", summarize(pairs, args.trials))
    summary = summarize(pairs, args.trials)
    print(json.dumps(summary, indent=2), flush=True)
    return 0 if summary["timing_comparable"] else 1


if __name__ == "__main__":
    sys.exit(main())
