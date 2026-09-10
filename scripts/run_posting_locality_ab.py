"""Matched posting-local float16 qualification on consolidated source storage.

Use one pinned binary, fresh roots, and alternating A/B then B/A ordering.
The 1M arms run only after every 50K arm passes the public API qualification.
This runner deliberately uses the archived qualification harness to avoid
changing measurement code underneath other experiments in this worktree.
"""

import argparse
import hashlib
import json
import math
import os
import subprocess
import sys
import time
from pathlib import Path

from projection_locality_inputs import input_paths, receipt_passed
from source_capture_experiment import (
    FLAG as DEFER_CAPTURE_FLAG,
    capture_preparation_evidence,
)
from native_preparation_experiments import (
    REFINEMENTS as NATIVE_REFINEMENTS,
    require_disk_headroom,
    validate_native_treatment,
)

REFINEMENTS = {
    "pages": ["ANTFLY_EXPERIMENT_PROJECTION_PAGES"],
    "phases": ["ANTFLY_EXPERIMENT_PHASE_ADMISSION"],
    "prediction": ["ANTFLY_EXPERIMENT_SCAN_PREDICTION"],
    "replay": ["ANTFLY_EXPERIMENT_REPLAY_FINALIZE"],
}
REFINEMENTS["combined"] = [key for values in REFINEMENTS.values() for key in values]
REFINEMENTS.update(
    {
        "fused": ["ANTFLY_EXPERIMENT_FUSED_NO_COPY"],
        "queued_pages": [
            "ANTFLY_EXPERIMENT_PROJECTION_PAGES",
            "ANTFLY_EXPERIMENT_GROUPED_FALLBACKS",
        ],
        "aggregate": [
            "ANTFLY_EXPERIMENT_PHASE_ADMISSION",
            "ANTFLY_EXPERIMENT_AGGREGATE_ADMISSION",
        ],
        "angular": ["ANTFLY_EXPERIMENT_ANGULAR_BOUNDS"],
        "quantized_routing": ["ANTFLY_EXPERIMENT_QUANTIZED_ROUTING"],
        "centered_routing": [
            "ANTFLY_EXPERIMENT_QUANTIZED_ROUTING",
            "ANTFLY_EXPERIMENT_CENTERED_ROUTING",
        ],
        "clustering": ["ANTFLY_EXPERIMENT_PROJECTION_CLUSTERING"],
        "capture": ["ANTFLY_EXPERIMENT_CAPTURE_STAGES"],
        "subgroups_4": ["ANTFLY_EXPERIMENT_SUBGROUPS_4"],
        "subgroups_8": ["ANTFLY_EXPERIMENT_SUBGROUPS_8"],
        "subgroups_16": ["ANTFLY_EXPERIMENT_SUBGROUPS_16"],
        "subgroup_routing": ["ANTFLY_EXPERIMENT_SUBGROUP_ROUTING"],
        "global_subgroup_routing": ["ANTFLY_EXPERIMENT_GLOBAL_SUBGROUP_ROUTING"],
        "compact_subgroup_routing": [
            "ANTFLY_EXPERIMENT_GLOBAL_SUBGROUP_ROUTING",
            "ANTFLY_EXPERIMENT_COMPACT_SUBGROUP_ROUTING",
        ],
        "borrowed_pages": [
            "ANTFLY_EXPERIMENT_PROJECTION_PAGES",
            "ANTFLY_EXPERIMENT_GROUPED_FALLBACKS",
            "ANTFLY_EXPERIMENT_PROJECTION_BORROW",
        ],
        "incremental_publication": [
            "ANTFLY_SOURCE_VECTOR_APPEND_ONLY",
            "ANTFLY_SOURCE_VECTOR_SELECTIVE_GC",
            "ANTFLY_SOURCE_VECTOR_COALESCE_DIRECTORY",
        ],
        "compact_posting_deltas": ["ANTFLY_EXPERIMENT_COMPACT_POSTING_DELTAS"],
    }
)
REFINEMENTS["compact_borrowed"] = (
    REFINEMENTS["compact_subgroup_routing"] + REFINEMENTS["borrowed_pages"]
)
REFINEMENTS["recovery_v2"] = list(
    dict.fromkeys(
        flag
        for name in (
            "fused",
            "queued_pages",
            "aggregate",
            "angular",
            "clustering",
            "capture",
        )
        for flag in REFINEMENTS[name]
    )
)
REFINEMENTS["admitted_quantized_routing"] = (
    REFINEMENTS["aggregate"] + REFINEMENTS["quantized_routing"]
)
REFINEMENTS.update(NATIVE_REFINEMENTS)
ALL_REFINEMENT_FLAGS = sorted(
    {flag for flags in REFINEMENTS.values() for flag in flags}
)


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def validate_subgroup_treatment(arm, environment, filename="public-query-profile.json"):
    enabled = any(
        environment.get(flag) == "1"
        for flag in (
            "ANTFLY_EXPERIMENT_SUBGROUP_ROUTING",
            "ANTFLY_EXPERIMENT_GLOBAL_SUBGROUP_ROUTING",
        )
    ) and any(
        environment.get(f"ANTFLY_EXPERIMENT_SUBGROUPS_{count}") == "1"
        for count in (4, 8, 16)
    )
    required = []
    if enabled:
        required += ["hbc_subgroup_leaves_scored", "hbc_subgroup_vectors_skipped"]
    if environment.get("ANTFLY_EXPERIMENT_COMPACT_SUBGROUP_ROUTING") == "1":
        required.append("hbc_subgroup_compact_groups_scored")
    if environment.get("ANTFLY_EXPERIMENT_PROJECTION_BORROW") == "1":
        required.append("hbc_rerank_vector_projection_borrows")
    if not required:
        return None
    try:
        profile = json.loads((arm / filename).read_text())
        observed = {
            key: float(profile["profile_values"][key]["mean"]) for key in required
        }
    except (OSError, ValueError, KeyError, TypeError) as error:
        raise RuntimeError(
            f"missing subgroup treatment evidence: {arm.name}"
        ) from error
    if not all(math.isfinite(value) and value > 0 for value in observed.values()):
        raise RuntimeError(f"inert subgroup routing treatment: {arm.name}: {observed}")
    return observed


def validate_pair_recall(control, candidate, profile_count):
    """Completion is not a recall gate. Require parity before scaling a pair."""
    observed = {}
    for label in ("online-live", "reopened-warm"):
        values = []
        for arm in (control, candidate):
            summary = json.loads((arm / "qualification-summary.json").read_text())
            runs = [run for run in summary["runs"] if run["label"].endswith(label)]
            if len(runs) != 1:
                raise RuntimeError(f"missing unique {label} recall: {arm}")
            values.append(runs[0]["recall"])
        observed[label] = values
    if profile_count:
        values = []
        for arm in (control, candidate):
            profile = json.loads((arm / "public-query-profile.json").read_text())
            if profile.get("count") != profile_count:
                raise RuntimeError(f"incomplete fixed recall profile: {arm}")
            values.append(profile["recall"])
        observed["fixed-profile"] = values
    for label, (before, after) in observed.items():
        if not all(
            isinstance(value, (int, float))
            and not isinstance(value, bool)
            and math.isfinite(value)
            and 0 < value <= 1
            for value in (before, after)
        ):
            raise RuntimeError(f"invalid paired recall for {label}")
        if after + 0.010000000001 < before:
            raise RuntimeError(
                f"paired recall loss exceeds one percentage point for {label}: {before} -> {after}"
            )
    return observed


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument(
        "--control-binary",
        type=Path,
        help="Optional preserved baseline executable; both arms remain fresh no-copy loads",
    )
    parser.add_argument("--port", type=int, default=19450)
    parser.add_argument("--health-port", type=int, default=19451)
    parser.add_argument("--pairs", type=int, default=2)
    parser.add_argument("--include-1m", action="store_true")
    parser.add_argument("--resume", action="store_true")
    parser.add_argument(
        "--sample-process",
        action="store_true",
        help="Collect non-suspending Darwin CPU, page-in, I/O and footprint samples",
    )
    parser.add_argument("--refinement", choices=list(REFINEMENTS))
    parser.add_argument(
        "--common-refinement",
        action="append",
        default=[],
        choices=list(REFINEMENTS),
        help="Enable a disjoint treatment identically in both arms",
    )
    parser.add_argument("--encoding", choices=["float16"], default="float16")
    parser.add_argument("--memory-budget-mb", type=int)
    parser.add_argument("--query-seconds", type=int, default=30)
    parser.add_argument("--mixed-seconds", type=int, default=30)
    parser.add_argument("--mixed-write-rows-per-second", type=float, default=0)
    parser.add_argument("--profile-count", type=int, default=1000)
    parser.add_argument(
        "--capture-stages",
        action="store_true",
        help="Trace capture stages identically in both arms",
    )
    args = parser.parse_args()
    if args.sample_process and sys.platform != "darwin":
        parser.error("--sample-process currently requires Darwin")
    if args.pairs < 2:
        parser.error("use at least two pairs to reverse run order")
    if (
        not math.isfinite(args.mixed_write_rows_per_second)
        or args.mixed_write_rows_per_second < 0
    ):
        parser.error("mixed offered write rate must be finite and non-negative")
    common_flags = {
        flag for name in args.common_refinement for flag in REFINEMENTS[name]
    }
    if common_flags.intersection(REFINEMENTS.get(args.refinement, [])):
        parser.error("common refinements must not overlap the A/B treatment")
    layout_flags = {
        "ANTFLY_EXPERIMENT_SUBGROUPS_4",
        "ANTFLY_EXPERIMENT_SUBGROUPS_8",
        "ANTFLY_EXPERIMENT_SUBGROUPS_16",
    }
    candidate_flags = common_flags.union(REFINEMENTS.get(args.refinement, []))
    if len(candidate_flags.intersection(layout_flags)) > 1:
        parser.error("choose only one physical subgroup layout per fresh-load A/B")
    if "ANTFLY_EXPERIMENT_CERTIFIED_SUBGROUPS" in candidate_flags and (
        not candidate_flags.intersection(layout_flags) or args.profile_count <= 0
    ):
        parser.error(
            "certified subgroup qualification requires a physical subgroup layout and a nonempty public profile"
        )
    if (
        candidate_flags.intersection(layout_flags)
        and candidate_flags.intersection(
            {
                "ANTFLY_EXPERIMENT_SUBGROUP_ROUTING",
                "ANTFLY_EXPERIMENT_GLOBAL_SUBGROUP_ROUTING",
            }
        )
        and args.profile_count <= 0
    ):
        parser.error(
            "subgroup routing qualification requires a nonempty public profile"
        )
    if args.capture_stages and args.refinement == "capture":
        parser.error("common tracing would make the capture A/B inert")
    capture_experiment = DEFER_CAPTURE_FLAG in candidate_flags
    if capture_experiment and not (
        args.capture_stages or "ANTFLY_EXPERIMENT_CAPTURE_STAGES" in common_flags
    ):
        parser.error(
            "deferred capture qualification requires common capture-stage tracing"
        )
    binary = args.binary.resolve(strict=True)
    control_binary = (
        args.control_binary.resolve(strict=True) if args.control_binary else binary
    )
    scripts = Path(__file__).resolve().parent
    harness = scripts / "run_vdbbench_qualification_snapshot_20260906.sh"
    # These helpers are read by the harness at runtime; detect edits instead
    # of silently combining different measurement implementations.
    inputs = input_paths(binary, Path(__file__).resolve())
    inputs.add(control_binary)
    inputs.add(scripts / "native_preparation_experiments.py")
    inputs.add(scripts / "source_capture_experiment.py")
    if args.sample_process:
        inputs.add(scripts / "sample_macos_process_memory.py")
    expected = {str(path): digest(path) for path in sorted(inputs)}
    environment = os.environ.copy()
    # Source refinements are a separate experiment. Do not accidentally
    # inherit a refinement or routing/admission treatment from the shell.
    for key in list(environment):
        if key.startswith(("ANTFLY_", "VDBBENCH_")):
            del environment[key]
    environment["ANTFLY_BIN"] = str(binary)
    environment["ANTFLY_VDBBENCH_SYNC_LEVEL"] = "write"
    environment["VDBBENCH_MIXED_WRITE_ROWS_PER_SECOND"] = str(
        args.mixed_write_rows_per_second
    )
    args.root.mkdir(parents=True, exist_ok=args.resume)
    root = args.root.resolve()
    receipts = json.loads((root / "ab-runs.json").read_text()) if args.resume else []

    def save():
        (root / "ab-runs.json").write_text(json.dumps(receipts, indent=2) + "\n")

    def verify():
        for name, checksum in expected.items():
            if digest(Path(name)) != checksum:
                raise RuntimeError(f"qualification input changed: {name}")

    cases = ["Performance1536D50K"]
    if args.include_1m:
        cases.append("Performance768D1M")
    for case in cases:
        for pair in range(args.pairs):
            modes = (
                ["control", "candidate"]
                if args.refinement or args.control_binary
                else ["local_on", "local_off"]
            )
            if pair % 2:
                modes.reverse()
            for mode in modes:
                verify()
                arm_environment = environment.copy()
                arm_environment["ANTFLY_BIN"] = str(
                    control_binary if mode == "control" else binary
                )
                arm_environment["ANTFLY_EXPERIMENT_POSTING_LOCAL_PROJECTIONS"] = (
                    "1" if mode == "local_on" else "0"
                )
                if args.refinement:
                    for key in REFINEMENTS[args.refinement]:
                        arm_environment[key] = "1" if mode == "candidate" else "0"
                for key in common_flags:
                    arm_environment[key] = "1"
                if args.capture_stages:
                    arm_environment["ANTFLY_EXPERIMENT_CAPTURE_STAGES"] = "1"
                if args.refinement in (
                    "coalesced_deletes",
                    "reused_delete_vectors",
                    "dense_delete_plan",
                    "stable_posting_origins",
                    "posting_row_deltas",
                ):
                    # Identical stage instrumentation in control and treatment.
                    arm_environment["ANTFLY_BENCH_BATCH_PROFILE"] = "1"
                    arm_environment["ANTFLY_BENCH_HBC_WRITE_PROFILE"] = "1"
                arm = root / f"{case}-{pair + 1}-{mode}"
                command = [
                    str(harness),
                    str(arm),
                    str(args.port),
                    str(args.health_port),
                    "--case",
                    case,
                    "--dense-embeddings",
                    "vector_store",
                    "--native-hbc",
                    "--vector-blocks",
                    "--vector-block-encoding",
                    args.encoding,
                    "--batch",
                    "100",
                    "--workers",
                    "4",
                    "--query-concurrency",
                    "1,10,20,30",
                    "--query-seconds",
                    str(args.query_seconds),
                    "--mixed-seconds",
                    str(args.mixed_seconds),
                    "--profile-count",
                    str(args.profile_count),
                ]
                if args.memory_budget_mb is not None:
                    command.extend(["--memory-budget-mb", str(args.memory_budget_mb)])
                controlled_environment = {
                    key: value
                    for key, value in arm_environment.items()
                    if key.startswith(("ANTFLY_", "VDBBENCH_"))
                }
                previous = next(
                    (
                        r
                        for r in receipts
                        if r["case"] == case
                        and r["pair"] == pair + 1
                        and r["mode"] == mode
                    ),
                    None,
                )
                if previous is not None:
                    if (
                        previous["command"] != command
                        or previous["inputs_sha256"] != expected
                        or previous["environment"] != controlled_environment
                        or not receipt_passed(root, previous)
                    ):
                        raise RuntimeError(
                            f"cannot resume failed or changed arm: {arm.name}"
                        )
                    continue
                require_disk_headroom(root, case)
                receipt = {
                    "case": case,
                    "pair": pair + 1,
                    "mode": mode,
                    "table_mode": "vector_store",
                    "refinement": args.refinement,
                    "common_refinements": args.common_refinement,
                    "encoding": args.encoding,
                    "command": command,
                    "started_at": time.time(),
                    "inputs_sha256": expected,
                    "environment": controlled_environment,
                }
                receipts.append(receipt)
                save()
                print(f"Starting {arm.name}", flush=True)
                with (root / f"{arm.name}.log").open("w") as log:
                    sampler = None
                    try:
                        if args.sample_process:
                            samples = root / f"{arm.name}-process.jsonl"
                            sampler = subprocess.Popen(
                                [
                                    sys.executable,
                                    str(scripts / "sample_macos_process_memory.py"),
                                    "--pid-file",
                                    str(arm / "antfly.pid"),
                                    "--output",
                                    str(samples),
                                    "--seconds",
                                    "7200",
                                ],
                                stdout=log,
                                stderr=subprocess.STDOUT,
                            )
                            receipt["process_samples"] = str(samples)
                            save()
                        result = subprocess.run(
                            command,
                            env=arm_environment,
                            stdout=log,
                            stderr=subprocess.STDOUT,
                            check=False,
                        )
                    finally:
                        if sampler is not None:
                            if sampler.poll() is None:
                                sampler.terminate()
                            try:
                                sampler.wait(timeout=10)
                            except subprocess.TimeoutExpired:
                                sampler.kill()
                                sampler.wait()
                            receipt["process_sampler_exit_code"] = sampler.returncode
                receipt.update(exit_code=result.returncode, finished_at=time.time())
                try:
                    verify()
                    if (
                        args.sample_process
                        and receipt["process_sampler_exit_code"] != 0
                    ):
                        raise RuntimeError(
                            "process sampler failed; performance attribution is incomplete"
                        )
                    if result.returncode == 0:
                        observation = validate_subgroup_treatment(arm, arm_environment)
                        if observation is not None:
                            receipt["treatment_observation"] = observation
                        native_observation = validate_native_treatment(
                            arm, arm_environment
                        )
                        if native_observation is not None:
                            receipt["native_treatment_observation"] = native_observation
                        if capture_experiment:
                            receipt["capture_preparation_observation"] = (
                                capture_preparation_evidence(
                                    (arm / "antfly-initial.log")
                                    .read_text(errors="replace")
                                    .splitlines(),
                                    arm_environment.get(DEFER_CAPTURE_FLAG) == "1",
                                )
                            )
                except RuntimeError as error:
                    receipt["invalid_reason"] = str(error)
                    save()
                    raise
                save()
                if result.returncode:
                    raise RuntimeError(f"{arm.name} failed; later arms are gated")
                print(f"Passed {arm.name}", flush=True)
            control_mode, candidate_mode = (
                ("control", "candidate")
                if args.refinement or args.control_binary
                else ("local_on", "local_off")
            )
            pair_receipt = next(
                r
                for r in reversed(receipts)
                if r["case"] == case and r["pair"] == pair + 1
            )
            try:
                recall = validate_pair_recall(
                    root / f"{case}-{pair + 1}-{control_mode}",
                    root / f"{case}-{pair + 1}-{candidate_mode}",
                    args.profile_count,
                )
            except (OSError, ValueError, KeyError, TypeError, RuntimeError) as error:
                pair_receipt["invalid_reason"] = f"paired recall gate failed: {error}"
                save()
                raise RuntimeError(pair_receipt["invalid_reason"]) from error
            pair_receipt["paired_recall"] = recall
            save()


if __name__ == "__main__":
    main()
