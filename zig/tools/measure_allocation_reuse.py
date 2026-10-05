#!/usr/bin/env python3
"""Compare native allocation benchmarks; both trees must use the same harness.

Example:
  python3 tools/measure_allocation_reuse.py \
    --baseline-bin ../.worktrees/baseline/zig/zig-out/bin \
    --candidate-bin zig-out/bin --output /tmp/allocation-comparison

JSON document fixtures interpret --documents as renders, and --document-batches
as numeric fields per render (json_numbers) or input media bytes (json_media).
Fixture setup is outside measurement and uses the C allocator; measured document
collectors use either the diagnostic counter or the production smp allocator.

Replay counted timings include diagnostic counter overhead. Replay timing runs
use the production smp allocator without counting. Vector timing runs disable
counting and retain the benchmark's normal allocator. Heap counts are requested
bytes through the benchmark allocator, not RSS or complete process allocation.
allocations counts successful raw allocs. resize_calls/remap_calls count attempts;
moving_remaps/moved_bytes count successful pointer changes and their new sizes.
allocated_bytes counts successful logical storage growth, excluding transient
backend realloc overlap.
"""

import argparse
import json
import itertools
import os
from pathlib import Path
import statistics
import subprocess
import tempfile


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-bin", type=Path, required=True)
    parser.add_argument("--candidate-bin", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--documents", type=int, default=50000)
    parser.add_argument("--dimensions", type=int, default=1536)
    parser.add_argument("--samples", type=int, default=7)
    parser.add_argument("--vector-samples", type=int, default=3)
    parser.add_argument(
        "--vector-measurements",
        nargs="+",
        choices=["counted", "timing"],
        default=["counted", "timing"],
    )
    parser.add_argument(
        "--index-kinds",
        nargs="+",
        choices=["full_text", "algebraic"],
        default=["full_text", "algebraic"],
    )
    parser.add_argument(
        "--replay-sources",
        nargs="+",
        choices=["journal", "primary"],
        default=["journal"],
    )
    parser.add_argument("--documents-per-record", nargs="+", type=int, default=[1, 128])
    parser.add_argument("--batches", nargs="+", type=int, default=[256, 1024])
    parser.add_argument(
        "--replay-operation",
        choices=[
            "replay",
            "enrichment",
            "latest",
            "ordinal",
            "scalar_ordinal",
            "scratch_trim",
        ],
        default="replay",
    )
    parser.add_argument("--repetitions", type=int, default=1)
    parser.add_argument(
        "--replay-measurements",
        nargs="+",
        choices=["counted", "timing"],
        default=["counted"],
    )
    parser.add_argument("--replay-only", action="store_true")
    parser.add_argument("--vector-only", action="store_true")
    parser.add_argument(
        "--vector-modes",
        nargs="+",
        choices=["ingest", "retry", "retry-mixed", "session-only"],
        default=["ingest"],
    )
    parser.add_argument("--document-lookup-only", action="store_true")
    parser.add_argument("--document-batches", nargs="+", type=int, default=[256])
    parser.add_argument("--document-value-bytes", type=int, default=4096)
    parser.add_argument(
        "--document-cases",
        nargs="+",
        choices=[
            "short",
            "long",
            "missing",
            "sparse",
            "sparse_large",
            "text_large",
            "embedding_identity",
            "embedding_identity_chunk",
            "text",
            "text_asset",
            "text_asset_large",
            "text_asset_escaped",
            "text_mixed",
            "text_missing",
            "delete_set",
            "relational",
            "json_numbers",
            "json_media",
        ],
        default=["short"],
    )
    parser.add_argument(
        "--ordinal-case", choices=["short", "long", "missing"], default="short"
    )
    args = parser.parse_args()
    if (
        min(
            args.documents,
            args.dimensions,
            args.samples,
            args.vector_samples,
            *args.documents_per_record,
            *args.batches,
            *args.document_batches,
            args.document_value_bytes,
            args.repetitions,
        )
        <= 0
    ):
        parser.error("counts and sample sizes must be positive")
    args.output.mkdir(parents=True, exist_ok=False)
    binaries = {
        "baseline": args.baseline_bin.resolve(),
        "changed": args.candidate_bin.resolve(),
    }
    results = []
    session_checksums = set()
    env = os.environ.copy()
    env.pop("ANTFLY_SOURCE_VECTOR_BACKGROUND_CHECKPOINT", None)

    def run(label, command, child_env):
        result = subprocess.run(
            [str(x) for x in command], env=child_env, capture_output=True, text=True
        )
        (args.output / (label + ".log")).write_text(result.stdout + result.stderr)
        result.check_returncode()
        return result.stderr.splitlines()

    def save():
        (args.output / "results.json").write_text(json.dumps(results, indent=2) + "\n")

    for measurement, replay_source, kind, records, budgeted, batch in itertools.product(
        (
            []
            if args.document_lookup_only or args.vector_only
            else args.replay_measurements
        ),
        args.replay_sources,
        args.index_kinds,
        args.documents_per_record,
        ([False, True] if args.replay_operation == "replay" else [False]),
        args.batches,
    ):
        for pair in range(args.samples):
            for variant in (
                ["baseline", "changed"] if pair % 2 == 0 else ["changed", "baseline"]
            ):
                label = f"replay-{measurement}-{replay_source}-{kind}-{records}-{budgeted}-{batch}-{pair}-{variant}"
                lines = run(
                    label,
                    [
                        binaries[variant] / "replay-allocation-bench",
                        args.documents,
                        batch,
                        1,
                        "budgeted" if budgeted else "unbudgeted",
                        records,
                        kind,
                        replay_source,
                        args.replay_operation,
                        args.repetitions,
                        measurement,
                        args.ordinal_case,
                    ],
                    env,
                )
                data = json.loads(next(x for x in lines if x.startswith("{")))
                if data.get("source", "journal") != replay_source:
                    raise ValueError(
                        "both replay binaries must support the requested source"
                    )
                if (
                    data.get("operation", "replay") != args.replay_operation
                    or data.get("repetitions", 1) != args.repetitions
                ):
                    raise ValueError(
                        "both replay binaries must support the requested operation and repetitions"
                    )
                if data.get("measurement", "counted") != measurement:
                    raise ValueError(
                        "both replay binaries must support the requested measurement"
                    )
                results.append(
                    dict(workload="replay", variant=variant, pair=pair, **data)
                )
                print(label, data["elapsed_ns"], flush=True)
                save()
    for fixture, mode in itertools.product(
        args.vector_modes,
        (
            []
            if args.replay_only or args.document_lookup_only
            else args.vector_measurements
        ),
    ):
        child_env = env.copy()
        if mode == "counted":
            child_env["ANTFLY_COUNT_BENCH_ALLOCATIONS"] = "1"
        else:
            child_env.pop("ANTFLY_COUNT_BENCH_ALLOCATIONS", None)
        for pair in range(args.vector_samples):
            for variant in (
                ["baseline", "changed"] if pair % 2 == 0 else ["changed", "baseline"]
            ):
                label = f"vector-{fixture}-{mode}-{pair}-{variant}"
                with tempfile.TemporaryDirectory(
                    prefix="antfly-allocation-"
                ) as directory:
                    root = Path(directory) / "source"
                    if fixture not in ("ingest", "session-only"):
                        setup_env = env.copy()
                        setup_env.pop("ANTFLY_COUNT_BENCH_ALLOCATIONS", None)
                        run(
                            label + "-setup",
                            [
                                binaries["baseline"] / "vector-payload-bench",
                                "ingest",
                                root,
                                args.documents,
                                args.dimensions,
                            ],
                            setup_env,
                        )
                    lines = run(
                        label,
                        [
                            binaries[variant] / "vector-payload-bench",
                            fixture,
                            root,
                            args.documents,
                            args.dimensions,
                        ],
                        child_env,
                    )
                    data = json.loads(
                        next(
                            x[len("payload_bench ") :]
                            for x in lines
                            if x.startswith("payload_bench ")
                        )
                    )
                    if fixture == "session-only":
                        session_checksums.add(data["reference_checksum"])
                        if len(session_checksums) != 1:
                            raise ValueError(
                                "session preparation produced different references across runs"
                            )
                    expected = (
                        (args.documents + 127) // 128
                        if fixture == "retry-mixed"
                        else 0
                        if fixture in ("retry", "session-only")
                        else args.documents
                    )
                    if data["stats"]["prepared_payloads"] != expected:
                        raise ValueError(
                            "vector fixture did not prepare the expected number of payloads"
                        )
                    counts = next(
                        (
                            json.loads(x[len("allocation_bench ") :])
                            for x in lines
                            if x.startswith("allocation_bench ")
                        ),
                        {},
                    )
                    if fixture != "session-only":
                        run(
                            label + "-verify",
                            [
                                binaries[variant] / "vector-payload-bench",
                                "read-mixed" if fixture == "retry-mixed" else "read",
                                root,
                                args.documents,
                                args.dimensions,
                            ],
                            child_env,
                        )
                    results.append(
                        dict(
                            workload="vector",
                            case=fixture,
                            variant=variant,
                            pair=pair,
                            measurement=mode,
                            **data,
                            **counts,
                        )
                    )
                    print(label, data["run_ns"], flush=True)
                    save()
    if args.document_lookup_only:
        for fixture, batch, pair in itertools.product(
            args.document_cases, args.document_batches, range(args.samples)
        ):
            for variant in (
                ["baseline", "changed"] if pair % 2 == 0 else ["changed", "baseline"]
            ):
                label = f"document-{fixture}-{batch}-{pair}-{variant}"
                child_env = env.copy()
                child_env["ANTFLY_DOCUMENT_BENCH_CASE"] = fixture
                child_env["ANTFLY_DOCUMENT_BENCH_BATCH"] = str(batch)
                child_env["ANTFLY_DOCUMENT_BENCH_DOCUMENTS"] = str(args.documents)
                child_env["ANTFLY_DOCUMENT_BENCH_VALUE_BYTES"] = str(
                    args.document_value_bytes
                )
                lines = run(
                    label, [binaries[variant] / "document-lookup-bench"], child_env
                )
                measurements = [
                    json.loads(line.split("document_lookup_bench ", 1)[1])
                    for line in lines
                    if "document_lookup_bench {" in line
                ]
                if {x["measurement"] for x in measurements} != {
                    "counted",
                    "timing",
                } or len(measurements) != 2:
                    raise ValueError(
                        "both document collector binaries must emit counted and timing results"
                    )
                for data in measurements:
                    if (
                        data.get("case", "short") != fixture
                        or data["batch"] != batch
                        or data["documents"] != args.documents
                    ):
                        raise ValueError(
                            "both document collector binaries must support the requested fixture"
                        )
                    results.append(
                        dict(
                            workload="document",
                            source="primary",
                            value_bytes=args.document_value_bytes,
                            variant=variant,
                            pair=pair,
                            **data,
                        )
                    )
                print(
                    label,
                    [(x["measurement"], x["elapsed_ns"]) for x in measurements],
                    flush=True,
                )
                save()

    def checksum_fixture(x):
        # The delete-set microbenchmark visits six keys per batch; its checksum
        # intentionally depends on batch size. Ordinary document/replay sums
        # remain invariant across batching.
        return (
            x["workload"],
            x.get("case", "default"),
            x["batch"] if x.get("case") == "delete_set" else None,
        )

    for fixture in {
        checksum_fixture(x) for x in results if x["workload"] in ("replay", "document")
    }:
        checksums = {
            x["checksum"]
            for x in results
            if x["workload"] in ("replay", "document")
            and checksum_fixture(x) == fixture
        }
        if len(checksums) > 1:
            raise ValueError(f"baseline and candidate checksums differ for {fixture}")

    def measurement_mode(x, workload):
        measurement = x.get("measurement", "counted")
        return (
            measurement + ("-budgeted" if x.get("budgeted") else "-unbudgeted")
            if workload == "replay"
            else measurement
        )

    summary = {}
    for workload in ["replay", "vector", "document"]:
        groups = sorted(
            {
                (
                    x.get("case", "default"),
                    x.get("source", "journal"),
                    x.get("index_kind", "none"),
                    x.get("documents_per_record", 1),
                    x.get("batch", 0),
                    measurement_mode(x, workload),
                )
                for x in results
                if x["workload"] == workload
            }
        )
        for fixture, replay_source, kind, records, batch, mode in groups:
            group = {
                v: [
                    x
                    for x in results
                    if x["workload"] == workload
                    and x["variant"] == v
                    and x.get("case", "default") == fixture
                    and x.get("source", "journal") == replay_source
                    and x.get("index_kind", "none") == kind
                    and x.get("documents_per_record", 1) == records
                    and x.get("batch", 0) == batch
                    and measurement_mode(x, workload) == mode
                ]
                for v in binaries
            }
            fields = (
                ["elapsed_ns", "allocations", "allocated_bytes", "peak_live_bytes"]
                if workload in ("replay", "document")
                else [
                    "run_ns",
                    "final_checkpoint_ns",
                    "max_batch_ns",
                    "allocations",
                    "allocated_bytes",
                    "peak_additional_live_bytes",
                ]
            )
            fields += ["resize_calls", "remap_calls", "moving_remaps", "moved_bytes"]
            if workload in ("replay", "document") and mode.startswith("timing"):
                fields = ["elapsed_ns"]
            stats = {
                f: {v: statistics.median(x[f] for x in xs) for v, xs in group.items()}
                for f in fields
                if all(f in x for xs in group.values() for x in xs)
            }
            for values in stats.values():
                values["change_percent"] = (
                    100 * (values["changed"] / values["baseline"] - 1)
                    if values["baseline"]
                    else 0.0
                    if not values["changed"]
                    else None
                )
            field = "elapsed_ns" if workload in ("replay", "document") else "run_ns"
            ratios = [
                100
                * (
                    next(x[field] for x in group["changed"] if x["pair"] == pair)
                    / next(x[field] for x in group["baseline"] if x["pair"] == pair)
                    - 1
                )
                for pair in range(len(group["baseline"]))
                if next(x[field] for x in group["baseline"] if x["pair"] == pair) != 0
            ]
            stats["paired_elapsed_change_percent_median"] = (
                statistics.median(ratios) if ratios else None
            )
            summary[
                f"{workload}-"
                + (
                    f"{fixture}-"
                    if workload != "replay" or fixture != "default"
                    else ""
                )
                + f"{replay_source}-{kind}-{records}-{batch}-{mode}"
            ] = stats
    (args.output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
