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

"""Local replay feedback loop; uses prebuilt libraries and synthetic inputs.

Example (each case runs in a separate process, without rebuilding):
  python3 scripts/bench/lite_namespace_replay.py --library /path/libantfly.dylib \
      --idle-namespaces 0 8 45 --output .tools/namespace-bench

The default measured interval includes a write burst and a quiet replay window.
Use --active-seconds for continuous writes with sampled visibility of earlier
targets; sampling overhead is reported separately. No status calls occur during
the quiet window. Explicit draining,
acknowledgment, search, integrity and reopen checks follow outside measurement.
This diagnostic is not a substitute for original-importer qualification.
"""

from __future__ import annotations

import argparse
from contextlib import ExitStack
import hashlib
import json
import os
import resource
import subprocess
import sys
import time
from pathlib import Path

from lite_contention import percentile, sdk, usage_delta


def digest(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def progress(db):
    return [
        {
            key: index.get(key)
            for key in (
                "name",
                "kind",
                "replay_applied_sequence",
                "replay_target_sequence",
                "replay_catch_up_required",
                "doc_count",
                "term_count",
            )
        }
        for index in db.stats()["indexes"]
    ]


def counters(db):
    return db.pending_work_stats().get("background_publications", {})


def ensure_text_index(db):
    # Native tables can already own the default full-text index. Avoid
    # accidentally benchmarking duplicate indexing of the same source.
    if not any(index["kind"] == "full_text" for index in db.stats()["indexes"]):
        db.add_index({"name": "text", "type": "full_text"})


def delta(before, after):
    # Missing instrumentation is unknown, not zero. Preserve resets explicitly.
    return {
        key: (value - before[key] if value >= before[key] else None)
        for key, value in after.items()
        if key in before
        and type(value) is int
        and type(before[key]) is int
        and key not in ("retirement_work_pages", "replay_budget_table_id")
        and not key.endswith("_table_id")
        and not key.endswith("_max_ns")
    }


def active_progress(samples, deadline, *, excluded_indexes=()):
    """A moving target cannot prove visibility; follow each captured prefix."""
    samples = [
        dict(
            sample,
            indexes=[
                index
                for index in sample["indexes"]
                if index["name"] not in excluded_indexes
            ],
        )
        for sample in samples
    ]
    pending = None
    completed = []
    overdue = []
    initial = (
        {
            index["name"]: index.get("replay_applied_sequence")
            for index in samples[0]["indexes"]
        }
        if samples
        else {}
    )
    advancing = set()
    missing = False
    for sample in samples:
        current = {index["name"]: index for index in sample["indexes"]}
        missing |= set(current) != set(initial) or any(
            index.get(field) is None
            for index in current.values()
            for field in ("replay_applied_sequence", "replay_target_sequence")
        )
        advancing.update(
            name
            for name, index in current.items()
            if initial.get(name) is not None
            and index.get("replay_applied_sequence") is not None
            and index["replay_applied_sequence"] > initial[name]
        )
        if pending is not None:
            captured_at, targets = pending
            age = sample["seconds"] - captured_at
            reached = all(
                name in current
                and current[name].get("replay_applied_sequence") is not None
                and current[name]["replay_applied_sequence"] >= target
                for name, target in targets.items()
            )
            if reached:
                completed.append(age)
                if age > deadline:
                    overdue.append(age)
                pending = None
            elif age > deadline:
                overdue.append(age)
        if pending is None:
            targets = {
                name: index["replay_target_sequence"]
                for name, index in current.items()
                if index.get("replay_target_sequence") is not None
            }
            if targets and all(target > 0 for target in targets.values()):
                pending = sample["seconds"], targets
    return {
        "passed": bool(initial)
        and advancing == set(initial)
        and bool(completed)
        and not overdue
        and not missing,
        "deadline_seconds": deadline,
        "excluded_indexes": list(excluded_indexes),
        "completed_prefixes": len(completed),
        "observed_completion_seconds": completed,
        "overdue_observations": len(overdue),
        "advancing_indexes": sorted(advancing),
        "missing_evidence": missing,
        "pending_age_seconds": samples[-1]["seconds"] - pending[0]
        if pending is not None
        else None,
    }


def resolver_progress(db):
    return db.resolver_reviews({"operation": "progress"})["resolvers"]


def backfill_progress(samples, revision, target, deadline):
    """Require the accepted policy and its corpus scan, not quiet drain later."""
    for sample in samples:
        for row in sample.get("resolver_progress", []):
            if row.get("name") != "bench_concepts":
                continue
            generation = max(
                row.get("curation_revision", 0),
                row.get("namespace_curation_generation", 0),
            )
            applied = row.get("resolution", {}).get("applied_sequence")
            if (
                generation >= revision
                and row.get("backfill_pending") is False
                and applied is not None
                and applied >= target
            ):
                return {
                    "passed": sample["seconds"] <= deadline,
                    "completion_seconds": sample["seconds"],
                    "revision": revision,
                    "target_sequence": target,
                    "deadline_seconds": deadline,
                }
    return {
        "passed": False,
        "completion_seconds": None,
        "revision": revision,
        "target_sequence": target,
        "deadline_seconds": deadline,
    }


def worker(args):
    af = sdk(str(Path(args.library).resolve()))
    case = args.output
    case.mkdir(parents=True, exist_ok=False)
    path = case / "benchmark.aflite"
    row = {
        "schema_version": 1,
        "library_sha256": digest(args.library),
        "benchmark_sha256": digest(__file__),
        "idle_namespaces": args.idle_namespaces[0],
        "batches": args.batches,
        "active_seconds_requested": args.active_seconds,
        "progress_interval_seconds": args.progress_interval,
        "documents_per_batch": args.documents,
        "text_bytes": args.text_bytes,
        "quiet_seconds": args.seconds,
        "active_table": args.active_table,
        "grouped_copy": args.grouped_copy,
        "resolver_backfill_requested": args.resolver_backfill,
        "warm_documents": args.warm_documents if args.resolver_backfill else 0,
        "warm_text_bytes": args.warm_text_bytes if args.resolver_backfill else 0,
        "no_sync": args.no_sync,
        "busy_timeout": args.busy_timeout,
        "settle_timeout_seconds": args.settle_timeout,
        "peak_sampling": "post-burst and every 50 ms during quiet window",
        "status": "error",
        "pid": os.getpid(),
        "load_average": os.getloadavg(),
    }
    started = time.monotonic()
    acknowledged = {}
    latencies = []
    failures = []
    with (
        af.create(
            path,
            no_sync=args.no_sync,
            busy_timeout=args.busy_timeout,
            generated_enrichment_replay=args.grouped_copy or args.resolver_backfill,
        ) as root,
        ExitStack() as handles,
    ):
        # Include real index artifacts in otherwise idle namespaces.
        for number in range(args.idle_namespaces[0]):
            name = f"idle_{number:03d}"
            root.create_table(name, {})
            with root.open_table(name) as table:
                ensure_text_index(table)
                table.batch_json(
                    {"inserts": {"seed": {"text": "idle seeded artifact"}}}
                )
                table.run_until_idle()
        if args.active_table != "default":
            root.create_table(args.active_table, {})
        db = handles.enter_context(root.open_table(args.active_table))
        ensure_text_index(db)
        if args.grouped_copy:
            db.add_enrichment(
                {
                    "name": "turns",
                    "kind": "group",
                    "source": {
                        "group_by": "thread",
                        "order_by": "ordinal",
                        "segment_by": "batch",
                        "field": "text",
                    },
                }
            )
            db.add_enrichment(
                {
                    "name": "copied",
                    "kind": "asset",
                    "source_artifact_name": "turns",
                    "producer_json": json.dumps({"type": "copy"}),
                }
            )
            db.add_index(
                {"name": "copied_text", "type": "full_text", "artifact_name": "copied"}
            )
        if args.resolver_backfill:
            # A configured extraction stream can be empty (e.g. imports with
            # semantic extraction disabled). Repair still traverses the corpus.
            db.add_index(
                {
                    "name": "knowledge",
                    "type": "graph",
                    "source": {
                        "artifact": "kg",
                        "format": "extraction_graph",
                        "mention_edge_type": "mentions",
                    },
                    "artifact": {
                        "name": "kg",
                        "kind": "asset",
                        "source": {"type": "field", "value": "kg"},
                        "content_type": "application/json",
                    },
                    "edge_types": [{"name": "mentions"}],
                    "resolvers": [
                        {
                            "name": "bench_concepts",
                            "table": "default",
                            "promote": False,
                            "source_artifact": "kg",
                            "resolution_artifact": "resolved",
                            "key_template": "concept/{{uslug _entity.text}}",
                        }
                    ],
                }
            )
            for first in range(0, args.warm_documents, 32):
                warm = {
                    f"warm-{i}": {
                        "text": "namespacecanary " + "x" * args.warm_text_bytes,
                        "thread": f"warm-{i // 32}",
                        "batch": i // 32,
                        "ordinal": i,
                    }
                    for i in range(first, min(first + 32, args.warm_documents))
                }
                db.batch_json({"inserts": warm, "sync_level": "write"})
                acknowledged.update(warm)
        root.run_until_idle()
        # Manual draining does not service the background owner's startup
        # schedule. Let it visit every namespace before measuring steady work.
        # Polling is confined to setup; it stops before the measured interval.
        settled = counters(root)
        initial_round = settled.get("maintenance_rounds")
        if initial_round is None:
            raise RuntimeError("library lacks maintenance-round instrumentation")
        settle_began = time.monotonic()
        while settled["maintenance_rounds"] <= initial_round:
            if time.monotonic() - settle_began >= args.settle_timeout:
                raise TimeoutError("background startup round did not complete")
            time.sleep(0.2)
            settled = counters(root)
        row["startup_settle_seconds"] = time.monotonic() - settle_began
        before = counters(root)
        initial_progress = progress(db)
        if args.resolver_backfill:
            row["enrichment_before"] = db.pending_work_stats()["enrichment"]
        initial_size = path.stat().st_size
        row["setup_seconds"] = time.monotonic() - started
        usage = resource.getrusage(resource.RUSAGE_SELF)
        initial_resolver_target = (
            resolver_progress(db)[0]["resolution"]["target_sequence"]
            if args.resolver_backfill
            else None
        )
        began = time.monotonic()
        accepted_revision = None
        if args.resolver_backfill:
            accepted = db.curate_entity(
                "default", "concept/a", {"action": "merge", "into": "concept/b"}
            )
            accepted_revision = accepted["results"][0]["revision"]
            row["curation_command_seconds"] = time.monotonic() - began
            row["accepted_curation_revision"] = accepted_revision
            row["resolver_initial_target"] = initial_resolver_target
        batch = 0
        active_samples = []
        sampling_seconds = 0
        next_sample = began + args.progress_interval
        while (
            time.monotonic() - began < args.active_seconds
            if args.active_seconds
            else batch < args.batches
        ):
            documents = {
                f"doc-{batch}-{i}": {
                    "text": f"namespacecanary payload {batch} {i} "
                    + ("amber bronze cedar delta " * (args.text_bytes // 25 + 1))[
                        : args.text_bytes
                    ],
                    "batch": batch,
                    "ordinal": batch * args.documents + i if args.grouped_copy else i,
                    **({"thread": "benchmark"} if args.grouped_copy else {}),
                }
                for i in range(args.documents)
            }
            call = time.monotonic_ns()
            try:
                db.batch_json({"inserts": documents, "sync_level": "write"})
                acknowledged.update(documents)
            except af.BusyError as error:
                failures.append({"batch": batch, "error": type(error).__name__})
            latencies.append(time.monotonic_ns() - call)
            batch += 1
            if args.active_seconds and time.monotonic() >= next_sample:
                sample_began = time.monotonic()
                indexes = progress(db)
                resolvers = resolver_progress(db) if args.resolver_backfill else []
                enrichment = (
                    db.pending_work_stats()["enrichment"]
                    if args.resolver_backfill
                    else None
                )
                sampled = time.monotonic()
                sampling_seconds += sampled - sample_began
                active_samples.append(
                    {
                        "seconds": sampled - began,
                        "successful_batches": batch - len(failures),
                        "file_bytes": path.stat().st_size,
                        "indexes": indexes,
                        "resolver_progress": resolvers,
                        "enrichment": enrichment,
                    }
                )
                next_sample = sampled + args.progress_interval
        burst_seconds = time.monotonic() - began
        peak_size = max(
            [path.stat().st_size] + [sample["file_bytes"] for sample in active_samples]
        )
        quiet = time.monotonic()
        while time.monotonic() - quiet < args.seconds:
            peak_size = max(peak_size, path.stat().st_size)
            time.sleep(min(0.05, max(0, args.seconds - (time.monotonic() - quiet))))
        elapsed = time.monotonic() - began
        measured_usage = usage_delta(usage, resource.getrusage(resource.RUSAGE_SELF))
        size = path.stat().st_size
        # These calls may acquire admission; keep them outside the timed window.
        observed_progress = progress(db)
        after = counters(root)
        if args.resolver_backfill:
            row["enrichment_after"] = db.pending_work_stats()["enrichment"]
        row.update(
            measured_seconds=elapsed,
            burst_seconds=burst_seconds,
            attempted_batches=batch,
            successful_batches=batch - len(failures),
            active_samples=active_samples,
            active_sampling_seconds=sampling_seconds,
            active_progress=active_progress(
                active_samples,
                args.visibility_deadline,
                excluded_indexes=("knowledge",) if args.resolver_backfill else (),
            )
            if args.active_seconds
            else None,
            resolver_backfill_progress=backfill_progress(
                active_samples,
                accepted_revision,
                initial_resolver_target,
                args.visibility_deadline,
            )
            if args.resolver_backfill
            else None,
            busy=failures,
            acknowledged_documents=len(acknowledged),
            p50_ms=percentile(latencies, 50) / 1e6,
            p95_ms=percentile(latencies, 95) / 1e6,
            initial_bytes=initial_size,
            final_bytes=size,
            peak_bytes=peak_size,
            growth_bytes=size - initial_size,
            progress_before=initial_progress,
            progress_after=observed_progress,
            counters_before=before,
            counters_after=after,
            counter_deltas=delta(before, after),
            **measured_usage,
        )
        # Save diagnostics even if drain hangs and the parent watchdog fires.
        (case / "measurement.json").write_text(json.dumps(row, indent=2) + "\n")
        if args.measurement_only:
            row.update(
                status="active_visibility_failed"
                if (args.active_seconds and not row["active_progress"]["passed"])
                or (
                    args.resolver_backfill
                    and not row["resolver_backfill_progress"]["passed"]
                )
                else "busy"
                if failures
                else "measured",
                verified=False,
                qualification=False,
                verification="skipped_by_request",
            )
            (case / "result.json").write_text(json.dumps(row, indent=2) + "\n")
            print(json.dumps(row), flush=True)
            return
        verify = time.monotonic()
        db.run_until_idle()
        for key, expected in acknowledged.items():
            if db.lookup(key) != expected:
                raise RuntimeError(f"acknowledged document mismatch: {key}")
        row["acknowledged_verification_passed"] = True
        row["progress_after_drain"] = progress(db)
        if args.grouped_copy:
            enrichment = db.pending_work_stats()["enrichment"]
            row["enrichment_after_drain"] = enrichment
            if (
                enrichment["applied_sequence"] < enrichment["target_sequence"]
                or enrichment["worker_failed"]
                or enrichment["error_count"]
            ):
                raise RuntimeError("grouped-copy producer did not drain cleanly")
        (case / "measurement.json").write_text(json.dumps(row, indent=2) + "\n")
        if args.resolver_backfill:
            row["resolver_after_drain"] = resolver_progress(db)
            if not all(
                not r["backfill_pending"]
                and not r["resolution"]["catch_up_required"]
                and not r["resolution"]["error_count"]
                for r in row["resolver_after_drain"]
            ):
                raise RuntimeError("explicit drain left resolver backfill pending")
        if any(
            index["replay_catch_up_required"] for index in row["progress_after_drain"]
        ):
            raise RuntimeError("explicit drain left active index behind")
        indexes = [
            index
            for index in row["progress_after_drain"]
            if index["kind"] == "full_text" and index["name"] != "copied_text"
        ]
        if not indexes or any(
            index["doc_count"] != len(acknowledged) for index in indexes
        ):
            raise RuntimeError(
                "drained index count differs from acknowledged documents"
            )
        if args.grouped_copy and acknowledged:
            copied = [
                index
                for index in row["progress_after_drain"]
                if index["name"] == "copied_text"
            ]
            if len(copied) != 1 or copied[0]["doc_count"] <= 0:
                raise RuntimeError("grouped copy produced no indexed artifacts")
        if acknowledged:
            result = db.search(
                {
                    "full_text_search": {
                        "match": {"field": "text", "text": "namespacecanary"}
                    },
                    "limit": 10,
                }
            )
            if not result["responses"][0]["hits"]["hits"]:
                raise RuntimeError("search canary missing")
        if not root.check()["valid"]:
            raise RuntimeError("integrity failed")
        row["verification_seconds"] = time.monotonic() - verify
    with af.open_readonly(path) as root, root.open_table(args.active_table) as db:
        for key, expected in acknowledged.items():
            if db.lookup(key) != expected:
                raise RuntimeError(f"reopen mismatch: {key}")
        if not root.check()["valid"]:
            raise RuntimeError("reopen integrity failed")
    status = "busy" if failures else "ok"
    if (args.active_seconds and not row["active_progress"]["passed"]) or (
        args.resolver_backfill and not row["resolver_backfill_progress"]["passed"]
    ):
        status = "active_visibility_failed"
    row.update(status=status, verified=True, qualification=False)
    (case / "result.json").write_text(json.dumps(row, indent=2) + "\n")
    print(json.dumps(row), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--library", required=True)
    parser.add_argument(
        "--active-table", choices=("default", "active"), default="active"
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--idle-namespaces", type=int, nargs="+", default=[0, 8, 45])
    parser.add_argument("--batches", type=int, default=8)
    parser.add_argument("--active-seconds", type=float, default=0)
    parser.add_argument("--progress-interval", type=float, default=1)
    parser.add_argument("--visibility-deadline", type=float, default=10)
    parser.add_argument("--documents", type=int, default=128)
    parser.add_argument("--text-bytes", type=int, default=1024)
    parser.add_argument("--seconds", type=float, default=5)
    parser.add_argument("--busy-timeout", type=float, default=5)
    parser.add_argument("--watchdog", type=float, default=120)
    parser.add_argument("--settle-timeout", type=float, default=15)
    parser.add_argument("--no-sync", action="store_true")
    parser.add_argument(
        "--measurement-only",
        action="store_true",
        help="save diagnostics without explicit drain/integrity/reopen verification",
    )
    parser.add_argument(
        "--grouped-copy",
        action="store_true",
        help="exercise grouped-source fan-in and built-in copy enrichment",
    )
    parser.add_argument(
        "--resolver-backfill",
        action="store_true",
        help="apply curation over a warm corpus while writes continue",
    )
    parser.add_argument("--warm-documents", type=int, default=2048)
    parser.add_argument("--warm-text-bytes", type=int, default=8192)
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if (
        min(args.idle_namespaces) < 0
        or min(args.batches, args.documents, args.text_bytes) < 1
        or min(
            args.seconds,
            args.watchdog,
            args.busy_timeout,
            args.settle_timeout,
            args.progress_interval,
            args.visibility_deadline,
        )
        <= 0
        or args.active_seconds < 0
    ):
        parser.error(
            "counts and durations must be positive; idle namespaces may be zero"
        )
    if args.resolver_backfill and (
        args.active_seconds <= 0 or min(args.warm_documents, args.warm_text_bytes) < 1
    ):
        parser.error(
            "resolver backfill requires active writes and a nonempty warm corpus"
        )
    if args.worker:
        try:
            worker(args)
        except Exception as error:
            measured = args.output / "measurement.json"
            row = json.loads(measured.read_text()) if measured.exists() else {}
            row.update(status="error", error_type=type(error).__name__, verified=False)
            if args.output.exists():
                (args.output / "result.json").write_text(
                    json.dumps(row, indent=2) + "\n"
                )
            raise
        return
    args.output.mkdir(parents=True, exist_ok=False)
    results = []
    for order, count in enumerate(args.idle_namespaces):
        case = args.output / f"{order:02d}-idle-{count}"
        command = [
            sys.executable,
            __file__,
            "--worker",
            "--library",
            args.library,
            "--output",
            str(case),
            "--idle-namespaces",
            str(count),
            "--active-table",
            args.active_table,
        ]
        for option in (
            "batches",
            "documents",
            "text_bytes",
            "seconds",
            "busy_timeout",
            "settle_timeout",
            "active_seconds",
            "progress_interval",
            "visibility_deadline",
            "warm_documents",
            "warm_text_bytes",
        ):
            command.extend(
                ["--" + option.replace("_", "-"), str(getattr(args, option))]
            )
        if args.no_sync:
            command.append("--no-sync")
        if args.grouped_copy:
            command.append("--grouped-copy")
        if args.resolver_backfill:
            command.append("--resolver-backfill")
        if args.measurement_only:
            command.append("--measurement-only")
        with (args.output / f"{order:02d}.log").open("w") as log:
            try:
                process = subprocess.run(
                    command, stdout=log, stderr=log, timeout=args.watchdog
                )
                status = "ok" if process.returncode == 0 else "error"
            except subprocess.TimeoutExpired:
                status = "watchdog_timeout"
        result_path = case / "result.json"
        measurement_path = case / "measurement.json"
        if result_path.exists():
            result = json.loads(result_path.read_text())
        else:
            result = (
                json.loads(measurement_path.read_text())
                if measurement_path.exists()
                else {}
            )
            result.update(
                status=status, idle_namespaces=count, case=str(case), verified=False
            )
        result["order"] = order
        results.append(result)
        (args.output / "results.json").write_text(json.dumps(results, indent=2) + "\n")
        print(json.dumps(result), flush=True)
    expected_status = "measured" if args.measurement_only else "ok"
    if any(result["status"] != expected_status for result in results):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
