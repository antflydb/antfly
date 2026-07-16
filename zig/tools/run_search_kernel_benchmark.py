#!/usr/bin/env python3
"""Reproducible persistent-process runner for the V1 search-kernel protocol.

The runner keeps adapter processes alive, correctness-gates every query before
timing, retains raw per-query samples, and writes the artifact bundle specified
by FULL_TEXT_PERFORMANCE.md.  A comparator is optional for local regression
runs and mandatory for cross-engine claims.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import shlex
import shutil
import statistics
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_QUERIES = ROOT / "bench/full_text/testdata/search_benchmark_queries.jsonl"
DEFAULT_ANALYZER = ROOT / "bench/full_text/testdata/search_benchmark_analyzer.jsonl"


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--corpus", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--index-dir", required=True, type=Path)
    parser.add_argument("--queries", type=Path, default=DEFAULT_QUERIES)
    parser.add_argument("--analyzer-fixtures", type=Path, default=DEFAULT_ANALYZER)
    parser.add_argument("--segment-mode", choices=("single", "production"), default="single")
    parser.add_argument("--bm25-k1", type=float, default=1.2)
    parser.add_argument("--bm25-b", type=float, default=0.75)
    parser.add_argument("--merge-max-segments-per-tier", type=int)
    parser.add_argument("--merge-max-at-once", type=int)
    parser.add_argument("--postings-chunk-size", type=int)
    parser.add_argument("--warmup-queries", type=int, default=100)
    parser.add_argument("--warmup-seconds", type=float, default=1.0)
    parser.add_argument("--repetitions", type=int, default=5)
    parser.add_argument(
        "--resource-profile-seconds",
        type=float,
        default=2.0,
        help="minimum wall time per engine/query class for CPU and RSS profiling (0 disables)",
    )
    parser.add_argument("--shuffle-seed", type=int, default=7349)
    parser.add_argument("--reuse-index", action="store_true")
    parser.add_argument("--antfly-manifest", type=Path, help="manifest for --reuse-index")
    parser.add_argument(
        "--refresh-reused-index-layout",
        action="store_true",
        help="read the current settled layout from the adapter after out-of-band maintenance",
    )
    parser.add_argument(
        "--reuse-index-extra-elapsed-ns",
        type=int,
        default=0,
        help="maintenance elapsed time to add to a reused index manifest",
    )
    parser.add_argument("--antfly-index-command")
    parser.add_argument("--antfly-query-command")
    parser.add_argument("--comparator-query-command")
    parser.add_argument("--comparator-index-command")
    parser.add_argument("--comparator-index-dir", type=Path)
    parser.add_argument("--comparator-manifest", type=Path, help="manifest for a reused comparator index")
    parser.add_argument("--comparator-analyzer-command")
    parser.add_argument("--abs-score-tol", type=float, default=1e-5)
    parser.add_argument("--rel-score-tol", type=float, default=1e-5)
    parser.add_argument("--skip-diagnostics", action="store_true")
    args = parser.parse_args()
    if args.repetitions < 5:
        parser.error("--repetitions must be at least 5")
    if args.warmup_queries < 1 or args.warmup_seconds < 0:
        parser.error("warmup settings must be non-negative and include a query")
    if args.resource_profile_seconds < 0:
        parser.error("--resource-profile-seconds must be non-negative")
    if not (args.bm25_k1 >= 0 and 0 <= args.bm25_b <= 1):
        parser.error("invalid BM25 parameters")
    if args.merge_max_segments_per_tier is not None and args.merge_max_segments_per_tier < 2:
        parser.error("--merge-max-segments-per-tier must be at least 2")
    if args.merge_max_at_once is not None and args.merge_max_at_once < 2:
        parser.error("--merge-max-at-once must be at least 2")
    if args.postings_chunk_size is not None and (
        args.postings_chunk_size < 64 or args.postings_chunk_size & (args.postings_chunk_size - 1)
    ):
        parser.error("--postings-chunk-size must be a power of two of at least 64")
    if args.comparator_index_command and not (args.comparator_index_dir and args.comparator_query_command):
        parser.error("comparator indexing requires --comparator-index-dir and --comparator-query-command")
    if args.reuse_index and not args.antfly_manifest:
        parser.error("--reuse-index requires --antfly-manifest")
    if args.reuse_index and args.postings_chunk_size is not None:
        parser.error("--postings-chunk-size cannot be used with --reuse-index")
    if args.refresh_reused_index_layout and not args.reuse_index:
        parser.error("--refresh-reused-index-layout requires --reuse-index")
    if args.reuse_index_extra_elapsed_ns < 0:
        parser.error("--reuse-index-extra-elapsed-ns must be non-negative")
    if args.comparator_query_command and not args.comparator_index_command and not args.comparator_manifest:
        parser.error("a reused comparator requires --comparator-manifest")
    if args.comparator_query_command and (args.merge_max_segments_per_tier is not None or args.merge_max_at_once is not None):
        parser.error("merge-policy overrides are Antfly-only engineering runs and cannot be used with a comparator")
    return args


def command(raw: str | None, default: list[str], **values: object) -> list[str]:
    parts = shlex.split(raw) if raw else default
    return [part.format(**values) for part in parts]


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for line_number, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not raw.strip():
            continue
        value = json.loads(raw)
        if not isinstance(value, dict):
            raise ValueError(f"{path}:{line_number}: expected object")
        records.append(value)
    return records


def write_json(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def copy_manifest(source: Path, destination: Path) -> None:
    if source.resolve() != destination.resolve():
        shutil.copyfile(source, destination)


def assert_compatible_manifests(left: dict[str, Any], right: dict[str, Any]) -> None:
    for key_path in (("query_grammar",), ("segment_mode",), ("corpus", "sha256"), ("corpus", "indexed_documents"), ("analysis",), ("bm25",)):
        left_value: Any = left
        right_value: Any = right
        for key in key_path:
            left_value = left_value[key]
            right_value = right_value[key]
        if left_value != right_value:
            raise RuntimeError(f"index manifest mismatch at {'.'.join(key_path)}: {left_value!r} != {right_value!r}")


def assert_settled_antfly_layout(manifest: dict[str, Any]) -> None:
    """Reject timing an index that still has maintenance work or a bad layout."""
    layout = manifest.get("layout")
    if not isinstance(layout, dict):
        raise RuntimeError("Antfly index manifest is missing layout data")
    segments = layout.get("segments")
    if not isinstance(segments, list):
        raise RuntimeError("Antfly index manifest is missing segment data")
    if manifest.get("segment_mode") == "single" and len(segments) != 1:
        raise RuntimeError(f"single segment mode requires exactly one segment, found {len(segments)}")

    merge_stats = layout.get("merge_stats")
    if not isinstance(merge_stats, dict):
        raise RuntimeError("Antfly index manifest is missing merge debt data")
    debt_fields = (
        "in_flight_merges",
        "in_flight_segments",
        "pending_indexes",
        "pending_segments",
        "pending_bytes",
        "pending_mmap_bytes",
        "pending_heap_bytes",
        "quarantined_merges",
        "quarantined_segments",
    )
    debt = {field: merge_stats.get(field) for field in debt_fields if merge_stats.get(field, 0) != 0}
    if debt:
        raise RuntimeError(f"Antfly index is not settled; merge debt remains: {debt}")
    if merge_stats.get("failed_merges", 0) != 0:
        raise RuntimeError(f"Antfly index recorded failed merges: {merge_stats['failed_merges']}")


def directory_bytes(path: Path) -> int:
    return sum(item.stat().st_size for item in path.rglob("*") if item.is_file())


def positive_manifest_metric(manifest: dict[str, Any], key: str) -> int | None:
    """Return a persisted measurement without accepting booleans or sentinels."""
    value = manifest.get(key)
    if isinstance(value, int) and not isinstance(value, bool) and value > 0:
        return value
    return None


def rss_bytes(pid: int) -> int | None:
    try:
        value = subprocess.check_output(
            ["ps", "-o", "rss=", "-p", str(pid)], text=True, stderr=subprocess.DEVNULL
        ).strip()
        return int(value) * 1024 if value else None
    except (OSError, subprocess.SubprocessError, ValueError):
        return None


def parse_ps_cpu_time(raw: str) -> int:
    """Parse ps utime/stime ([[dd-]hh:]mm:ss.cc) into nanoseconds."""
    value = raw.strip()
    days = 0
    if "-" in value:
        day_text, value = value.split("-", 1)
        days = int(day_text)
    fields = value.split(":")
    if len(fields) == 3:
        hours, minutes, seconds = fields
    elif len(fields) == 2:
        hours = "0"
        minutes, seconds = fields
    else:
        raise ValueError(f"unsupported ps CPU time: {raw!r}")
    total_seconds = days * 86_400 + int(hours) * 3_600 + int(minutes) * 60 + float(seconds)
    return int(total_seconds * 1_000_000_000)


def process_resources(pid: int) -> dict[str, int] | None:
    try:
        raw = subprocess.check_output(
            ["ps", "-o", "utime=,stime=,rss=", "-p", str(pid)],
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
        if not raw:
            return None
        fields = raw.split()
        if len(fields) != 3:
            return None
        return {
            "user_cpu_ns": parse_ps_cpu_time(fields[0]),
            "system_cpu_ns": parse_ps_cpu_time(fields[1]),
            "rss_bytes": int(fields[2]) * 1024,
        }
    except (OSError, subprocess.SubprocessError, ValueError):
        return None


class PeakSampler:
    def __init__(self, pid: int):
        self.pid = pid
        self.peak = 0
        self.last_resources: dict[str, int] | None = None
        self.stop = threading.Event()
        self.thread = threading.Thread(target=self._run, daemon=True)

    def _run(self) -> None:
        while not self.stop.wait(0.01):
            resources = process_resources(self.pid)
            if resources is not None:
                self.last_resources = resources
                self.peak = max(self.peak, resources["rss_bytes"])

    def __enter__(self) -> "PeakSampler":
        self.thread.start()
        return self

    def __exit__(self, *_: object) -> None:
        self.stop.set()
        self.thread.join()
        resources = process_resources(self.pid)
        if resources is not None:
            self.last_resources = resources
            self.peak = max(self.peak, resources["rss_bytes"])


def wait_with_live_stderr(proc: subprocess.Popen[str]) -> str:
    """Relay long-running index progress while retaining diagnostics."""
    lines: list[str] = []

    def relay() -> None:
        assert proc.stderr
        for line in proc.stderr:
            lines.append(line)
            sys.stderr.write(line)
            sys.stderr.flush()

    thread = threading.Thread(target=relay, daemon=True)
    thread.start()
    proc.wait()
    thread.join()
    return "".join(lines)


class Adapter:
    def __init__(self, argv: list[str]):
        self.argv = argv
        self.process: subprocess.Popen[str] | None = None

    def start(self) -> None:
        self.process = subprocess.Popen(
            self.argv,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )

    def ask(self, request: str) -> tuple[str, int]:
        assert self.process and self.process.stdin and self.process.stdout
        started = time.perf_counter_ns()
        self.process.stdin.write(request + "\n")
        self.process.stdin.flush()
        response = self.process.stdout.readline()
        elapsed = time.perf_counter_ns() - started
        if not response:
            stderr = self.process.stderr.read() if self.process.stderr else ""
            raise RuntimeError(f"adapter exited during request: {stderr}")
        return response.rstrip("\n"), elapsed

    def close(self) -> None:
        if not self.process:
            return
        if self.process.stdin:
            self.process.stdin.close()
        try:
            self.process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self.process.terminate()
            self.process.wait(timeout=10)
        if self.process.returncode:
            stderr = self.process.stderr.read() if self.process.stderr else ""
            raise RuntimeError(f"adapter exited {self.process.returncode}: {stderr}")

    @property
    def pid(self) -> int:
        assert self.process
        return self.process.pid


def git_metadata() -> dict[str, Any]:
    def run(*args: str) -> str:
        return subprocess.check_output(args, cwd=ROOT, text=True).strip()

    try:
        return {
            "commit": run("git", "rev-parse", "HEAD"),
            "dirty": bool(run("git", "status", "--porcelain")),
        }
    except (OSError, subprocess.SubprocessError):
        return {"commit": None, "dirty": None}


def corpus_metadata(path: Path) -> dict[str, Any]:
    digest = hashlib.sha256()
    size = 0
    documents = 0
    with path.open("rb") as source:
        for raw in source:
            digest.update(raw)
            size += len(raw)
            if raw.strip():
                documents += 1
    return {"path": str(path), "source_sha256": digest.hexdigest(), "source_bytes": size, "nonblank_lines": documents}


def percentile(values: list[int], fraction: float) -> int:
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, max(0, int((len(ordered) - 1) * fraction + 0.999999)))]


def summarize(samples: list[int]) -> dict[str, Any]:
    return {
        "samples": len(samples),
        "min_ns": min(samples),
        "max_ns": max(samples),
        "median_ns": int(statistics.median(samples)),
        "p50_ns": percentile(samples, 0.50),
        "p95_ns": percentile(samples, 0.95),
        "p99_ns": percentile(samples, 0.99),
    }


def verify_analyzer(adapter: Adapter, fixtures: list[dict[str, Any]]) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for fixture in fixtures:
        raw, _ = adapter.ask(f"ANALYZE\t{fixture['text']}")
        if raw == "UNSUPPORTED":
            raise RuntimeError("adapter does not support ANALYZE")
        output.append({"name": fixture["name"], "text": fixture["text"], "analysis": json.loads(raw)})
    return output


def main() -> int:
    args = arguments()
    args.output.mkdir(parents=True, exist_ok=False)
    index_manifest = args.output / "index-manifest.json"
    bin_dir = ROOT / "zig-out/bin"
    comparator_manifest = args.output / "comparator-index-manifest.json"
    values = {
        "index": args.index_dir,
        "comparator_index": args.comparator_index_dir or "",
        "comparator_manifest": comparator_manifest,
        "root": ROOT,
        "corpus": args.corpus,
    }
    index_argv = command(args.antfly_index_command, [str(bin_dir / "search_benchmark_index"), str(args.index_dir)], **values)
    index_argv += ["--segment-mode", args.segment_mode, "--manifest", str(index_manifest), "--bm25-k1", str(args.bm25_k1), "--bm25-b", str(args.bm25_b)]
    query_argv = command(args.antfly_query_command, [str(bin_dir / "search_benchmark_query"), str(args.index_dir)], **values)
    query_argv += ["--bm25-k1", str(args.bm25_k1), "--bm25-b", str(args.bm25_b)]
    if args.merge_max_segments_per_tier is not None:
        merge_args = ["--merge-max-segments-per-tier", str(args.merge_max_segments_per_tier)]
        index_argv += merge_args
        query_argv += merge_args
    if args.merge_max_at_once is not None:
        merge_args = ["--merge-max-at-once", str(args.merge_max_at_once)]
        index_argv += merge_args
        query_argv += merge_args
    if args.postings_chunk_size is not None:
        index_argv += ["--postings-chunk-size", str(args.postings_chunk_size)]

    index_elapsed = None
    index_peak_rss = None
    index_cpu_ns = None
    if not args.reuse_index:
        if args.index_dir.exists():
            shutil.rmtree(args.index_dir)
        started = time.perf_counter_ns()
        with args.corpus.open("r", encoding="utf-8") as source:
            proc = subprocess.Popen(index_argv, stdin=source, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
            with PeakSampler(proc.pid) as sampler:
                stderr = wait_with_live_stderr(proc)
            if proc.returncode:
                raise RuntimeError(f"indexer exited {proc.returncode}: {stderr}")
            (args.output / "index-stderr.log").write_text(stderr, encoding="utf-8")
            index_peak_rss = sampler.peak or None
            if sampler.last_resources:
                index_cpu_ns = sampler.last_resources["user_cpu_ns"] + sampler.last_resources["system_cpu_ns"]
        index_elapsed = time.perf_counter_ns() - started
    else:
        copy_manifest(args.antfly_manifest, index_manifest)
        if args.refresh_reused_index_layout:
            refresh_adapter = Adapter(query_argv)
            refresh_adapter.start()
            try:
                raw_layout, _ = refresh_adapter.ask("COMPACT\t")
            finally:
                refresh_adapter.close()
            if raw_layout == "UNSUPPORTED":
                raise RuntimeError("Antfly adapter does not support reused-index layout refresh")
            live_layout = json.loads(raw_layout)["layout"]
            refreshed = json.loads(index_manifest.read_text(encoding="utf-8"))
            refreshed["segment_mode"] = args.segment_mode
            refreshed["layout"] = live_layout
            if len(live_layout["segments"]) != 1:
                raise RuntimeError(f"refreshed single index has {len(live_layout['segments'])} segments")
            previous_elapsed = refreshed.get("index_elapsed_ns")
            if isinstance(previous_elapsed, int):
                refreshed["index_elapsed_ns"] = previous_elapsed + args.reuse_index_extra_elapsed_ns
            refreshed["resumed_index"] = {
                "layout_refreshed": True,
                "extra_maintenance_elapsed_ns": args.reuse_index_extra_elapsed_ns,
            }
            write_json(index_manifest, refreshed)
    if not index_manifest.exists():
        raise RuntimeError("index manifest is required (reuse-index must point at an archived build)")

    comparator_index_elapsed = None
    comparator_index_peak_rss = None
    comparator_index_cpu_ns = None
    if args.comparator_index_command:
        assert args.comparator_index_dir
        if args.comparator_index_dir.exists():
            shutil.rmtree(args.comparator_index_dir)
        comparator_argv = command(args.comparator_index_command, [], **values)
        started = time.perf_counter_ns()
        with args.corpus.open("r", encoding="utf-8") as source:
            proc = subprocess.Popen(comparator_argv, stdin=source, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
            with PeakSampler(proc.pid) as sampler:
                stderr = wait_with_live_stderr(proc)
            if proc.returncode:
                raise RuntimeError(f"comparator indexer exited {proc.returncode}: {stderr}")
            (args.output / "comparator-index-stderr.log").write_text(stderr, encoding="utf-8")
            comparator_index_peak_rss = sampler.peak or None
            if sampler.last_resources:
                comparator_index_cpu_ns = sampler.last_resources["user_cpu_ns"] + sampler.last_resources["system_cpu_ns"]
        comparator_index_elapsed = time.perf_counter_ns() - started
        if not comparator_manifest.exists():
            raise RuntimeError("comparator index command did not emit {comparator_manifest}")
    elif args.comparator_manifest:
        copy_manifest(args.comparator_manifest, comparator_manifest)

    # Configuration compatibility is a preflight gate. Do this before either
    # query adapter starts so an incompatible run cannot leave timing files.
    index_data = json.loads(index_manifest.read_text(encoding="utf-8"))
    if index_peak_rss is None:
        internal_peak = index_data.get("process_peak_rss_bytes")
        if isinstance(internal_peak, int) and internal_peak > 0:
            index_peak_rss = internal_peak
    if index_cpu_ns is None:
        internal_cpu = index_data.get("process_cpu_ns")
        if isinstance(internal_cpu, int) and internal_cpu > 0:
            index_cpu_ns = internal_cpu
    assert_settled_antfly_layout(index_data)
    comparator_index_data = json.loads(comparator_manifest.read_text(encoding="utf-8")) if comparator_manifest.exists() else None
    if comparator_index_data:
        assert_compatible_manifests(index_data, comparator_index_data)

    # A reused index is still an indexed artifact. Preserve the measurements
    # recorded when it was built instead of emitting nulls that make reuse look
    # cheaper or less well-qualified than a same-process build.
    if index_elapsed is None:
        index_elapsed = positive_manifest_metric(index_data, "index_elapsed_ns")
    if index_cpu_ns is None:
        index_cpu_ns = positive_manifest_metric(index_data, "process_cpu_ns")
    if index_peak_rss is None:
        index_peak_rss = positive_manifest_metric(index_data, "process_peak_rss_bytes")
    if comparator_index_data:
        if comparator_index_elapsed is None:
            comparator_index_elapsed = positive_manifest_metric(comparator_index_data, "index_elapsed_ns")
        if comparator_index_cpu_ns is None:
            comparator_index_cpu_ns = positive_manifest_metric(comparator_index_data, "process_cpu_ns")
        if comparator_index_peak_rss is None:
            comparator_index_peak_rss = positive_manifest_metric(comparator_index_data, "process_peak_rss_bytes")

    queries = read_jsonl(args.queries)
    fixtures = read_jsonl(args.analyzer_fixtures)
    adapter = Adapter(query_argv)
    reopen_started = time.perf_counter_ns()
    adapter.start()
    first_query = queries[0]
    adapter.ask(f"VERIFY_TOP_{first_query['top_k']}_COUNT\t{first_query['query']}")
    reopen_ns = time.perf_counter_ns() - reopen_started
    antfly_analysis = verify_analyzer(adapter, fixtures)
    write_json(args.output / "analyzer-antfly.json", antfly_analysis)

    comparator = None
    if args.comparator_query_command:
        comparator = Adapter(command(args.comparator_query_command, [], **values))
        comparator.start()
        comparator_analysis = verify_analyzer(comparator, fixtures)
        write_json(args.output / "analyzer-comparator.json", comparator_analysis)
        if antfly_analysis != comparator_analysis:
            raise RuntimeError("analyzer token streams differ; see analyzer artifacts")

    correctness_antfly: list[dict[str, Any]] = []
    correctness_comparator: list[dict[str, Any]] = []
    for query in queries:
        request = f"VERIFY_TOP_{query['top_k']}_COUNT\t{query['query']}"
        raw, _ = adapter.ask(request)
        if raw == "UNSUPPORTED":
            raise RuntimeError(f"Antfly rejected declared query: {query['query']}")
        record = json.loads(raw)
        record["query"] = query["query"]
        correctness_antfly.append(record)
        if comparator:
            other_raw, _ = comparator.ask(request)
            if other_raw == "UNSUPPORTED":
                raise RuntimeError(f"comparator rejected declared query: {query['query']}")
            other = json.loads(other_raw)
            other["query"] = query["query"]
            correctness_comparator.append(other)

    antfly_correctness_path = args.output / "correctness-antfly.jsonl"
    antfly_correctness_path.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in correctness_antfly), encoding="utf-8")
    if comparator:
        comparator_path = args.output / "correctness-comparator.jsonl"
        comparator_path.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in correctness_comparator), encoding="utf-8")
        verify_argv = [sys.executable, str(ROOT / "tools/verify_search_benchmark.py"), str(antfly_correctness_path), str(comparator_path), "--abs-score-tol", str(args.abs_score_tol), "--rel-score-tol", str(args.rel_score_tol), "--diagnostics", str(args.output / "correctness.json")]
        subprocess.run(verify_argv, check=True)
    else:
        write_json(args.output / "correctness.json", {"schema_version": 1, "ok": True, "cross_engine": False, "queries": len(queries)})

    if not args.skip_diagnostics:
        diagnostic_rows: list[dict[str, Any]] = []
        for query in queries:
            raw, _ = adapter.ask(f"PROFILE_TOP_{query['top_k']}\t{query['query']}")
            if raw == "UNSUPPORTED":
                raise RuntimeError(f"Antfly rejected diagnostic query: {query['query']}")
            record = json.loads(raw)
            record["query"] = query["query"]
            record["class"] = query["class"]
            diagnostic_rows.append(record)
        (args.output / "diagnostics-antfly.jsonl").write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in diagnostic_rows), encoding="utf-8")

    # Warm both adapters until both the query-count and wall-time minima hold.
    warm_started = time.perf_counter()
    warm_count = 0
    while warm_count < args.warmup_queries or time.perf_counter() - warm_started < args.warmup_seconds:
        query = queries[warm_count % len(queries)]
        adapter.ask(f"TOP_{query['top_k']}\t{query['query']}")
        if comparator:
            comparator.ask(f"TOP_{query['top_k']}\t{query['query']}")
        warm_count += 1

    import random
    rng = random.Random(args.shuffle_seed)
    orders: list[list[int]] = []
    for repetition in range(args.repetitions):
        order = list(range(len(queries)))
        rng.shuffle(order)
        orders.append(order)

    def measure(engine: str, target: Adapter) -> dict[str, list[int]]:
        samples: dict[str, list[int]] = {}
        rows_by_class: dict[str, list[dict[str, Any]]] = {}
        for repetition, order in enumerate(orders):
            for index in order:
                query = queries[index]
                response, elapsed = target.ask(f"TOP_{query['top_k']}\t{query['query']}")
                if response != "1":
                    raise RuntimeError(f"{engine} invalid timed acknowledgement: {response!r}")
                query_class = query["class"]
                samples.setdefault(query_class, []).append(elapsed)
                rows_by_class.setdefault(query_class, []).append({"schema_version": 1, "engine": engine, "repetition": repetition, "query_index": index, "query": query["query"], "operation": "top_k", "elapsed_ns": elapsed})
        for query_class, rows in rows_by_class.items():
            prefix = "queries" if engine == "antfly-zig" else f"{engine}-queries"
            (args.output / f"{prefix}-{query_class}.jsonl").write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in rows), encoding="utf-8")
        return samples

    def resource_profile(engine: str, target: Adapter) -> dict[str, Any]:
        """Measure child CPU time and RSS separately from latency sampling.

        `ps` CPU accounting has centisecond resolution on macOS, so each class
        runs for a minimum wall duration. The protocol round trip and the
        parent sampler are not charged to child CPU time.
        """
        if args.resource_profile_seconds == 0:
            return {"enabled": False, "classes": {}}

        def snapshot() -> dict[str, int | None] | None:
            if engine != "antfly-zig":
                return process_resources(target.pid)
            response, _ = target.ask("RESOURCES\t")
            try:
                resources = json.loads(response)
                return {
                    "user_cpu_ns": int(resources["user_cpu_ns"]),
                    "system_cpu_ns": int(resources["system_cpu_ns"]),
                    # getrusage exposes a process high-water mark, not current RSS.
                    "rss_bytes": None,
                    "peak_rss_bytes": int(resources["peak_rss_bytes"]),
                }
            except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
                raise RuntimeError(f"invalid Antfly RESOURCES response: {response!r}") from error

        rows: dict[str, Any] = {}
        for query_class in sorted({query["class"] for query in queries}):
            class_queries = [query for query in queries if query["class"] == query_class]
            before = snapshot()
            started = time.perf_counter_ns()
            count = 0
            with PeakSampler(target.pid) as sampler:
                while count < 100 or time.perf_counter_ns() - started < args.resource_profile_seconds * 1_000_000_000:
                    query = class_queries[count % len(class_queries)]
                    response, _ = target.ask(f"TOP_{query['top_k']}\t{query['query']}")
                    if response != "1":
                        raise RuntimeError(f"{engine} invalid resource-profile acknowledgement: {response!r}")
                    count += 1
            elapsed_ns = time.perf_counter_ns() - started
            after = snapshot()
            cpu = None
            if before is not None and after is not None:
                user_ns = max(0, after["user_cpu_ns"] - before["user_cpu_ns"])
                system_ns = max(0, after["system_cpu_ns"] - before["system_cpu_ns"])
                total_ns = user_ns + system_ns
                cpu = {
                    "user_ns": user_ns,
                    "system_ns": system_ns,
                    "total_ns": total_ns,
                    "per_query_ns": total_ns // count,
                    "utilization_percent": total_ns * 100.0 / elapsed_ns,
                }
            rows[query_class] = {
                "queries": count,
                "wall_ns": elapsed_ns,
                "wall_per_query_ns": elapsed_ns // count,
                "cpu": cpu,
                "rss_start_bytes": before["rss_bytes"] if before else None,
                "rss_end_bytes": after["rss_bytes"] if after else None,
                "rss_peak_bytes": sampler.peak or (after.get("peak_rss_bytes") if after else None),
            }
        return {"enabled": True, "minimum_wall_seconds_per_class": args.resource_profile_seconds, "classes": rows}

    class_samples = measure("antfly-zig", adapter)
    comparator_samples = measure("comparator", comparator) if comparator else None
    resource_profiles = {"antfly-zig": resource_profile("antfly-zig", adapter)}
    if comparator:
        resource_profiles["comparator"] = resource_profile("comparator", comparator)
    antfly_steady_rss = rss_bytes(adapter.pid)
    comparator_steady_rss = rss_bytes(comparator.pid) if comparator else None
    adapter.close()
    if comparator:
        comparator.close()

    write_json(args.output / "segments.json", index_data["layout"])
    write_json(args.output / "indexing.json", {
        "antfly-zig": {"elapsed_ns": index_elapsed, "cpu_ns": index_cpu_ns, "peak_rss_bytes": index_peak_rss, "database_bytes": directory_bytes(args.index_dir), "text_index_bytes": index_data["layout"]["total_bytes"]},
        **({"comparator": {"elapsed_ns": comparator_index_elapsed, "cpu_ns": comparator_index_cpu_ns, "peak_rss_bytes": comparator_index_peak_rss, "index_bytes": directory_bytes(args.comparator_index_dir)}} if args.comparator_index_dir and args.comparator_index_dir.exists() else {}),
    })
    write_json(args.output / "resources.json", {"schema_version": 1, "engines": resource_profiles})
    write_json(args.output / "memory.json", {
        "schema_version": 2,
        "engines": {
            "antfly-zig": {
                "index_peak_rss_bytes": index_peak_rss,
                "query_steady_rss_bytes": antfly_steady_rss,
                "query_peak_rss_bytes": max(
                    (row["rss_peak_bytes"] or 0 for row in resource_profiles["antfly-zig"]["classes"].values()),
                    default=0,
                ) or None,
            },
            **({
                "comparator": {
                    "index_peak_rss_bytes": comparator_index_peak_rss,
                    "query_steady_rss_bytes": comparator_steady_rss,
                    "query_peak_rss_bytes": max(
                        (row["rss_peak_bytes"] or 0 for row in resource_profiles["comparator"]["classes"].values()),
                        default=0,
                    ) or None,
                }
            } if comparator else {}),
        },
    })
    summary = {name: summarize(samples) for name, samples in sorted(class_samples.items())}
    comparator_summary = {name: summarize(samples) for name, samples in sorted(comparator_samples.items())} if comparator_samples else None
    write_json(args.output / "summary.json", {"schema_version": 1, "engines": {"antfly-zig": summary, **({"comparator": comparator_summary} if comparator_summary else {})}})
    manifest = {
        "schema_version": 1,
        "query_grammar": "V1",
        "engine": "antfly-zig",
        "git": git_metadata(),
        "corpus": corpus_metadata(args.corpus),
        "index_manifest": index_data,
        "comparator_index_manifest": comparator_index_data,
        "segment_mode": args.segment_mode,
        "bm25": {"k1": args.bm25_k1, "b": args.bm25_b},
        "merge_policy_override": {"max_segments_per_tier": args.merge_max_segments_per_tier, "max_merge_at_once": args.merge_max_at_once},
        "build": {"optimization": "ReleaseFast", "python": sys.version, "argv": sys.argv},
        "environment": {"platform": platform.platform(), "machine": platform.machine(), "processor": platform.processor(), "logical_cpus": os.cpu_count()},
        "measurement": {"warmup_queries": warm_count, "warmup_min_seconds": args.warmup_seconds, "repetitions": args.repetitions, "shuffle_seed": args.shuffle_seed, "resource_profile_seconds": args.resource_profile_seconds},
        "reopen_ns": reopen_ns,
        "comparator": bool(comparator),
        "unsupported_queries": 0,
    }
    write_json(args.output / "manifest.json", manifest)
    print(json.dumps({"output": str(args.output), "summary": summary}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
