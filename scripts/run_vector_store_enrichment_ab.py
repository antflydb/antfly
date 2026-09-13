#!/usr/bin/env python3
"""Repeated fresh-table A/B/BA enrichment comparisons on the available host.

Pins executable and workload hashes, records host load, sampled process RSS,
readiness, query tails, all on-disk files, and restart readiness. Host idleness
is not required; report individual paired ratios and variability.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import statistics
import subprocess
import sys
import threading
import time

import httpx

from vector_store_disk_accounting import inventory
from vector_store_experiment_settings import ALL_FLAGS, TREATMENTS, configure


def digest(path):
    with path.open("rb") as stream:
        result = hashlib.sha256()
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            result.update(chunk)
    return result.hexdigest()


def save(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


def stop(server):
    if server.poll() is not None:
        return
    server.terminate()
    try:
        server.wait(timeout=60)
    except subprocess.TimeoutExpired:
        server.kill()
        server.wait()
        raise RuntimeError("server required forced shutdown")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--pairs", type=int, default=2)
    parser.add_argument(
        "--refinement",
        choices=list(TREATMENTS),
        help="Compare control/candidate settings on vector_store tables using the same binary.",
    )
    parser.add_argument("--rows", type=int, default=4000)
    parser.add_argument("--queries", type=int, default=1000)
    parser.add_argument("--port", type=int, default=18098)
    parser.add_argument("--encoding", choices=["float32", "float16"], default="float32")
    args = parser.parse_args()
    if args.pairs < 2:
        parser.error("at least two pairs are needed for A/B then B/A")
    args.root.mkdir(parents=True, exist_ok=False)
    binary = args.binary.resolve(strict=True)
    binary_hash = digest(binary)
    # Freeze the workload script so concurrent worktree edits cannot mix arms.
    workload = args.root.resolve() / "profile_vector_store_enrichment.py"
    workload.write_bytes(Path(__file__).with_name(workload.name).read_bytes())
    workload_hash = digest(workload)
    environment = os.environ.copy()
    environment.update(
        ANTFLY_HBC_POSTING_SIDECAR="1",
        ANTFLY_HBC_POSTING_WAL_STORE="1",
        ANTFLY_HBC_VECTOR_BLOCK_STORE="1",
        ANTFLY_HBC_VECTOR_BLOCK_ENCODING=args.encoding,
    )
    result = {
        "refinement": args.refinement,
        "binary": str(binary),
        "binary_sha256": binary_hash,
        "workload_sha256": workload_hash,
        "encoding": args.encoding,
        "rows": args.rows,
        "arms": [],
    }
    save(args.root / "comparison.json", result)
    base = f"http://127.0.0.1:{args.port}/db/v1/tables"
    for pair in range(args.pairs):
        labels = (
            ["control", "candidate"]
            if args.refinement
            else ["primary_lsm", "vector_store"]
        )
        modes = labels if pair % 2 == 0 else list(reversed(labels))
        for mode in modes:
            if digest(binary) != binary_hash or digest(workload) != workload_hash:
                raise RuntimeError("measurement inputs changed")
            root = args.root.resolve() / f"{pair + 1}-{mode}"
            root.mkdir()
            arm_environment = environment.copy()
            table_mode = "vector_store" if args.refinement else mode
            if args.refinement:
                arm_environment = configure(
                    environment, args.refinement, mode == "candidate"
                )
            arm = {
                "pair": pair + 1,
                "mode": mode,
                "table_mode": table_mode,
                "refinement_environment": {
                    k: arm_environment.get(k) for k in ALL_FLAGS
                },
                "load_before": os.getloadavg(),
                "rss_samples_kib": [],
            }
            result["arms"].append(arm)
            save(args.root / "comparison.json", result)
            command = [
                str(binary),
                "standalone",
                "--host",
                "127.0.0.1",
                "--port",
                str(args.port),
                "--health-port",
                str(args.port + 1),
                "--auth",
                "false",
                "--process-memory-budget-mb",
                "4096",
                "--data-dir",
                str(root / "data"),
            ]
            with (root / "server.log").open("w") as log:
                server = subprocess.Popen(
                    command, stdout=log, stderr=subprocess.STDOUT, env=arm_environment
                )
                done = threading.Event()

                def sample():
                    while not done.wait(1):
                        try:
                            raw = subprocess.check_output(
                                ["ps", "-o", "rss=", "-p", str(server.pid)], text=True
                            )
                            arm["rss_samples_kib"].append(int(raw.strip()))
                        except (OSError, ValueError, subprocess.CalledProcessError):
                            pass

                sampler = threading.Thread(target=sample, daemon=True)
                sampler.start()
                try:
                    with httpx.Client(timeout=10) as client:

                        def ready(table=False):
                            deadline = time.monotonic() + 180
                            while time.monotonic() < deadline:
                                if server.poll() is not None:
                                    raise RuntimeError(
                                        "server exited; inspect server.log"
                                    )
                                try:
                                    response = client.get(
                                        base + ("/source_enrichment" if table else "")
                                    )
                                    if response.status_code == 200:
                                        value = response.json()
                                        if not table:
                                            return value
                                        status = (
                                            client.get(
                                                base
                                                + "/source_enrichment/indexes/semantic"
                                            )
                                            .json()
                                            .get("status", {})
                                        )
                                        if (
                                            status.get("readiness", {}).get("complete")
                                            and status.get("query_visible_doc_count")
                                            == args.rows
                                        ):
                                            return value
                                except httpx.HTTPError:
                                    pass
                                time.sleep(0.1)
                            raise RuntimeError("readiness deadline exceeded")

                        ready()
                        subprocess.run(
                            [
                                sys.executable,
                                str(workload),
                                "--port",
                                str(args.port),
                                "--mode",
                                table_mode,
                                "--rows",
                                str(args.rows),
                                "--queries",
                                str(args.queries),
                                "--output",
                                str(root / "enrichment.json"),
                            ],
                            check=True,
                        )
                        arm["enrichment"] = json.loads(
                            (root / "enrichment.json").read_text()
                        )
                        stop(server)
                        save(root / "disk-after-stop.json", inventory(root / "data"))
                        started = time.perf_counter()
                        server = subprocess.Popen(
                            command,
                            stdout=log,
                            stderr=subprocess.STDOUT,
                            env=arm_environment,
                        )
                        arm["table_after_restart"] = ready(table=True)
                        arm["restart_ready_s"] = time.perf_counter() - started
                        stop(server)
                        disk = inventory(root / "data")
                        save(root / "disk-after-restart.json", disk)
                        arm["total_logical_bytes"] = disk["total_logical_bytes"]
                        arm["total_allocated_bytes"] = disk["total_allocated_bytes"]
                        arm["disk_groups"] = disk["groups"]
                    if digest(binary) != binary_hash:
                        raise RuntimeError("binary changed during arm")
                except Exception as error:
                    arm["error"] = str(error)
                    raise
                finally:
                    done.set()
                    sampler.join()
                    stop(server)
                    arm["load_after"] = os.getloadavg()
                    arm["sampled_peak_rss_kib"] = max(
                        arm["rss_samples_kib"], default=None
                    )
                    save(args.root / "comparison.json", result)
            print(
                f"{pair + 1} {mode}: readiness {[p['ready_s'] for p in arm['enrichment']['phases']]}, disk {arm['total_logical_bytes']}",
                flush=True,
            )
    summaries = {}
    for mode in (
        ["control", "candidate"] if args.refinement else ["primary_lsm", "vector_store"]
    ):
        arms = [arm for arm in result["arms"] if arm["mode"] == mode]
        summaries[mode] = {}
        metrics = {
            "initial_ready_s": lambda arm: arm["enrichment"]["phases"][0]["ready_s"],
            "update_ready_s": lambda arm: arm["enrichment"]["phases"][1]["ready_s"],
            "semantic_qps": lambda arm: arm["enrichment"]["semantic"]["qps"],
            "semantic_p99_ms": lambda arm: arm["enrichment"]["semantic"]["p99_ms"],
            "semantic_failed_queries": lambda arm: arm["enrichment"]["semantic"][
                "failed_queries"
            ],
            "full_text_qps": lambda arm: arm["enrichment"]["full_text"]["qps"],
            "full_text_failed_queries": lambda arm: arm["enrichment"]["full_text"][
                "failed_queries"
            ],
            "total_logical_bytes": lambda arm: arm["total_logical_bytes"],
            "sampled_peak_rss_kib": lambda arm: arm["sampled_peak_rss_kib"],
            "restart_ready_s": lambda arm: arm["restart_ready_s"],
        }
        source_counters = (
            "prepare_requests",
            "prepare_batches",
            "prepared_payloads",
            "prepare_lock_wait_ns",
            "decode_outside_lock_ns",
            "durable_append_ns",
            "snapshot_read_ns",
            "directory_bytes_written",
            "directory_entries",
            "directory_hits",
            "directory_misses",
            "source_segments",
            "ownership_index_collections",
            "ownership_index_entries_scanned",
            "location_cache_hits",
            "location_cache_misses",
            "location_cache_bytes",
            "cache_reclaimed_bytes",
            "retired_ann_references_skipped",
            "retained_payload_bytes",
            "collection_bytes_written",
            "collection_mark_ns",
            "checkpoint_bytes_written",
            "heap_bytes",
        )
        for counter in source_counters:
            metrics["source_" + counter] = lambda arm, key=counter: (
                arm["enrichment"]
                .get("table", {})
                .get("storage_status", {})
                .get("source_vectors")
                or {}
            ).get(key)
        for name, get in metrics.items():
            values = [get(arm) for arm in arms]
            available = [value for value in values if value is not None]
            summaries[mode][name] = {
                "median": statistics.median(available) if available else None,
                "min": min(available) if available else None,
                "max": max(available) if available else None,
                "runs": values,
            }
    result["summary"] = summaries
    result["qualified"] = all(arm["enrichment"]["qualified"] for arm in result["arms"])
    result["paired_ratios"] = [
        {
            name: (source / primary if source is not None and primary else None)
            for name, get in metrics.items()
            for source in [
                get(
                    next(
                        a
                        for a in result["arms"]
                        if a["pair"] == pair
                        and a["mode"]
                        == ("candidate" if args.refinement else "vector_store")
                    )
                )
            ]
            for primary in [
                get(
                    next(
                        a
                        for a in result["arms"]
                        if a["pair"] == pair
                        and a["mode"]
                        == ("control" if args.refinement else "primary_lsm")
                    )
                )
            ]
        }
        for pair in range(1, args.pairs + 1)
    ]
    save(args.root / "comparison.json", result)
    if not result["qualified"]:
        raise SystemExit(
            "enrichment query failures disqualify this comparison; inspect comparison.json"
        )


if __name__ == "__main__":
    main()
