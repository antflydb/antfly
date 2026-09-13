"""Same-data public-API attribution of recovery switches, not qualification QPS.

Clone an offline qualified data directory for each arm. Run all treatments and
then reverse their order, with independent server lifetimes and a pinned binary.
Does not change effort, recall policy, formats, or the original data directory.
"""

import argparse
import json
import math
import os
import shutil
import socket
import subprocess
import sys
import time
from pathlib import Path

import httpx
from run_posting_locality_ab import (
    ALL_REFINEMENT_FLAGS,
    REFINEMENTS,
    digest,
    validate_subgroup_treatment,
)


def ready(payload):
    status = payload.get("status", {})
    return (
        status.get("readiness", {}).get("state") == "ready"
        and not status.get("backfill_active", True)
        and not status.get("dense_publish_pending", True)
        and not status.get("dense_vector_projection_pending", False)
        and status.get("hbc_posting", {}).get("dirty_postings", 0) == 0
    )


def stop(process):
    if process is None or process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=30)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=10)


def common_treatment_flags(modes, common, *, mixed=False):
    flags = {flag for name in common for flag in REFINEMENTS[name]}
    if any(flags.intersection(REFINEMENTS.get(mode, [])) for mode in modes):
        raise ValueError("common refinements must not overlap the A/B treatment")
    layout_flags = {
        "ANTFLY_EXPERIMENT_SUBGROUPS_4",
        "ANTFLY_EXPERIMENT_SUBGROUPS_8",
        "ANTFLY_EXPERIMENT_SUBGROUPS_16",
    }
    if len(flags & layout_flags) > 1:
        raise ValueError("choose one subgroup layout for the source generation")
    if any(layout_flags.intersection(REFINEMENTS.get(mode, [])) for mode in modes):
        raise ValueError(
            "subgroup layout requires fresh ingest; only routing can be query-only A/B"
        )
    if "incremental_publication" in modes or (
        "incremental_publication" in common and not mixed
    ):
        raise ValueError(
            "publication experiments require fresh ingest and mixed writes"
        )
    return flags


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    parser.add_argument("--source-data", required=True, type=Path)
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--dataset", required=True, type=Path)
    parser.add_argument("--port", type=int, default=19450)
    parser.add_argument("--health-port", type=int, default=19451)
    parser.add_argument("--seconds", type=float, default=20)
    parser.add_argument("--modes", default="control,pages,phases,prediction,combined")
    parser.add_argument("--warmup-count", type=int, default=0)
    parser.add_argument("--mixed-seconds", type=float, default=0)
    parser.add_argument("--mixed-write-rows-per-second", type=float, default=0)
    parser.add_argument(
        "--common-refinement", action="append", default=[], choices=list(REFINEMENTS)
    )
    args = parser.parse_args()
    if args.seconds <= 0 or args.port == args.health_port:
        parser.error("positive duration and distinct ports are required")
    if any(
        not math.isfinite(value) or value < 0
        for value in (args.mixed_seconds, args.mixed_write_rows_per_second)
    ):
        parser.error("mixed duration and offered rate must be finite and non-negative")
    modes = args.modes.split(",")
    if (
        len(set(modes)) != len(modes)
        or not modes
        or any(mode != "control" and mode not in REFINEMENTS for mode in modes)
    ):
        parser.error("modes must be distinct registered refinements or control")
    if args.warmup_count < 0:
        parser.error("warmup-count must not be negative")
    if (
        any(
            mode in modes + args.common_refinement
            for mode in (
                "subgroup_routing",
                "global_subgroup_routing",
                "compact_subgroup_routing",
                "borrowed_pages",
                "compact_borrowed",
            )
        )
        and args.warmup_count == 0
    ):
        parser.error("subgroup routing requires a nonempty fixed-query warmup profile")
    try:
        common_flags = common_treatment_flags(
            modes, args.common_refinement, mixed=args.mixed_seconds > 0
        )
    except ValueError as error:
        parser.error(str(error))
    if any(
        "ANTFLY_EXPERIMENT_PROJECTION_CLUSTERING" in REFINEMENTS.get(mode, [])
        for mode in modes + args.common_refinement
    ):
        parser.error(
            "projection layout experiments require a fresh ingest/rewrite, not a query-only clone"
        )
    source = args.source_data.resolve(strict=True)
    binary = args.binary.resolve(strict=True)
    dataset = args.dataset.resolve(strict=True)
    root = args.root.resolve()
    if root.is_relative_to(source):
        parser.error("output must not be within source data")
    root.mkdir(parents=True, exist_ok=False)
    scripts = Path(__file__).resolve().parent
    inputs = [
        binary,
        Path(__file__).resolve(),
        scripts / "profile_vdbbench_concurrent_tail.py",
        scripts / "profile_vdbbench_public_query.py",
        scripts / "run_posting_locality_ab.py",
        scripts / "sample_macos_process_memory.py",
        dataset / "test.parquet",
        dataset / "neighbors.parquet",
    ]
    if args.mixed_seconds:
        inputs += [
            scripts / "profile_vdbbench_mixed_public_workload.py",
            dataset / "shuffle_train.parquet",
        ]
    expected = {str(path): digest(path) for path in inputs}
    environment = {
        k: v
        for k, v in os.environ.items()
        if not k.startswith(("ANTFLY_", "VDBBENCH_"))
    }
    environment.update(
        ANTFLY_HBC_POSTING_SIDECAR="1",
        ANTFLY_HBC_POSTING_WAL_STORE="1",
        ANTFLY_HBC_VECTOR_BLOCK_STORE="1",
        ANTFLY_HBC_VECTOR_BLOCK_ENCODING="float16",
        ANTFLY_EXPERIMENT_POSTING_LOCAL_PROJECTIONS="0",
    )
    receipts = []

    def save():
        (root / "runs.json").write_text(json.dumps(receipts, indent=2) + "\n")

    def verify():
        for name, checksum in expected.items():
            if digest(Path(name)) != checksum:
                raise RuntimeError(f"input changed during diagnostic: {name}")

    for pair, order in enumerate((modes, list(reversed(modes))), 1):
        for mode in order:
            verify()
            for port in (args.port, args.health_port):
                with socket.socket() as probe:
                    probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                    probe.bind(("127.0.0.1", port))
            arm = root / f"{pair}-{mode}"
            arm.mkdir()
            print(f"Starting {arm.name}", flush=True)
            if sys.platform == "darwin":
                subprocess.run(
                    ["cp", "-cR", str(source), str(arm / "data")], check=True
                )
            else:
                shutil.copytree(source, arm / "data")
            arm_environment = environment.copy()
            for flag in ALL_REFINEMENT_FLAGS:
                arm_environment[flag] = (
                    "1"
                    if flag in common_flags or flag in REFINEMENTS.get(mode, [])
                    else "0"
                )
            receipt = {
                "mode": mode,
                "pair": pair,
                "source_data": str(source),
                "inputs_sha256": expected,
                "started_at": time.time(),
                "diagnostic_only": True,
                "warmup_count": args.warmup_count,
                "mixed_seconds": args.mixed_seconds,
                "mixed_offered_rows_per_second": args.mixed_write_rows_per_second,
                "common_refinements": args.common_refinement,
                "environment": {
                    k: v for k, v in arm_environment.items() if k.startswith("ANTFLY_")
                },
            }
            receipts.append(receipt)
            save()
            server = sampler = None
            try:
                with (arm / "server.log").open("w") as log:
                    server = subprocess.Popen(
                        [
                            str(binary),
                            "standalone",
                            "--host",
                            "127.0.0.1",
                            "--port",
                            str(args.port),
                            "--health-port",
                            str(args.health_port),
                            "--auth",
                            "false",
                            "--data-dir",
                            str(arm / "data"),
                        ],
                        env=arm_environment,
                        stdout=log,
                        stderr=subprocess.STDOUT,
                    )
                receipt["pid"] = server.pid
                save()
                if sys.platform == "darwin":
                    sampler = subprocess.Popen(
                        [
                            sys.executable,
                            str(scripts / "sample_macos_process_memory.py"),
                            "--pid",
                            str(server.pid),
                            "--output",
                            str(arm / "memory.jsonl"),
                        ]
                    )
                deadline = time.monotonic() + 180
                with httpx.Client(timeout=5, trust_env=False) as client:
                    while True:
                        if server.poll() is not None:
                            raise RuntimeError("server exited before readiness")
                        try:
                            response = client.get(
                                f"http://127.0.0.1:{args.port}/db/v1/tables/vdbbench/indexes/vec"
                            )
                            response.raise_for_status()
                            payload = response.json()
                            if ready(payload):
                                (arm / "readiness.json").write_text(
                                    json.dumps(payload, indent=2)
                                )
                                break
                        except (httpx.HTTPError, ValueError):
                            pass
                        if time.monotonic() >= deadline:
                            raise TimeoutError("public readiness deadline exceeded")
                        time.sleep(0.5)
                if args.warmup_count:
                    with (arm / "warmup.log").open("w") as log:
                        subprocess.run(
                            [
                                sys.executable,
                                str(scripts / "profile_vdbbench_public_query.py"),
                                "--dataset",
                                str(dataset),
                                "--port",
                                str(args.port),
                                "--count",
                                str(args.warmup_count),
                                "--output",
                                str(arm / "warmup.json"),
                            ],
                            stdout=log,
                            stderr=subprocess.STDOUT,
                            check=True,
                            timeout=300,
                        )
                observation = validate_subgroup_treatment(
                    arm, arm_environment, "warmup.json"
                )
                if observation is not None:
                    receipt["treatment_observation"] = observation
                    save()
                for concurrency, processes, seconds in [
                    (1, 1, 8),
                    (5, 6, args.seconds),
                ]:
                    name = f"c{concurrency * processes}"
                    with (arm / f"{name}.log").open("w") as log:
                        subprocess.run(
                            [
                                sys.executable,
                                str(scripts / "profile_vdbbench_concurrent_tail.py"),
                                "--dataset",
                                str(dataset),
                                "--port",
                                str(args.port),
                                "--concurrency",
                                str(concurrency),
                                "--processes",
                                str(processes),
                                "--seconds",
                                str(seconds),
                                "--output",
                                str(arm / f"{name}.json"),
                            ],
                            stdout=log,
                            stderr=subprocess.STDOUT,
                            check=True,
                            timeout=180,
                        )
                if args.mixed_seconds:
                    with (arm / "mixed.log").open("w") as log:
                        subprocess.run(
                            [
                                sys.executable,
                                str(
                                    scripts
                                    / "profile_vdbbench_mixed_public_workload.py"
                                ),
                                "--dataset",
                                str(dataset),
                                "--port",
                                str(args.port),
                                "--seconds",
                                str(args.mixed_seconds),
                                "--write-rows-per-second",
                                str(args.mixed_write_rows_per_second),
                                "--output",
                                str(arm / "mixed.json"),
                            ],
                            stdout=log,
                            stderr=subprocess.STDOUT,
                            check=True,
                            timeout=args.mixed_seconds + 240,
                        )
                    with httpx.Client(timeout=5, trust_env=False) as client:
                        status = client.get(
                            f"http://127.0.0.1:{args.port}/db/v1/tables/vdbbench/indexes/vec"
                        )
                        status.raise_for_status()
                        if not ready(status.json()):
                            raise RuntimeError(
                                "native acceleration remains pending after mixed catch-up"
                            )
                verify()
                receipt["passed"] = True
            except BaseException as error:
                receipt["error"] = str(error)
                raise
            finally:
                stop(sampler)
                stop(server)
                receipt["finished_at"] = time.time()
                save()
            print(f"Passed {arm.name}", flush=True)


if __name__ == "__main__":
    main()
