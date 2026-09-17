#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
#
# Licensed under the Elastic License 2.0 (ELv2); you may not use this file
# except in compliance with the Elastic License 2.0. You may obtain a copy of
# the Elastic License 2.0 at
#
#     https://www.antfly.io/licensing/ELv2-license
#
# Unless required by applicable law or agreed to in writing, software distributed
# under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# Elastic License 2.0 for the specific language governing permissions and
# limitations.

"""Compare skewed tenant migrations under foreground traffic, serially.

Uses the production E2E workload and retains unsuccessful runs. Run from zig/:
uv run --project e2e/antfly python tools/benchmark_schema_migrations.py \
    --binary zig-out/bin/antfly --baseline /path/to/baseline --output results.json
"""

import argparse
import hashlib
import json
import os
import platform
import shutil
import signal
import subprocess
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCENARIO = "skewed_schema_migrations_with_foreground_traffic"


def positive(value):
    result = int(value)
    if result < 1:
        raise argparse.ArgumentTypeError("must be positive")
    return result


def digest(path):
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--baseline", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--samples", type=positive, default=3)
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--large-docs", type=positive, default=10000)
    parser.add_argument("--small-tenants", type=positive, default=5)
    parser.add_argument("--traffic-clients", type=positive, default=1)
    parser.add_argument("--min-free-disk-gib", type=positive, default=8)
    args = parser.parse_args()
    if args.warmups < 0:
        parser.error("warmups must be nonnegative")
    args.output = args.output.resolve()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    binaries = {"candidate": args.binary.resolve()}
    if args.baseline:
        binaries = {"baseline": args.baseline.resolve(), **binaries}
    result = {
        "scenario": SCENARIO,
        "platform": platform.platform(),
        "configuration": {
            "large_documents": args.large_docs,
            "small_tenants": args.small_tenants,
            "traffic_clients": args.traffic_clients,
            "warmups_per_binary": args.warmups,
            "samples_per_binary": args.samples,
        },
        "binaries": {
            label: {"path": str(path), "sha256": digest(path)}
            for label, path in binaries.items()
        },
        "source_head": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
        ).strip(),
        "source_diff_sha256": hashlib.sha256(
            subprocess.check_output(["git", "diff", "HEAD"], cwd=ROOT)
        ).hexdigest(),
        "runs": [],
        "limitations": [
            "Fresh three-metadata/three-data process cluster per run; provisioning excluded from workload timings.",
            "Closed-loop traffic; compare completion times and raw request distributions together, not as fixed offered-load SLOs.",
            "Warmups are retained and labeled; failed runs are retained, never included as successful latency samples.",
        ],
    }
    # E2E routing selects the production API prefix from the executable name.
    # Copy each immutable input once; arbitrary baseline filenames must not
    # silently select the legacy fixture's routes. Copies are outside timings.
    staging = tempfile.TemporaryDirectory(prefix="antfly-migration-benchmark-")
    try:
        staged = {}
        for label, binary in binaries.items():
            destination = Path(staging.name) / label / "antfly"
            destination.parent.mkdir()
            shutil.copy2(binary, destination)
            if digest(destination) != result["binaries"][label]["sha256"]:
                raise RuntimeError(f"{label} binary changed while staging")
            staged[label] = destination
        run_comparison(args, result, staged)
    finally:
        staging.cleanup()


def run_comparison(args, result, binaries):
    # Alternate revisions, while never running competing task-owned workloads.
    for iteration in range(args.warmups + args.samples):
        for label, binary in binaries.items():
            free_disk_bytes = shutil.disk_usage(tempfile.gettempdir()).free
            if free_disk_bytes < args.min_free_disk_gib * 1024**3:
                result["stopped_before_run"] = {
                    "binary": label,
                    "iteration": iteration,
                    "reason": "insufficient free disk space",
                    "free_disk_bytes": free_disk_bytes,
                    "required_free_disk_gib": args.min_free_disk_gib,
                }
                args.output.write_text(json.dumps(result, indent=2) + "\n")
                raise SystemExit(
                    "Insufficient free disk space for another cluster sample."
                )
            log_path = args.output.with_suffix(f".{label}.{iteration}.log")
            env = {
                **os.environ,
                "ANTFLY_BIN": str(binary),
                "ANTFLY_MIGRATION_LARGE_DOCS": str(args.large_docs),
                "ANTFLY_MIGRATION_SMALL_TENANTS": str(args.small_tenants),
                "ANTFLY_MIGRATION_TRAFFIC_CLIENTS": str(args.traffic_clients),
            }
            start = time.monotonic()
            timed_out = False
            interrupted = False
            with log_path.open("w") as log:
                try:
                    proc = subprocess.Popen(
                        [
                            "uv",
                            "run",
                            "--project",
                            "e2e/antfly",
                            "pytest",
                            "-q",
                            "-s",
                            "e2e/antfly/test_catalog_resilience.py::test_skewed_schema_migrations_keep_foreground_traffic_available",
                        ],
                        cwd=ROOT,
                        env=env,
                        stdout=log,
                        stderr=subprocess.STDOUT,
                        start_new_session=True,
                    )
                    exit_code = proc.wait(timeout=900)
                except (subprocess.TimeoutExpired, KeyboardInterrupt) as error:
                    timed_out = isinstance(error, subprocess.TimeoutExpired)
                    exit_code = None
                    # The pytest fixture owns six server children. Bound the
                    # entire process group, including children left behind if
                    # pytest exits before fixture teardown completes.
                    try:
                        os.killpg(proc.pid, signal.SIGTERM)
                    except ProcessLookupError:
                        pass
                    try:
                        proc.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        pass
                    try:
                        os.killpg(proc.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    proc.wait()
                    interrupted = isinstance(error, KeyboardInterrupt)
            observations = []
            seen_observations = set()
            log_text = log_path.read_text()
            disk_exhausted = "NoSpaceLeft" in log_text
            for line in log_text.splitlines():
                if f'"scenario": "{SCENARIO}"' not in line:
                    continue
                try:
                    record = json.loads(line[line.index("{") :])
                except (ValueError, json.JSONDecodeError):
                    continue
                if record.get("scenario") == SCENARIO:
                    # Pytest repeats assertion payloads in failure reports.
                    # Preserve the failed run, but count its JSON sample once.
                    identity = json.dumps(record, sort_keys=True)
                    if identity not in seen_observations:
                        seen_observations.add(identity)
                        observations.append(record)
            run = {
                "binary": label,
                "iteration": iteration,
                "warmup": iteration < args.warmups,
                "exit_code": exit_code,
                "timed_out": timed_out,
                "interrupted": interrupted,
                "environment_failure": "disk_exhaustion" if disk_exhausted else None,
                "elapsed_seconds": time.monotonic() - start,
                "free_disk_bytes_before": free_disk_bytes,
                "log": str(log_path),
                "observations": observations,
            }
            result["runs"].append(run)
            args.output.write_text(json.dumps(result, indent=2) + "\n")
            print(
                json.dumps(
                    {key: value for key, value in run.items() if key != "observations"}
                ),
                flush=True,
            )
            if interrupted:
                raise KeyboardInterrupt
            if disk_exhausted:
                raise SystemExit("Disk exhausted; stop before another cluster sample.")
            if timed_out:
                raise SystemExit(
                    "Timed out; stop before running another cluster and inspect the recorded log."
                )

    if any(run["exit_code"] != 0 for run in result["runs"]):
        raise SystemExit(
            "One or more workload runs failed; inspect the retained observations and logs."
        )


if __name__ == "__main__":
    main()
