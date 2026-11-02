#!/usr/bin/env python3
"""Declare and run the complete client-concurrency sweep without selecting defaults."""

import argparse
import copy
import hashlib
import json
import pathlib
import subprocess
import sys

CONCURRENCIES = (1, 5, 10, 20, 30, 40, 60, 80, 160)


def sweep_manifest(manifest, concurrency):
    reference = manifest["client_workers"]
    if (
        not isinstance(concurrency, int)
        or isinstance(concurrency, bool)
        or concurrency < 1
        or concurrency > manifest["max_pending_clients"]
    ):
        raise ValueError("concurrency must fit the existing client task envelope")
    if reference < 1 or not manifest.get("runnable_policy"):
        raise ValueError(
            "sweep requires a runnable profile with positive reference concurrency"
        )
    point = copy.deepcopy(manifest)
    point["name"] += "-c" + str(concurrency)
    point["client_workers"] = concurrency
    point["concurrency_sweep"] = dict(
        reference_workers=reference,
        client_workers=concurrency,
        overload_point=concurrency > 80,
    )
    for phase in point["phases"]:
        if phase.get("workload", "mixed") != "cheap":
            # Keep offered pressure per worker fixed. Low-load/recovery and
            # every resource/acceptance threshold remain unchanged.
            phase["offered_per_second"] *= concurrency / reference
    return point


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=pathlib.Path, required=True)
    parser.add_argument("--output-dir", type=pathlib.Path, required=True)
    parser.add_argument("--candidate", type=pathlib.Path)
    parser.add_argument("--baseline", type=pathlib.Path)
    parser.add_argument("--repetitions", type=int, default=4)
    parser.add_argument("--prepare-only", action="store_true")
    args = parser.parse_args()
    if not args.prepare_only and (not args.candidate or not args.baseline):
        parser.error("measurement requires both candidate and main binaries")
    if args.repetitions < 4:
        parser.error("four repetitions are required to rotate all four variants")
    if args.output_dir.exists() and any(args.output_dir.iterdir()):
        parser.error("output directory is not empty; use a fresh sweep directory")
    source = args.manifest.read_bytes()
    manifest = json.loads(source)
    points = [sweep_manifest(manifest, concurrency) for concurrency in CONCURRENCIES]
    manifests = args.output_dir / "manifests"
    manifests.mkdir(parents=True, exist_ok=True)
    records = []
    for point in points:
        concurrency = point["client_workers"]
        path = manifests / ("c%03d.json" % concurrency)
        path.write_text(json.dumps(point, indent=2) + "\n")
        records.append(
            dict(
                concurrency=concurrency,
                manifest=str(path),
                manifest_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
                state="prepared",
            )
        )
    summary = dict(
        source_manifest_sha256=hashlib.sha256(source).hexdigest(),
        concurrencies=list(CONCURRENCIES),
        points=records,
        automatic_defaults_qualified=False,
        measured=False,
        passed=False,
    )
    output = args.output_dir / "sweep.json"

    def publish():
        temporary = output.with_suffix(".tmp")
        temporary.write_text(json.dumps(summary, indent=2) + "\n")
        temporary.replace(output)

    publish()
    if args.prepare_only:
        return 0
    failed = False
    for record in records:
        directory = args.output_dir / ("c%03d" % record["concurrency"])
        command = [
            sys.executable,
            str(pathlib.Path(__file__).with_name("run_matrix.py")),
            "--candidate",
            str(args.candidate),
            "--baseline",
            str(args.baseline),
            "--manifest",
            record["manifest"],
            "--output-dir",
            str(directory),
            "--repetitions",
            str(args.repetitions),
        ]
        record.update(state="running", command=command)
        publish()
        receipt = directory / "matrix.json"
        record["receipt"] = str(receipt)
        try:
            result = subprocess.run(command)
            record["exit_code"] = result.returncode
            matrix = json.loads(receipt.read_text())
            record["matrix_recorded"] = True
            record["passed"] = result.returncode == 0 and matrix.get("passed") is True
        except (OSError, ValueError) as error:
            record.update(passed=False, matrix_recorded=False, reason=str(error))
        record["state"] = "terminal"
        failed |= not record["passed"]
        publish()
    summary.update(
        measured=all(record.get("matrix_recorded", False) for record in records),
        passed=not failed,
    )
    publish()
    return int(failed)


if __name__ == "__main__":
    raise SystemExit(main())
