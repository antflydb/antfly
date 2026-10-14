#!/usr/bin/env python3
"""Run each stateful decoder fuzz target with a fresh cache and retain evidence."""

import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys

TARGETS = {
    "vp8": "sequence VP8",
    "vp9": "sequence VP9",
    "av1": "sequence AV1",
    "annex-b": "sequence Annex B",
}


def parse_report(log: str, target: str) -> dict:
    # Some Zig fuzz crashes have returned build status zero. Require a complete
    # report for the requested target and independently reject crash output.
    if re.search(
        r"panic:|segmentation fault|bus error|error:|FAIL", log, re.IGNORECASE
    ):
        raise ValueError("campaign contains failure or crash output")
    reports = re.findall(
        r'Fuzz test: "([^"]+)"[^\n]*\nRuns: (\d+) -> (\d+)\n'
        r"Unique runs: (\d+) -> (\d+)\nCoverage: (\d+)/(\d+) -> (\d+)/(\d+)",
        log,
    )
    if len(reports) != 1 or TARGETS[target] not in reports[0][0]:
        raise ValueError("missing, ambiguous or incorrect target report")
    (
        name,
        before,
        runs,
        unique_before,
        unique,
        edges_before,
        total_before,
        edges,
        total,
    ) = reports[0]
    if any(int(value) for value in (before, unique_before, edges_before)):
        raise ValueError("campaign did not start with a fresh corpus")
    if not (int(runs) > 0 and int(unique) > 0 and 0 < int(edges) <= int(total)):
        raise ValueError("invalid campaign counts")
    if total_before != total:
        raise ValueError("instrumentation changed during campaign")
    if not re.search(r"Build Summary: \d+/\d+ steps succeeded", log):
        raise ValueError("missing successful build summary")
    return {
        "test": name,
        "runs": int(runs),
        "unique_runs": int(unique),
        "coverage_edges": int(edges),
        "total_instrumented_edges": int(total),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--zig", default="zig")
    parser.add_argument("--iterations", type=int, default=100_000)
    parser.add_argument("--target", action="append", choices=tuple(TARGETS))
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    if args.iterations <= 0:
        parser.error("--iterations must be positive")
    # No reuse: cumulative cache counts can obscure the coverage of this run.
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    zig_root = Path(__file__).resolve().parents[3]
    sources = sorted((zig_root / "lib/video/src").rglob("*.zig"))
    sources += sorted((zig_root / "lib/image/src").rglob("*.zig"))
    sources += [
        zig_root / "lib/video/video_fuzz_root.zig",
        zig_root / "lib/video/build_support.zig",
    ]
    sources += sorted((zig_root / "lib/video/testdata/sequence-fuzz").glob("*"))
    sources += sorted((zig_root / "lib/video/testdata/vpx-av1").glob("*.ivf"))
    sources += sorted((zig_root / "lib/video/testdata/av1-extended").glob("*.ivf"))
    receipt = {
        "source_sha256": {
            str(path.relative_to(zig_root)): hashlib.sha256(
                path.read_bytes()
            ).hexdigest()
            for path in sources
            if path.is_file()
        },
        "campaigns": {},
        "status": "running",
    }
    receipt_path = output / "receipt.json"

    def save() -> None:
        receipt_path.write_text(json.dumps(receipt, indent=2) + "\n")

    save()
    for target in args.target or TARGETS:
        command = [
            args.zig,
            "build",
            "fuzz-video",
            "-Doptimize=Debug",
            "-Dvideo-fuzz-filter=" + TARGETS[target],
            "--cache-dir",
            str(output / (target + "-cache")),
            "--fuzz=" + str(args.iterations),
            "--summary",
            "all",
        ]
        print(f"Running {target}; log: {output / (target + '.log')}", flush=True)
        campaign = {"command": command, "status": "running"}
        receipt["campaigns"][target] = campaign
        save()
        log_path = output / (target + ".log")
        with log_path.open("w") as log:
            process = subprocess.run(
                command, cwd=zig_root, stdout=log, stderr=subprocess.STDOUT
            )
        campaign["exit_code"] = process.returncode
        try:
            if process.returncode != 0:
                raise ValueError("nonzero build exit status")
            campaign.update(parse_report(log_path.read_text(), target))
            campaign["status"] = "passed"
        except ValueError as error:
            campaign.update(status="failed", reason=str(error))
            receipt["status"] = "failed"
            save()
            print(f"{target}: {error}; inspect {log_path}", file=sys.stderr)
            return 1
        save()
    receipt["status"] = "passed"
    save()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
