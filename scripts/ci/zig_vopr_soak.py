#!/usr/bin/env python3
"""Run reproducible VOPR shards and merge their replay-validated corpora."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time


def traces(directory):
    return sorted(
        path
        for path in directory.rglob("*.voprtrace")
        if "quarantine" not in path.parts
    )


def trace_digest(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def retain_working_corpus(output, retained, manifest):
    """Bound clean replay cost while preserving every distinct finding."""
    clean_limit = {"production-ha-scaling": 2, "distributed-data-vopr": 8}.get(
        manifest["scenario"], 128
    )
    clean, findings = [], {}
    for item in manifest["artifacts"]:
        path = output / item["path"]
        fingerprints = set()
        with path.open() as stream:
            for line in stream:
                if line.startswith('{"type":"failure",'):
                    fingerprints.add(json.loads(line)["fingerprint"])
        if not fingerprints:
            clean.append(path)
        for fingerprint in fingerprints:
            previous = findings.get(fingerprint)
            if previous is None or (path.stat().st_size, path.name) < (
                previous.stat().st_size,
                previous.name,
            ):
                findings[fingerprint] = path
    selected = set(findings.values()) | set(sorted(clean)[:clean_limit])
    retained.mkdir(parents=True, exist_ok=True)
    if any(retained.iterdir()):
        raise ValueError("retained corpus directory must be empty")
    for path in sorted(selected):
        shutil.copyfile(path, retained / path.name)
    selection = {
        "clean_limit": clean_limit,
        "unique_findings": len(findings),
        "selected": [path.name for path in sorted(selected)],
        "archived": len(manifest["artifacts"]) - len(selected),
    }
    (output / "retention.json").write_text(json.dumps(selection, indent=2) + "\n")


def run_shard(binary, scenario, seed, histories, corpus, output):
    output.mkdir(parents=True, exist_ok=True)
    # Never reuse stale results.json as evidence for a new or interrupted run.
    if (output / "run.json").exists() or (output / "results.json").exists():
        raise ValueError("output directory already contains a run")
    seeded = 0
    for path in traces(corpus):
        digest = trace_digest(path)
        target = output / f"seed-{digest}.voprtrace"
        if not target.exists():
            shutil.copyfile(path, target)
            seeded += 1
    command = [
        str(binary),
        "campaign",
        "--scenario",
        scenario,
        "--histories",
        str(histories),
        "--seed",
        str(seed),
        "--workers",
        "1",
        "--fail-on-findings",
        "--defer-diagnostics",
        "--artifact-dir",
        str(output),
    ]
    provenance = {
        "scenario": scenario,
        "seed": seed,
        "histories": histories,
        "seeded_files": seeded,
        "revision": os.environ.get("GITHUB_SHA"),
        "run_id": os.environ.get("GITHUB_RUN_ID"),
        "command": command,
        "started_at": time.time(),
        "completed": False,
    }
    metadata = output / "run.json"
    metadata.write_text(json.dumps(provenance, indent=2) + "\n")
    with (output / "campaign.log").open("w") as log:
        result = subprocess.run(
            command, stdout=log, stderr=subprocess.STDOUT, check=False
        )
    provenance.update(
        completed=True, exit_code=result.returncode, finished_at=time.time()
    )
    metadata.write_text(json.dumps(provenance, indent=2) + "\n")
    # A successful process without its aggregate report is not a passing soak.
    return result.returncode or (0 if (output / "results.json").is_file() else 1)


def merge_corpus(binary, inputs, output, retained=None):
    candidates = traces(inputs)
    if not candidates:
        raise ValueError("no retained traces were uploaded")
    # Prefer a freshly recorded trace as the compatibility authority. Old seed
    # traces may belong to a previous scenario version and must be quarantined.
    fresh = [path for path in candidates if path.name.startswith("history-")]
    if not fresh:
        raise ValueError("no fresh history is available as the corpus authority")
    base = fresh[0]
    command = [
        str(binary),
        "corpus-merge",
        "--base",
        str(base),
        "--out-dir",
        str(output),
    ]
    seen = {trace_digest(base)}
    for path in candidates:
        digest = trace_digest(path)
        if digest in seen:
            continue
        seen.add(digest)
        command.extend(["--trace", str(path)])
    output.mkdir(parents=True, exist_ok=True)
    with (output / "merge.log").open("w") as log:
        result = subprocess.run(
            command, stdout=log, stderr=subprocess.STDOUT, check=False
        )
    if result.returncode:
        return result.returncode
    manifest = json.loads((output / "index.json").read_text())
    if retained is not None:
        retain_working_corpus(output, retained, manifest)
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a") as stream:
            stream.write(
                f"VOPR corpus: {len(manifest['artifacts'])} retained, "
                f"{len(manifest['quarantine'])} quarantined. "
                "Review index.json and quarantine artifacts before promoting regressions.\n"
            )
    return (
        1
        if any(item["reason"] == "replay_diverged" for item in manifest["quarantine"])
        else 0
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    commands = parser.add_subparsers(dest="command", required=True)
    run = commands.add_parser("run")
    run.add_argument(
        "--scenario",
        choices=("ha", "raft", "distributed-data", "ha-scaling"),
        required=True,
    )
    run.add_argument("--seed", type=lambda value: int(value, 0), required=True)
    run.add_argument("--histories", type=int, required=True)
    run.add_argument("--corpus", type=Path, required=True)
    run.add_argument("--output", type=Path, required=True)
    merge = commands.add_parser("merge")
    merge.add_argument("--inputs", type=Path, required=True)
    merge.add_argument("--output", type=Path, required=True)
    merge.add_argument("--retained", type=Path)
    args = parser.parse_args()
    binary = args.binary.resolve(strict=True)
    if args.command == "run":
        if args.histories < 1 or not 0 <= args.seed < 2**64:
            parser.error("histories must be positive and seed must fit u64")
        return run_shard(
            binary, args.scenario, args.seed, args.histories, args.corpus, args.output
        )
    return merge_corpus(binary, args.inputs, args.output, args.retained)


if __name__ == "__main__":
    sys.exit(main())
