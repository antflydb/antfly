#!/usr/bin/env python3
"""Run reproducible VOPR shards and merge their replay-validated corpora."""

import argparse
import hashlib
import json
import os
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path


def write_json(path, value):
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2) + "\n")
    temporary.replace(path)


def run_process(command, *, stdout, timeout=13800, grace=30):
    """Reserve time for evidence upload, including when a child ignores TERM."""
    started = time.monotonic()
    previous = {}
    interrupted = False

    def interrupt(signum, frame):
        nonlocal interrupted
        # Do not throw between Popen creating a child and returning its handle.
        # The wait loop observes cancellation after launch and owns shutdown.
        interrupted = True

    process = None
    status = "exited"
    try:
        for signum in (signal.SIGTERM, signal.SIGINT):
            previous[signum] = signal.signal(signum, interrupt)
        process = subprocess.Popen(
            command, stdout=stdout, stderr=subprocess.STDOUT, start_new_session=True
        )
        while True:
            remaining = timeout - (time.monotonic() - started)
            if interrupted or remaining <= 0:
                status = "interrupted" if interrupted else "timeout"
                break
            try:
                code = process.wait(timeout=min(remaining, 0.25))
                break
            except subprocess.TimeoutExpired:
                continue
        if status != "exited":
            # A second cancellation must not interrupt shutdown and reaping.
            for signum in previous:
                signal.signal(signum, signal.SIG_IGN)
            try:
                os.killpg(process.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            try:
                process.wait(timeout=grace)
            except subprocess.TimeoutExpired:
                pass
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            finally:
                process.wait()
            code = 124 if status == "timeout" else 130
    finally:
        for signum, handler in previous.items():
            signal.signal(signum, handler)
    result = subprocess.CompletedProcess(command, code)
    result.status = status
    result.elapsed_seconds = time.monotonic() - started
    return result


def result_status(result, report):
    if getattr(result, "status", "exited") != "exited":
        return result.status
    if report is None:
        return "missing_report"
    histories = report.get("histories", {})
    if histories.get("harness_errors", 0):
        return "harness_error"
    if histories.get("replay_divergences", 0):
        return "replay_diverged"
    failed = [
        item["name"]
        for item in report.get("properties", [])
        if item["status"] == "fail"
    ]
    if any(not name.endswith(".history-completes") for name in failed):
        return "property_failure"
    if failed:
        return "incomplete_history"
    return "process_failure" if result.returncode else "passed"


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
        schedule = path.with_suffix(".schedule.json")
        if schedule.is_file():
            shutil.copyfile(schedule, retained / schedule.name)
    selection = {
        "clean_limit": clean_limit,
        "unique_findings": len(findings),
        "selected": [path.name for path in sorted(selected)],
        "archived": len(manifest["artifacts"]) - len(selected),
    }
    (output / "retention.json").write_text(json.dumps(selection, indent=2) + "\n")


def run_shard(
    binary,
    scenario,
    seed,
    histories,
    corpus,
    output,
    *,
    timeout=13800,
    policy="bounded-fair",
    require_seed=False,
):
    output.mkdir(parents=True, exist_ok=True)
    # Never reuse stale results.json as evidence for a new or interrupted run.
    if (output / "run.json").exists() or (output / "results.json").exists():
        raise ValueError("output directory already contains a run")
    seeded = 0
    seed_digests = []
    for path in traces(corpus):
        digest = trace_digest(path)
        target = output / f"seed-{digest}.voprtrace"
        if not target.exists():
            shutil.copyfile(path, target)
            schedule = path.with_suffix(".schedule.json")
            if schedule.is_file():
                shutil.copyfile(schedule, target.with_suffix(".schedule.json"))
            seeded += 1
            seed_digests.append(digest)
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
        "--exploration-policy",
        policy,
        "--artifact-dir",
        str(output),
    ]
    provenance = {
        "scenario": scenario,
        "seed": seed,
        "histories": histories,
        "seeded_files": seeded,
        "seed_digests": sorted(seed_digests),
        "binary_sha256": trace_digest(binary) if binary.is_file() else None,
        "exploration_policy": policy,
        "timeout_seconds": timeout,
        "revision": os.environ.get("GITHUB_SHA"),
        "run_id": os.environ.get("GITHUB_RUN_ID"),
        "command": command,
        "started_at": time.time(),
        "completed": False,
    }
    metadata = output / "run.json"
    write_json(metadata, provenance)
    try:
        if require_seed and not seeded:
            raise ValueError("qualification requires a restored corpus")
        with (output / "campaign.log").open("w") as log:
            result = run_process(command, stdout=log, timeout=timeout)
        report_path = output / "results.json"
        report = json.loads(report_path.read_text()) if report_path.is_file() else None
        status = result_status(result, report)
        consumed = report.get("corpus", {}).get("seeded", 0) if report else 0
        if require_seed and status == "passed" and not consumed:
            status = "corpus_not_consumed"
        provenance.update(
            completed=getattr(result, "status", "exited") == "exited",
            exit_code=result.returncode,
            status=status,
            seeded_entries_consumed=consumed,
            elapsed_seconds=getattr(result, "elapsed_seconds", None),
        )
        return result.returncode or (0 if status == "passed" else 1)
    except BaseException as error:
        provenance.update(status="wrapper_error", error=str(error))
        raise
    finally:
        provenance["finished_at"] = time.time()
        write_json(metadata, provenance)
        for path in output.glob("history-*.schedule.json"):
            schedule = json.loads(path.read_text())
            schedule.update(
                binary_sha256=provenance["binary_sha256"],
                revision=provenance["revision"],
            )
            write_json(path, schedule)


def merge_corpus(binary, inputs, output, retained=None, *, timeout=6600):
    candidates = traces(inputs)
    if not candidates:
        raise ValueError("no retained traces were uploaded")
    unique = {}
    for path in candidates:
        digest = trace_digest(path)
        unique.setdefault(digest, path)
    # The runner owns compatibility, authority selection and exact replay.
    # Prefer fresh histories but validate each unique candidate only once.
    ordered = sorted(
        unique.values(), key=lambda path: (not path.name.startswith("history-"), path)
    )
    output.mkdir(parents=True, exist_ok=True)
    if (output / "merge.json").exists() or (output / "index.json").exists():
        raise ValueError("output directory already contains a merge")
    progress = {
        "completed": False,
        "started_at": time.time(),
        "binary_sha256": trace_digest(binary) if binary.is_file() else None,
        "candidates": len(candidates),
        "unique_candidates": len(ordered),
        "input_bytes": sum(path.stat().st_size for path in ordered),
        "digests": sorted(unique),
        "timeout_seconds": timeout,
    }
    write_json(output / "merge.json", progress)
    with (output / "merge.log").open("w") as log:
        command = [
            str(binary),
            "corpus-merge",
            "--out-dir",
            str(output),
        ]
        for path in ordered:
            command.extend(["--trace", str(path)])
        try:
            result = run_process(command, stdout=log, timeout=timeout)
            progress.update(
                completed=result.returncode == 0 and (output / "index.json").is_file(),
                exit_code=result.returncode,
                status=getattr(result, "status", "exited"),
                elapsed_seconds=getattr(result, "elapsed_seconds", None),
            )
        except BaseException as error:
            progress.update(status="wrapper_error", error=str(error))
            raise
        finally:
            progress["finished_at"] = time.time()
            write_json(output / "merge.json", progress)
    if result.returncode:
        return result.returncode
    if not progress["completed"]:
        return 1
    try:
        manifest = json.loads((output / "index.json").read_text())
        for item in manifest["artifacts"]:
            artifact = output / item["path"]
            if not artifact.is_file():
                raise ValueError("completed corpus references a missing trace")
            source = unique.get(trace_digest(artifact))
            if source is not None:
                schedule = source.with_suffix(".schedule.json")
                if schedule.is_file():
                    shutil.copyfile(schedule, artifact.with_suffix(".schedule.json"))
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
        divergent = any(
            item["reason"] == "replay_diverged" for item in manifest["quarantine"]
        )
        progress["status"] = "replay_diverged" if divergent else "passed"
        return int(divergent)
    except BaseException as error:
        progress.update(completed=False, status="wrapper_error", error=str(error))
        raise
    finally:
        progress["finished_at"] = time.time()
        write_json(output / "merge.json", progress)


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
    run.add_argument("--timeout", type=float, default=13800)
    run.add_argument(
        "--exploration-policy",
        choices=("cooperative", "bounded-fair", "adversarial"),
        default="bounded-fair",
    )
    run.add_argument("--require-seed", action="store_true")
    merge = commands.add_parser("merge")
    merge.add_argument("--inputs", type=Path, required=True)
    merge.add_argument("--output", type=Path, required=True)
    merge.add_argument("--retained", type=Path)
    merge.add_argument("--timeout", type=float, default=6600)
    args = parser.parse_args()
    binary = args.binary.resolve(strict=True)
    if args.timeout <= 0:
        parser.error("timeout must be positive")
    if args.command == "run":
        if args.histories < 1 or not 0 <= args.seed < 2**64:
            parser.error("histories must be positive and seed must fit u64")
        return run_shard(
            binary,
            args.scenario,
            args.seed,
            args.histories,
            args.corpus,
            args.output,
            timeout=args.timeout,
            policy=args.exploration_policy,
            require_seed=args.require_seed,
        )
    return merge_corpus(
        binary, args.inputs, args.output, args.retained, timeout=args.timeout
    )


if __name__ == "__main__":
    sys.exit(main())
