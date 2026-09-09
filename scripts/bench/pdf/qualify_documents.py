"""Qualify profiled two-consumer PDF reuse across precommit/replay and memory caps.

Reuses the real remote-PDF harness's structural/content signatures. These are
diagnostics, never throughput ratios or an OCR accuracy oracle.
"""

import argparse
import re
from collections import Counter
from pathlib import Path
from types import SimpleNamespace

from compare import output_signature, run_subject, save
from render_matrix import render_observations

SCHEMA = "antfly.pdf.document_qualification.v1"


def evaluate_run(run, log, trials, memory_bytes):
    errors = []
    results = run.get("results", [])
    if (
        run.get("returncode") != 0
        or len(results) != trials
        or not all(row.get("passed") is True for row in results)
    ):
        errors.append("incomplete or failed indexing")
    if not run.get("metal_confirmed"):
        errors.append("existing harness did not confirm model backend selection")
    provenance = run.get("provenance", {})
    if (
        provenance.get("mode") != "always"
        or provenance.get("consumers") != 2
        or provenance.get("read_profile") is not True
        or provenance.get("render_memory_bytes") != memory_bytes
    ):
        errors.append("missing forced-OCR/two-consumer/profile/memory controls")
    signatures = []
    for row in results:
        try:
            signature = output_signature(row)
            if (
                len(signature["consumer_results"]) != 1
                or not signature["unit_text_sha256"]
                or not signature["unit_render_geometry"]
            ):
                raise ValueError("missing two-consumer content/page evidence")
            signatures.append(signature)
        except (KeyError, TypeError, ValueError) as exc:
            errors.append(str(exc))
    observations = render_observations(log)
    renders = [row for row in observations if row["phase"] == "pdf_render"]
    windows = [row for row in observations if row["phase"] == "pdf_render_window"]
    identities = Counter()
    try:
        for row in renders:
            source = row["source_fingerprint"]
            page = int(row["page"])
            if (
                not source
                or source == "null"
                or page < 1
                or row.get("failure") != "null"
            ):
                raise ValueError("failed/unattributed physical render")
            identities[(source, page)] += 1
        expected = sum(row["pages"] for row in results)
        if (
            expected <= 0
            or len(renders) != expected
            or any(count != trials for count in identities.values())
        ):
            errors.append(
                "physical renders do not cover each source/page exactly once per trial"
            )
        if not windows:
            errors.append("missing render-window admission evidence")
        for row in windows:
            peak = int(row["peak_bytes"])
            active = int(row["peak_parallelism"])
            requested = int(row["requested_parallelism"])
            if (
                row.get("failure") != "null"
                or not 0 < peak <= memory_bytes
                or not 1 <= active <= requested
            ):
                errors.append(
                    "failed window or invalid tracked memory/parallelism bounds"
                )
    except (KeyError, TypeError, ValueError) as exc:
        errors.append(f"invalid profile evidence: {exc}")
    return {
        "pass": not errors,
        "errors": errors,
        "signatures": signatures,
        "physical_renders": len(renders),
        "windows": windows,
    }


def summarize(runs, trials):
    checks = []
    reference = None
    identity = None
    for entry in runs:
        run = entry["run"]
        result = evaluate_run(run, entry["log"], trials, entry["memory_bytes"])
        result.update(
            sync_level=entry["sync_level"], memory_bytes=entry["memory_bytes"]
        )
        if run.get("provenance", {}).get("sync_level") != entry["sync_level"]:
            result["errors"].append("sync-level provenance mismatch")
        current_identity = {
            key: run.get(key) for key in ("models", "table_config", "server_config")
        }
        current_identity["provenance"] = {
            key: run.get("provenance", {}).get(key)
            for key in (
                "binary_sha256",
                "revision",
                "selected",
                "circus_revision",
                "suite",
                "reader_batch_size",
                "render_workers",
                "render_prefetch",
            )
        }
        if any(value is None for value in current_identity.values()) or any(
            value is None for value in current_identity["provenance"].values()
        ):
            result["errors"].append("missing pinned artifact/configuration identity")
        if identity is None:
            identity = current_identity
        elif current_identity != identity:
            result["errors"].append("binary/model/corpus/configuration drift")
        for signature in result.pop("signatures"):
            if reference is None:
                reference = signature
            elif reference != signature:
                result["errors"].append(
                    "content/page/vector signature differs across paths or memory caps"
                )
        result["pass"] = not result["errors"]
        checks.append(result)
    controls = [(entry["sync_level"], entry["memory_bytes"]) for entry in runs]
    caps = {cap for _, cap in controls}
    complete = (
        len(caps) >= 2
        and len(controls) == len(set(controls))
        and set(controls)
        == {(sync, cap) for sync in ("full_index", "write") for cap in caps}
    )
    return {
        "schema": SCHEMA,
        "pass": complete and all(check["pass"] for check in checks),
        "complete": complete,
        "checks": checks,
        "limitations": [
            "Profiled: no throughput claim.",
            "Tracked window admission is not peak RSS or GPU residency.",
            "Structural/text parity is not OCR semantic accuracy.",
            "Current PDF benchmark pins Florence/BGE and verifies Metal; other task/backend document lanes remain unqualified.",
        ],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("work-dir", "circus-dir", "binary", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--name", required=True)
    parser.add_argument(
        "--suite",
        choices=("text", "small", "throughput", "qualification"),
        default="text",
    )
    parser.add_argument(
        "--memory-bytes", type=int, nargs="+", default=[268435456, 134217728]
    )
    parser.add_argument("--trials", type=int, default=2)
    parser.add_argument("--timeout", type=int, default=300)
    parser.add_argument("--port", type=int, default=29700)
    args = parser.parse_args()
    if not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_.-]*", args.name) or not re.fullmatch(
        r"[0-9a-f]{40}", args.revision
    ):
        parser.error("name must be a safe directory name and revision a full SHA")
    if (
        min(args.memory_bytes + [args.trials, args.timeout]) <= 0
        or len(set(args.memory_bytes)) < 2
        or len(set(args.memory_bytes)) != len(args.memory_bytes)
    ):
        parser.error("require positive controls and at least two distinct memory caps")
    args.output.mkdir(parents=True, exist_ok=False)
    runs = []
    for sync in ("full_index", "write"):
        for cap in args.memory_bytes:
            current = SimpleNamespace(
                **vars(args),
                pr_binary=args.binary,
                pr_revision=args.revision,
                sync_level=sync,
                render_memory_bytes=cap,
                mode="always",
                consumers=2,
                reader_batch_size=4,
                render_workers=4,
                render_prefetch=1,
                read_profile=True,
            )
            run = run_subject(current, args.output, len(runs), "pr")
            log_path = args.work_dir / run["name"] / "antfly.log"
            runs.append(
                {
                    "sync_level": sync,
                    "memory_bytes": cap,
                    "run": run,
                    "log": log_path.read_text() if log_path.exists() else "",
                }
            )
            save(args.output / f"run-{len(runs):02d}.json", runs[-1])
            save(args.output / "summary.json", summarize(runs, args.trials))
    return 0 if summarize(runs, args.trials)["pass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
