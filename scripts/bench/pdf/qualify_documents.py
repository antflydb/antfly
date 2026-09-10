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


def expected_pages(result, selected):
    """Bind corpus page ranges to the fingerprints published by indexing."""
    manifests = result["manifests"]
    sources = {row["path"]: row["pages"] for row in selected}
    if (
        not sources
        or len(sources) != len(selected)
        or any(
            set(mapping) != set(sources)
            for mapping in (
                manifests,
                result["unit_render_geometry"],
                result["unit_text_sha256"],
            )
        )
    ):
        raise ValueError("manifest sources do not match the selected corpus")
    expected = Counter()
    fingerprints = set()
    for path, pages in sources.items():
        fingerprint = manifests[path]["source_fingerprint"]
        if (
            not isinstance(fingerprint, str)
            or not fingerprint
            or fingerprint == "null"
            or fingerprint in fingerprints
            or type(pages) is not int
            or pages <= 0
        ):
            raise ValueError("missing/ambiguous source identity or invalid page count")
        fingerprints.add(fingerprint)
        geometry = result["unit_render_geometry"][path]
        actual_pages = [item["page_number"] for item in geometry.values()]
        if (
            any(type(page) is not int for page in actual_pages)
            or sorted(actual_pages) != list(range(1, pages + 1))
            or set(geometry) != set(result["unit_text_sha256"][path])
        ):
            raise ValueError("retained units do not cover the corpus page range")
        expected.update((fingerprint, page) for page in range(1, pages + 1))
    if result["pages"] != sum(sources.values()):
        raise ValueError("result page count differs from corpus")
    return expected


def verify_trial_profile(observations, expected, memory_bytes, workers):
    """Require exact render and admission coverage, including partial windows."""
    renders = Counter()
    windows = Counter()
    for row in observations:
        phase = row["phase"]
        if phase not in ("pdf_render", "pdf_render_window"):
            continue
        source = row["source_fingerprint"]
        if row.get("failure") != "null":
            raise ValueError("failed physical render/window")
        if phase == "pdf_render":
            renders[(source, int(row["page"]))] += 1
            continue
        first, last, count = (
            int(row[key]) for key in ("first_page", "last_page", "pages")
        )
        if not 0 < count == last - first + 1 <= len(expected):
            raise ValueError("invalid render-window page range")
        windows.update((source, page) for page in range(first, last + 1))
        peak = int(row["peak_bytes"])
        active = int(row["peak_parallelism"])
        requested = int(row["requested_parallelism"])
        if not 0 < peak <= memory_bytes or not 1 <= active <= requested <= workers:
            raise ValueError("invalid tracked memory/parallelism bounds")
    if renders != expected:
        raise ValueError(
            "physical renders do not cover expected source/pages once in this trial"
        )
    if windows != expected:
        raise ValueError(
            "render windows do not cover expected source/pages once in this trial"
        )


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
            consumer = signature["consumer_results"][0]
            for primary, secondary in (
                ("unit_text_sha256", "unit_text_sha256"),
                ("unit_render_geometry", "unit_render_geometry"),
                ("documents", "manifest_counts"),
                ("vectors", "searchable_vectors"),
            ):
                if signature[primary] != consumer[secondary]:
                    raise ValueError(f"identical consumers differ: {primary}")
            signatures.append(signature)
        except (KeyError, TypeError, ValueError) as exc:
            errors.append(str(exc))
    # Keep byte offsets exact (including CRLF/non-ASCII log content).
    raw_log = log.encode("utf-8") if isinstance(log, str) else log
    observations = render_observations(raw_log.decode("utf-8"))
    renders = [row for row in observations if row["phase"] == "pdf_render"]
    windows = [row for row in observations if row["phase"] == "pdf_render_window"]
    try:
        workers = provenance["render_workers"]
        if type(workers) is not int or workers <= 0:
            raise ValueError("missing renderer worker bound")
        cursor = 0
        excluded = []
        for trial, row in enumerate(results):
            bounds = row["profile_log"]
            start, end = bounds["start_byte"], bounds["end_byte"]
            if (
                row["trial"] != trial
                or type(start) is not int
                or type(end) is not int
                or not cursor <= start < end <= len(raw_log)
                or (start > 0 and raw_log[start - 1 : start] != b"\n")
                or raw_log[end - 1 : end] != b"\n"
            ):
                raise ValueError("missing/invalid per-trial log boundaries")
            excluded.append(raw_log[cursor:start])
            cursor = end
            verify_trial_profile(
                render_observations(raw_log[start:end].decode("utf-8")),
                expected_pages(row, provenance["selected"]),
                memory_bytes,
                workers,
            )
        excluded.append(raw_log[cursor:])
        if any(
            row["phase"] in ("pdf_render", "pdf_render_window")
            for part in excluded
            for row in render_observations(part.decode("utf-8"))
        ):
            raise ValueError("render evidence exists outside indexed trial boundaries")
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
                    "log": log_path.read_bytes().decode("utf-8")
                    if log_path.exists()
                    else "",
                }
            )
            save(args.output / f"run-{len(runs):02d}.json", runs[-1])
            save(args.output / "summary.json", summarize(runs, args.trials))
    return 0 if summarize(runs, args.trials)["pass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
