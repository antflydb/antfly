"""Detect workload failures hidden by successful VectorDBBench retries."""

from collections import Counter
from pathlib import Path


def inspect_workload_errors(root: Path):
    required = [root / "vdbbench-live.log", root / "antfly-initial.log"]
    paths = sorted({*required, *root.glob("antfly-*.log")})
    counts = Counter()
    examples = []
    for path in paths:
        if not path.is_file():
            continue
        with path.open(errors="replace") as stream:
            for number, line in enumerate(stream, 1):
                kind = None
                if "Antfly insert error:" in line:
                    kind = "client_insert_error"
                elif "Insert failed," in line:
                    kind = "client_insert_retry"
                elif "public table batch failed" in line:
                    kind = "server_batch_error"
                elif "VectorPayloadStorePoisoned" in line:
                    kind = "source_store_poisoned"
                elif path.name.startswith("antfly-") and "OutOfMemory" in line:
                    kind = "server_out_of_memory"
                if kind is not None:
                    counts[kind] += 1
                    if counts[kind] <= 4:
                        examples.append({
                            "kind": kind,
                            "log": path.name,
                            "line": number,
                            "message": line.strip()[:1000],
                        })
    missing = [p.name for p in required if not p.is_file()]
    return {
        "qualified": not counts and not missing,
        "counts": dict(counts),
        "missing_logs": missing,
        "examples": examples,
    }
