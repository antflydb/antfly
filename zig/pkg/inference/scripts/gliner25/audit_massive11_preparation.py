#!/usr/bin/env python3
"""Verify MASSIVE locks, blinded transport, and an independent repeat; no model."""
from __future__ import annotations

import argparse
from pathlib import Path
import tempfile

import evaluation_contract as evaluation
import oracle
import prepare_massive11 as massive


def inventory(root):
    result = {}
    for path in sorted(root.rglob("*")):
        evaluation.checked(not path.is_symlink(), "prepared tree contains a symbolic link")
        if path.is_file():
            result[str(path.relative_to(root))] = {"size_bytes": path.stat().st_size, "sha256": oracle.sha256_file(path)}
    return result


def validate_transport(root, requests, pin, lock_sha256, prepared_sha256):
    """A shard is only a transport unit; it cannot change the locked test set."""
    transport = oracle.read_json(evaluation.pinned_path(root, pin))
    evaluation.checked(transport.get("scope") == "gliner25_transport_manifest/v1" and
                       transport.get("qualification") is False, "transport scope differs")
    evaluation.checked(transport.get("completion_policy") ==
                       "exact_global_id_coverage_and_order_all_errors_retained_atomically_aggregate_before_quality_report",
                       "transport completion policy differs")
    evaluation.checked(type(transport.get("records")) is int and transport["records"] == len(requests),
                       "transport denominator differs")
    for key, name in (("request_ids_sha256", "request_id"), ("request_sha256s_sha256", "request_sha256")):
        evaluation.checked(transport.get(key) == evaluation.digest(evaluation.encoded([row[name] for row in requests])),
                           "transport global request inventory differs")
    evaluation.checked(len({row["request_id"] for row in requests}) == len(requests), "duplicate global request ID")
    shards = transport.get("shards")
    evaluation.checked(isinstance(shards, list) and 0 < len(shards) <= 128, "invalid transport shard count")
    cursor = 0
    for index, spec in enumerate(shards):
        evaluation.checked(type(spec.get("index")) is int and spec["index"] == index and
                           type(spec.get("start")) is int and spec["start"] == cursor and
                           type(spec.get("end")) is int and cursor < spec["end"] <= len(requests) and
                           spec["end"] - cursor <= massive.MAX_SHARD_ROWS, "transport range has a gap, overlap or excess rows")
        path = evaluation.pinned_path(root, spec["file"])
        evaluation.checked(path.stat().st_size <= 32 * 1024 * 1024, "transport shard exceeds byte cap")
        shard = oracle.read_json(path)
        expected = {"scope": "gliner25_blinded_transport_shard/v1", "qualification": False,
            "lock_sha256": lock_sha256, "prepared_sha256": prepared_sha256, "global_records": len(requests),
            "index": index, "start": cursor, "end": spec["end"], "requests": requests[cursor:spec["end"]]}
        evaluation.checked(shard == expected, "transport shard changed, reordered or disclosed fields")
        cursor = spec["end"]
    evaluation.checked(cursor == len(requests), "transport omitted locked test requests")
    return {"records": cursor, "shards": len(shards), "rows_per_shard": [row["end"] - row["start"] for row in shards],
            "request_ids_sha256": transport["request_ids_sha256"], "request_sha256s_sha256": transport["request_sha256s_sha256"]}


def audit(root, repeat):
    first, second = inventory(root), inventory(repeat)
    evaluation.checked(first == second and bool(first), "independent preparation trees differ")
    summary = oracle.read_json(root / "preparation.json")
    evaluation.checked(summary.get("scope") == "gliner25_massive11_preparation/v1" and
                       summary.get("qualification") is False and summary.get("no_model_execution") is True,
                       "preparation scope differs")
    for key, path in (("source_manifest_sha256", massive.MANIFEST), ("adapter_sha256", Path(massive.__file__)),
                      ("harness_sha256", Path(evaluation.__file__))):
        evaluation.checked(summary.get(key) == oracle.sha256_file(path), "preparation source helper differs")
    manifest = oracle.read_json(massive.MANIFEST)
    profiles = massive.profile_definitions(manifest)
    evaluation.checked([row["profile"] for row in summary["profiles"]] == profiles, "preparation profile inventory differs")
    output = []
    for row in summary["profiles"]:
        directory = root / row["profile"]["id"]
        lock = directory / "lock.json"
        admitted = evaluation.audit(lock)["summary"]
        evaluation.checked(admitted == row["admission"] and oracle.sha256_file(lock) == row["lock_sha256"],
                           "profile admission receipt differs")
        # Reconstruct blinded requests and gold separately from the locked rows,
        # then compare both to the prepared bytes before inspecting transport.
        with tempfile.TemporaryDirectory(prefix="gliner25-massive-audit-") as temporary:
            restored = Path(temporary) / "prepared"
            prepared = evaluation.prepare(lock, restored)
            evaluation.checked(prepared == row["prepared"], "prepared receipt differs")
            for name in ("requests.jsonl", "gold.jsonl", "prepared.json"):
                evaluation.checked((restored / name).read_bytes() == (directory / "prepared" / name).read_bytes(),
                                   "prepared bytes differ from locked test rows")
        requests = list(evaluation.rows(directory / "prepared/requests.jsonl"))
        transport = validate_transport(directory, requests, row["transport"], row["lock_sha256"],
                                       oracle.sha256_file(directory / "prepared/prepared.json"))
        output.append({"profile": row["profile"], "statistics": row["statistics"], "lock_sha256": row["lock_sha256"],
            "prepared_sha256": oracle.sha256_file(directory / "prepared/prepared.json"),
            "requests_sha256": prepared["requests_sha256"], "gold_sha256": prepared["gold_sha256"],
            "transport_sha256": row["transport"]["sha256"], "transport": transport})
    split_audit = summary["split_audit"]
    return {"scope": "gliner25_massive11_preparation_evidence/v1", "qualification": False, "no_model_execution": True,
        "source_manifest_sha256": summary["source_manifest_sha256"], "adapter_sha256": summary["adapter_sha256"],
        "harness_sha256": summary["harness_sha256"], "audit_generator_sha256": oracle.sha256_file(Path(__file__)),
        "download_generator_sha256": oracle.sha256_file(Path(__file__).with_name("download_massive11.py")),
        "preparation_sha256": oracle.sha256_file(root / "preparation.json"),
        "reproducibility": {"independent_directories_equal": True, "files": len(first),
            "tree_inventory_sha256": evaluation.digest(evaluation.encoded(first, sorted_keys=True)),
            "logical_bytes_per_tree": sum(value["size_bytes"] for value in first.values())},
        "source_alignment_counts": summary["source_alignment_counts"],
        "split_audit_sha256": evaluation.digest(evaluation.encoded(split_audit)),
        "excluded_original_ids": len(split_audit["excluded_source_ids"]),
        "profiles": output, "worker_support": summary["worker_support"],
        "pretraining_contamination_status": "unknown"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prepared-root", type=Path, required=True)
    parser.add_argument("--repeat-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    evidence = audit(args.prepared_root, args.repeat_root)
    oracle.write_json(args.output, evidence)
    print("verified", len(evidence["profiles"]), "profiles and", evidence["reproducibility"]["files"], "repeated files; no model execution")


if __name__ == "__main__":
    main()
