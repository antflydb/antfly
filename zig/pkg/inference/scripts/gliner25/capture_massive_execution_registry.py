#!/usr/bin/env python3
"""Freeze the already audited MASSIVE transport identities; no model or download."""
from __future__ import annotations

import argparse
from pathlib import Path

import evaluate
import evaluation_contract as contract
import oracle
import prepare_massive11 as massive

HERE = Path(__file__).resolve().parent
FROZEN = ("evaluate.py", "benchmark_cpu.py", "check_bundles.py", "evaluation_contract.py",
          "oracle.py", "prepare_crossner_ai.py")


def zig_pins(registry: dict, registry_sha256: str) -> str:
    """The worker embeds only blinded admission pins, never gold annotations."""
    lines = [f'const massive_registry_sha256 = "{registry_sha256}";',
             f'const massive_adapter_sha256 = "{registry["adapter_sha256"]}";',
             f'const massive_harness_sha256 = "{registry["frozen_helpers"]["evaluation_contract.py"]}";',
             'const massive_model_pins = [_]struct { name: []const u8, files_sha256: []const u8 }{']
    for variant, model in registry["models"].items():
        digest = contract.digest(contract.encoded(model["source_files"]))
        lines.append(f'    .{{ .name = "{variant}", .files_sha256 = "{digest}" }},')
    lines.extend(['};', 'const massive_profile_pins = [_]MassiveProfilePin{'])
    for row in registry["profiles"]:
        profile = row["profile"]
        lines.extend(['    .{', f'        .name = "{profile["id"]}",', f'        .locale = "{profile["locale"]}",',
                      f'        .splitter = .{profile["word_splitter"]},'])
        for field, key in (("lock", "lock_sha256"), ("prepared", "prepared_sha256"), ("requests", "requests_sha256"),
                           ("schema", "schema_sha256"), ("transport", "transport_sha256")):
            lines.append(f'        .{field} = "{row[key]}",')
        lines.append('        .shards = .{')
        for shard in row["shards"]:
            lines.append(f'            .{{ .shard = "{shard["shard_sha256"]}", .ids = "{shard["request_ids_sha256"]}", .requests = "{shard["request_sha256s_sha256"]}" }},')
        lines.extend(['        },', '    },'])
    lines.append('};')
    return "\n".join(lines) + "\n"


def capture(root: Path) -> dict:
    evidence = oracle.read_json(HERE / "massive11_preparation.json")
    contract.checked(oracle.sha256_file(root / "preparation.json") == evidence["preparation_sha256"],
                     "preparation differs from the audited repeat")
    manifest = oracle.read_json(massive.MANIFEST)
    profiles = massive.profile_definitions(manifest)
    rows = []
    for profile, approved in zip(profiles, evidence["profiles"], strict=True):
        directory = root / profile["id"]
        contract.checked(approved["profile"] == profile, "profile inventory differs")
        pins = {}
        for name, key in (("lock.json", "lock_sha256"), ("prepared/prepared.json", "prepared_sha256"),
                          ("prepared/requests.jsonl", "requests_sha256"), ("prepared/gold.jsonl", "gold_sha256"),
                          ("transport.json", "transport_sha256")):
            path = directory / name
            contract.checked(oracle.sha256_file(path) == approved[key], "audited profile bytes differ")
            pins[key] = approved[key]
        schema, metrics = massive.schema_and_metrics(profile, manifest)
        requests = list(contract.rows(directory / "prepared/requests.jsonl"))
        transport = oracle.read_json(directory / "transport.json")
        shards = []
        for shard in transport["shards"]:
            path = contract.pinned_path(directory, shard["file"])
            selected = requests[shard["start"]:shard["end"]]
            contract.checked(oracle.read_json(path)["requests"] == selected, "shard differs from ordered requests")
            shards.append({key: shard[key] for key in ("index", "start", "end")} | {
                "shard_sha256": shard["file"]["sha256"],
                "request_ids_sha256": contract.digest(contract.encoded([row["request_id"] for row in selected])),
                "request_sha256s_sha256": contract.digest(contract.encoded([row["request_sha256"] for row in selected]))})
        rows.append({"profile": profile, **pins, "schema_sha256": contract.digest(contract.encoded(schema)),
                     "metrics_sha256": contract.digest(contract.encoded(metrics)),
                     "request_ids_sha256": transport["request_ids_sha256"],
                     "request_sha256s_sha256": transport["request_sha256s_sha256"], "shards": shards})
    return {"scope": "gliner25_massive11_execution_registry/v1", "qualification": False,
            "records": 2974, "source_commit": oracle.UPSTREAM_COMMIT,
            "frozen_helpers": {name: oracle.sha256_file(HERE / name) for name in FROZEN},
            "preparation_receipt_sha256": oracle.sha256_file(HERE / "massive11_preparation.json"),
            "corpus_manifest_sha256": oracle.sha256_file(massive.MANIFEST),
            "adapter_sha256": oracle.sha256_file(Path(massive.__file__)),
            "models": {variant: evaluate.source_identity(variant) for variant in ("small", "base", "multi")},
            "profiles": rows}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prepared-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    oracle.write_json(args.output, capture(args.prepared_root))


if __name__ == "__main__":
    main()
