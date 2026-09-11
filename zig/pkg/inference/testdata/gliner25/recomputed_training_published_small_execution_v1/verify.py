#!/usr/bin/env python3
"""Offline bounded receipt/byte-integrity audit. Does not load a model or run training."""
from pathlib import Path
import collections
import gzip
import hashlib
import io
import json
import os
import stat

ROOT = Path(__file__).resolve().parent
CAP = 2 * 1024 * 1024


def require(condition, context):
    if not condition:
        raise ValueError(context)


def read_regular(path, maximum=CAP):
    fd = os.open(path, os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW)
    try:
        st = os.fstat(fd)
        require(stat.S_ISREG(st.st_mode) and st.st_size <= maximum, str(path))
        with os.fdopen(fd, "rb", closefd=False) as stream:
            data = stream.read(maximum + 1)
        require(len(data) == st.st_size, f"changed/truncated: {path}")
        return data
    finally:
        os.close(fd)


def identity(data):
    return {"size_bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()}


def main():
    manifest = json.loads(read_regular(ROOT / "manifest.json"))
    require(manifest["format"] == "antfly.gliner25-recomputed-published-small-execution/v1", "format")
    decoded = {}
    stored_paths = set()
    for entry in manifest["files"]:
        path = Path(entry["path"])
        require(not path.is_absolute() and all(p not in {"..", "."} for p in path.parts), "path")
        require(entry["path"] not in stored_paths, "duplicate stored path")
        stored_paths.add(entry["path"])
        payload = read_regular(ROOT / path)
        require(identity(payload) == entry["stored"], f"stored bytes: {path}")
        length = entry["decoded"]["size_bytes"]
        require(type(length) is int and 0 <= length <= CAP, "decoded cap")
        if entry["encoding"] == "gzip":
            with gzip.GzipFile(fileobj=io.BytesIO(payload)) as stream:
                data = stream.read(length + 1)
        else:
            require(entry["encoding"] == "identity", "encoding")
            data = payload
        require(identity(data) == entry["decoded"], f"decoded bytes: {path}")
        require(entry["logical_path"] not in decoded, "duplicate logical path")
        decoded[entry["logical_path"]] = data
    observed = {str(p.relative_to(ROOT)) for p in ROOT.rglob("*") if p.is_file()}
    require(observed == stored_paths | {"manifest.json"}, "missing/extra ledger files")
    def obj(path):
        return json.loads(decoded[path])
    def pin(path):
        return identity(decoded[path])
    exact_names = ["result.json", "latest.safetensors", "model/adapter_config.json", "model/adapter_model.safetensors", "model/antfly_gliner25_adapter.json", "model/antfly_gliner25_training.json"]
    old_process = obj("raw/native_v1/executions/lora-resumed/process.json")
    require(old_process["returncode"] == 1 and old_process["cleanup"]["complete"], "v1 failure/cleanup")
    require(b"TrainingOptimizerLimitExceeded" in decoded["raw/native_v1/executions/lora-resumed/stderr.log"], "v1 declared denial")
    require(obj("raw/native_v1/executions/lora-paused/validation.json")["status"] == "pass", "v1 pause")
    for version in [1, 2]:
        prefix = f"raw/native_v{version}"
        build = f"build/native_v{version}"
        binding = obj(prefix + "/executable.json")
        for key, name in [("build_receipt", "build-receipt.json"), ("source_inventory", "recorded-build-source-inventory.json"), ("source_archive_receipt", "recorded-build-source-archive.json")]:
            require(pin(build + "/" + name) == {k: binding[key][k] for k in ["size_bytes", "sha256"]}, key)
        inventory = obj(build + "/recorded-build-source-inventory.json")
        controller = f"source_snapshots/native_v{version}/seeded_gradient_trainer.zig"
        require(pin(controller) == inventory["zig/pkg/inference/src/finetune/seeded_gradient_trainer.zig"], "exact frozen controller")
        require(manifest["builds"][str(version)]["controller"] == pin(controller), "controller summary")
        require(pin("helpers/supervision.py") == {k: binding["supervisor"][k] for k in ["size_bytes", "sha256"]}, "supervisor")
        for name in ["checker.py", "run_phase.py"]:
            require(pin(prefix + "/" + name) == binding["helpers"][name], "bound helper")
    inventory = obj("build/native_v2/recorded-build-source-inventory.json")
    plan = obj("raw/native_v2/resource_plan.json")
    for reference in plan["code_formula_pins"].values():
        require(inventory[reference["path"]] == {k: reference[k] for k in ["size_bytes", "sha256"]}, "frozen code formula")
    for mode, slots in [("lora", 262), ("dora", 393)]:
        final_path = f"raw/native_v2/{mode}-validation.json"
        final = obj(final_path)
        require(final["status"] == "pass" and final["microbatches"] == 5 and final["updates"] == 3, "final counters")
        require(final["flush_after"] == [2, 4, 5] and final["zero_loss_fallback"] == [False] * 5, "encoder remains active")
        require(final["quality_or_published_source_numerical_parity"] is False, "scope")
        require(manifest["modes"][mode]["final_validation"] == pin(final_path), "final pin")
        for phase in ["paused", "resumed", "uninterrupted"]:
            prefix = f"raw/native_v2/executions/{mode}-{phase}"
            validation = obj(prefix + "/validation.json")
            process = obj(prefix + "/process.json")
            require(final["phases"][phase] == validation, "embedded phase")
            require(validation["process_receipt"] == pin(prefix + "/process.json"), "process pin")
            require(process["returncode"] == 0 and process["failure"] is None and process["cleanup"]["complete"], "process success")
            require(process["cleanup"]["direct_child_reaped"] and process["cleanup"]["known_children_gone"], "owned cleanup")
            require(not process["cleanup"]["errors"] and not process["cleanup"]["survivors"] and not process["inspection_errors"], "clean process proof")
            require(process["max_child_tree_rss_bytes"] == 4 * 1024**3 and process["peak_child_tree_rss_bytes"] <= process["max_child_tree_rss_bytes"], "RSS guard")
            require(process["timeout_seconds"] == 1890 and process["elapsed_seconds"] < 1890, "deadline")
            require(not decoded[prefix + "/stderr.log"], "successful stderr")
            checkpoint = validation["checkpoint"]
            require(len(checkpoint["slots"]) == slots and len({s["name"] for s in checkpoint["slots"]}) == slots, "exact unique slot count")
            step_counts = dict(collections.Counter(s["adam_step"] for s in checkpoint["slots"]))
            expected = {0: slots} if phase == "paused" else ({0: 28, 2: 4, 3: 230} if mode == "lora" else {0: 42, 2: 6, 3: 345})
            require(step_counts == expected, "per-slot steps")
            present = sum(s["present"] for s in checkpoint["slots"])
            expected_present = (234 if mode == "lora" else 351) if phase == "paused" else 0
            require(present == expected_present, "presence")
            summary = manifest["modes"][mode]["phases"][phase]
            require(summary["resources"] == validation["resources"] and summary["validation"] == pin(prefix + "/validation.json"), "resource/pin summary")
        require(final["phases"]["resumed"]["checkpoint"] == final["phases"]["uninterrupted"]["checkpoint"], "exact state reports")
        for name in exact_names:
            expected = final["artifacts"][name]
            for phase in ["resumed", "uninterrupted"]:
                require(final["phases"][phase]["files"][name] == expected, "exact final file pair")
            require(manifest["modes"][mode]["exact_resumed_uninterrupted_artifacts"][name] == expected, "artifact summary")
        # Configs preserve every owner cap between failed v1 and successful v2.
        for phase in ["paused", "uninterrupted"]:
            old = obj(f"raw/native_v1/{mode}-{phase}.json")
            new = obj(f"raw/native_v2/{mode}-{phase}.json")
            for key in ["memory", "source_limits", "dataset_limits", "training_limits", "run", "peft", "expected_source", "train_file"]:
                require(old[key] == new[key], "unchanged cap or semantics")
            require(new["memory"]["optimizer_transaction_bytes"] == 33554432, "32 MiB transaction")
    require(pin("inputs/train.jsonl") == manifest["dataset"], "dataset")
    print(json.dumps({"status": "pass", "archived_files": len(stored_paths), "decoded_bytes": sum(len(x) for x in decoded.values()), "native_v2_phases": 6, "resume_modes": 2, "v1_declared_denial_preserved": True, "model_execution": False}, sort_keys=True))


if __name__ == "__main__":
    main()
