#!/usr/bin/env python3
"""One explicitly requested published Metal pause probe; no implicit retry."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import signal
import time

import cpu_reference as reference
import supervision

ROOT = Path(__file__).resolve().parent
MIB = 1024**2
PREPARATION_SHA256 = "b75921b2a0e88de90fdf0ed58a9ec4207202f028d0d45efe128b8f8df90db03c"
RSS_LIMIT = 3 * 1024**3
require = reference.require


def preparation():
    raw = reference.read(ROOT / "preparation.json", 64 * 1024)
    require(hashlib.sha256(raw).hexdigest() == PREPARATION_SHA256, "preparation changed")
    value = reference.loads(raw)
    require(value["format"] == "antfly.gliner25-published-classifier-metal-pause-preparation/v1", "wrong preparation")
    require(value["model_execution"] is False and value["qualification"] is False, "preparation is not execution evidence")
    require(reference.digest(ROOT / "train.jsonl", MIB) == value["data"], "authored rows changed")
    for name, pin in {**value["configs"], **value["reference_files"]}.items():
        require(reference.digest(ROOT / name, MIB) == pin, f"prepared bytes changed: {name}")
    return value


def strict_nonnegative(value, label):
    require(type(value) is int and value >= 0, f"invalid nonnegative integer: {label}")
    return value


def controller_fingerprint(mode, manifest_bytes):
    # Only the explicitly resident optimizer domain differs from the frozen
    # native helper. Keep production numeric lexemes and ordered slot shapes.
    manifest = json.loads(manifest_bytes, object_pairs_hook=reference.unique,
                          parse_float=reference.JsonFloatToken,
                          parse_constant=lambda value: (_ for _ in ()).throw(ValueError(value)))
    run = manifest["config"]["run"]
    require(manifest["backend"] == "metal" and manifest["config"]["execution"] == "resident_metal", "not resident Metal")
    require(run["mode"] == mode and run["scheduler"] == "constant" and run["warmup_steps"] == 0 and run["epochs"] == 1 and run["batch_size"] == 1 and run["accumulation"] == 2 and run["max_optimizer_steps"] is None, "unsupported pause recipe")
    optimizer = {"beta1": run["beta1"], "beta2": run["beta2"], "eps": run["epsilon"], "weight_decay": run["weight_decay"]}
    schedule = {"warmup_constant": {"initial_lr": run["task_lr"], "warmup_steps": 0, "total_steps": 3}}
    settings = {"groups": [{"optimizer": optimizer, "schedule": schedule}], "grad_accum_steps": 2, "max_grad_norm": run["max_grad_norm"], "partial_window": "actual_microbatches"}
    hasher = hashlib.sha256(b"antfly.seeded-gradient-trainer.v1\x00resident_f32_optimizer_v1\x00" + reference.byte_array(manifest["run_fingerprint"]) + reference.compact_original_numbers(settings).encode())
    for name, shape in reference.slot_shapes(mode).items():
        hasher.update(reference.compact_original_numbers({"name": name, "dimensions": shape, "group": 0}).encode())
    return list(hasher.digest())


def validate_resource_report(report, config):
    memory = config["memory"]
    require(strict_nonnegative(report["host_peak_bytes"], "host peak") <= memory["host_bytes"], "host cap exceeded")
    # The backend allocator owns host metadata; the separate resident bound
    # includes frozen weights, persistent device state and the complete step.
    require(strict_nonnegative(report["backend_peak_bytes"], "metadata peak") <= memory["backend_metadata_bytes"], "metadata cap exceeded")
    resident = strict_nonnegative(report["resident_device_upper_bound_bytes"], "resident upper bound")
    require(0 < resident <= memory["backend_bytes"], "missing/exceeded resident admission")
    strict_nonnegative(report["resident_gradient_control_bytes"], "gradient control bytes")
    transfers = report["transfers"]
    require(type(transfers) is dict and set(transfers) == {"bytes", "upload_calls", "readback_calls"}, "missing bounded transfer diagnostic")
    strict_nonnegative(transfers["upload_calls"], "upload calls")
    strict_nonnegative(transfers["readback_calls"], "readback calls")
    counts = transfers["bytes"]
    require(type(counts) is dict and set(counts) == {"upload_bytes", "proposal_logits", "proposal_features", "loss_logits", "finite_control"}, "unknown transfer categories")
    for name, value in counts.items():
        strict_nonnegative(value, "transfer " + name)
    require(counts["upload_bytes"] <= 256 * MIB and sum(value for name, value in counts.items() if name != "upload_bytes") <= 128 * MIB, "default training transfer cap exceeded")


def validate(mode, prep):
    folder = ROOT / "executions" / f"{mode}-paused"
    process = reference.load(folder / "process.json")
    require(process["returncode"] == 0 and process["failure"] is None, "process failed")
    require(process["cleanup"]["complete"] is True and process["cleanup"]["direct_child_reaped"] is True and process["cleanup"]["known_children_gone"] is True, "owned process cleanup incomplete")
    require(process["psutil_version"] == "7.1.3" and process["max_child_tree_rss_bytes"] == RSS_LIMIT, "wrong RSS profile")
    require(0 < strict_nonnegative(process["peak_child_tree_rss_bytes"], "tree RSS") <= RSS_LIMIT, "RSS cap exceeded")
    require(any(p["relation"] == "observed_descendant" for p in process["tracked_processes"]), "CLI worker not observed")
    for field, path in (("driver", Path(__file__)), ("supervision", Path(supervision.__file__)), ("reference", Path(reference.__file__))):
        require(process[field] == reference.digest(path, MIB), f"checker helper changed: {field}")
    config_path = ROOT / f"{mode}-paused.json"
    config = reference.load(config_path, 64 * 1024)
    require(process["config"] == reference.digest(config_path, 64 * 1024), "consumed configuration changed")
    require(process["binary"] == {key: prep["binary"][key] for key in ("size_bytes", "sha256")}, "wrong executable")
    output = Path(config["output_dir"])
    result = reference.load(output / "result.json", 64 * 1024)
    manifest_bytes = reference.read(output / "run.json", MIB)
    manifest = reference.loads(manifest_bytes)
    reference.config_subset(config, manifest["config"])
    require(manifest["source"] == prep["source"] and manifest["backend"] == "metal" and manifest["math_policy"] == "strict_f32_activations_v1", "source/backend/math policy changed")
    require(reference.executable_snapshot_digest(manifest["observed_executable"]["digest"]) == process["binary"], "consumed executable differs")
    require(reference.byte_array(manifest["train_sha256"]).hex() == prep["data"]["sha256"], "consumed data differs")
    require(manifest["evaluation_performed"] is False and manifest["calibration_sha256"] is None and manifest["test_sha256"] is None, "unexpected evaluation")
    require(manifest["initial_identity"] == {"optimizer_step": 0, "microbatch_step": 0} and manifest["restore_receipt"] is None, "unexpected restored state")
    require(result["status"] == "paused" and result["identity"] == {"optimizer_step": 0, "microbatch_step": 1} and type(result["accumulated_microbatches"]) is int and result["accumulated_microbatches"] == 1, "wrong paused identity")
    require(result["run_fingerprint"] == manifest["run_fingerprint"], "run identity changed")
    require(result["portable_model"] is None and not (output / "model").exists(), "paused run published a model")
    reports = [reference.loads(line) for line in reference.read(output / "progress.jsonl", MIB).splitlines()]
    reference.validate_reports(reports, "paused")
    for report in reports:
        validate_resource_report(report, config)
    events = [reference.loads(line) for line in reference.read(folder / "stdout.jsonl", 4 * MIB).splitlines()]
    require(events == [{"event": "step", "report": reports[0]}, {"event": "result", "result": result, "output_dir": str(output)}], "stdout/durable output differs")
    checkpoint = reference.validate_checkpoint(output / "latest.safetensors", mode, True, controller_fingerprint(mode, manifest_bytes), result["state_sha256"])
    reference.verify_source(prep)
    files = {name: reference.digest(output / name, 4 * MIB) for name in ("result.json", "run.json", "progress.jsonl", "latest.safetensors")}
    files.update({"process.json": reference.digest(folder / "process.json"), "stdout.jsonl": reference.digest(folder / "stdout.jsonl"), "start.json": reference.digest(folder / "start.json")})
    return {"format": "antfly.gliner25-published-classifier-metal-pause-validation/v1", "mode": mode, "status": "pass", "phase": "paused", "binary": process["binary"], "source": prep["source"], "data": prep["data"], "preparation_sha256": PREPARATION_SHA256, "result": result, "slots": checkpoint, "files": files, "resources": {"host_peak_bytes": reports[0]["host_peak_bytes"], "backend_metadata_peak_bytes": reports[0]["backend_peak_bytes"], "resident_device_upper_bound_bytes": reports[0]["resident_device_upper_bound_bytes"], "sampled_child_tree_rss_bytes": process["peak_child_tree_rss_bytes"], "transfers": reports[0]["transfers"], "resident_gradient_control_bytes": reports[0]["resident_gradient_control_bytes"]}, "checker": reference.digest(Path(__file__)), "claim": prep["scope"], "inactive_batch_executed": False, "optimizer_update_executed": False, "published_source_numerical_parity": False}


def run(mode, prep):
    binary = Path(prep["binary"]["path"])
    observed = reference.digest(binary)
    require(observed == {key: prep["binary"][key] for key in ("size_bytes", "sha256")}, "binary pin mismatch")
    reference.verify_source(prep)
    config_path = ROOT / f"{mode}-paused.json"
    config = reference.load(config_path, 64 * 1024)
    require(not Path(config["output_dir"]).exists(), "output exists; no retry/overwrite")
    folder = ROOT / "executions" / f"{mode}-paused"
    folder.parent.mkdir(exist_ok=True)
    folder.mkdir(mode=0o700)
    command = [str(binary), "finetune", "train", "gliner25", str(config_path), "--shutdown-grace-seconds", "30", "--stop-after-microbatches", "1"]
    receipt = {"format": "antfly.gliner25-published-classifier-metal-pause-process/v1", "command": command, "binary": observed, "config": reference.digest(config_path, 64 * 1024), "preparation_sha256": PREPARATION_SHA256, "driver": reference.digest(Path(__file__)), "supervision": reference.digest(Path(supervision.__file__)), "reference": reference.digest(Path(reference.__file__)), "outer_timeout_seconds": config["timeout_seconds"] + 90, "max_child_tree_rss_bytes": RSS_LIMIT, "max_stdout_bytes": 4 * MIB, "max_stderr_bytes": 4 * MIB, "failure": None, "returncode": None}
    reference.write_new(folder / "start.json", receipt)
    started = time.monotonic()
    try:
        receipt.update(supervision.run(command, folder / "stdout.jsonl", folder / "stderr.log", timeout_seconds=receipt["outer_timeout_seconds"], rss_limit_bytes=RSS_LIMIT))
        require(receipt["failure"] is None and receipt["returncode"] == 0 and receipt["cleanup"]["complete"], receipt["failure"] or "process/cleanup failure")
        require(reference.digest(binary) == observed, "executable changed during invocation")
        require(reference.digest(config_path, 64 * 1024) == receipt["config"], "config changed during invocation")
        preparation()
        for field, path in (("driver", Path(__file__)), ("supervision", Path(supervision.__file__)), ("reference", Path(reference.__file__))):
            require(reference.digest(path, MIB) == receipt[field], f"helper changed during invocation: {field}")
        reference.verify_source(prep)
    except BaseException as error:
        if receipt["failure"] is None:
            receipt["failure"] = f"{type(error).__name__}: {error}"
        raise
    finally:
        receipt["elapsed_seconds"] = time.monotonic() - started
        reference.write_new(folder / "process.json", receipt)
    checked = validate(mode, prep)
    reference.write_new(folder / "validation.json", checked)
    print(json.dumps(checked, allow_nan=False))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("inspect", "run", "validate"))
    parser.add_argument("--mode", choices=("lora", "dora"))
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    def terminate(_signal, _frame):
        raise InterruptedError("driver termination requested")
    signal.signal(signal.SIGTERM, terminate)
    prep = preparation()
    if args.command == "inspect":
        require(args.mode is None and args.output is None, "inspect takes no execution options")
        print(json.dumps({"preparation_sha256": PREPARATION_SHA256, "model_execution": False, "resource_analysis": prep["resource_analysis"], "outer_supervision": prep["outer_supervision"]}, indent=2))
    else:
        require(args.mode is not None, "mode is required")
        if args.command == "run":
            require(args.output is None, "run output is fixed; no overwrite")
            run(args.mode, prep)
        else:
            require(args.output is not None and not args.output.exists(), "offline validation requires a new output path")
            receipt = validate(args.mode, prep)
            reference.write_new(args.output, receipt)
            print(json.dumps(receipt, allow_nan=False))


if __name__ == "__main__":
    main()
