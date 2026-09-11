#!/usr/bin/env python3
"""Bounded one-invocation driver and offline published inactive-job validation.

No numerical packages, source patches, model inference, or hidden retries.
The `run` command is explicit; `inspect`, `validate`, `validate-phase`, and tests launch no model.
"""
from __future__ import annotations

import argparse
import copy
import hashlib
import json
import math
import os
from pathlib import Path
import selectors
import signal
import stat
import struct
import subprocess
import time
import supervision

ROOT = Path(__file__).resolve().parent
MIB = 1024 * 1024
PREPARATION_SHA256 = 'e49ca907daf093303dd3919b56ab61f36983eb9d9174e3d3ccd7993ac21e83b9'
PHASES = ("uninterrupted", "paused", "resumed")
SEMANTIC_REPORT = ("epoch", "batch", "examples", "terms", "coverage", "optimizer", "decision_fingerprint", "zero_loss_fallback")
TERM_NAMES = {"start", "end", "pair", "inside", "soft_iou", "rerank_listwise", "proposal", "consistency", "abstention", "count", "classification", "record_object", "record_field", "relation", "total"}
MODEL_FILES = {"adapter_config.json", "adapter_model.safetensors", "antfly_gliner25_adapter.json", "antfly_gliner25_training.json"}
# Exact Zig Job.Config leaf types. Only these fields undergo the same IEEE
# binary32 conversion as the production parser; observed JSON stays unchanged.
# run.warmup_ratio is f64 and deliberately excluded from binary32 conversion.
F32_CONFIG_FIELDS = frozenset(
    {"config.run." + name for name in ("encoder_lr", "task_lr", "weight_decay", "beta1", "beta2", "epsilon", "max_grad_norm", "num_cycles")}
    | {"config.peft.alpha", "config.peft.dropout"}
    | {"config.weights." + name for name in ("start", "end", "pair", "inside")}
    | {"config.gold_start", "config.gold_end", "config.gold_hold_fraction"}
)
F64_CONFIG_FIELDS = frozenset({"config.run.warmup_ratio"})
CONFIG_COMPARISON_POLICY = "zig_job_declared_f32_roundtrip_exact_v1"


def require(ok, message):
    if not ok:
        raise ValueError(message)


def regular(path):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC)
    try:
        info = os.fstat(fd)
        require(stat.S_ISREG(info.st_mode), f"not a regular file: {path}")
    except BaseException:
        os.close(fd)
        raise
    return fd, info


def read(path, maximum=4 * MIB):
    fd, before = regular(path)
    with os.fdopen(fd, "rb") as stream:
        require(0 < before.st_size <= maximum, f"file size outside bound: {path}")
        value = stream.read(maximum + 1)
        after = os.fstat(stream.fileno())
    require(len(value) == before.st_size and (before.st_size, before.st_mtime_ns, before.st_ctime_ns) == (after.st_size, after.st_mtime_ns, after.st_ctime_ns), f"file changed: {path}")
    return value


def digest(path, maximum=1024 * MIB):
    fd, before = regular(path)
    hasher = hashlib.sha256()
    with os.fdopen(fd, "rb") as stream:
        require(0 < before.st_size <= maximum, f"digest size outside bound: {path}")
        size = 0
        while chunk := stream.read(MIB):
            size += len(chunk)
            require(size <= maximum, f"file grew: {path}")
            hasher.update(chunk)
        after = os.fstat(stream.fileno())
    require(size == before.st_size and (before.st_size, before.st_mtime_ns, before.st_ctime_ns) == (after.st_size, after.st_mtime_ns, after.st_ctime_ns), f"file changed: {path}")
    return {"size_bytes": size, "sha256": hasher.hexdigest()}


def unique(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, f"duplicate JSON key: {key}")
        result[key] = value
    return result


def loads(raw):
    return json.loads(raw, object_pairs_hook=unique, parse_constant=lambda value: (_ for _ in ()).throw(ValueError(f"nonfinite JSON: {value}")))


def load(path, maximum=4 * MIB):
    return loads(read(path, maximum))


def write_new(path, value):
    raw = (json.dumps(value, indent=2, allow_nan=False) + "\n").encode()
    with path.open("xb") as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())


def finite(value):
    require(type(value) in (int, float) and math.isfinite(value), "expected finite JSON number")
    return value


def byte_array(value):
    require(type(value) is list and len(value) == 32 and all(type(x) is int and 0 <= x <= 255 for x in value), "expected 32-byte digest array")
    return bytes(value)


def executable_snapshot_digest(value):
    # runtime.file_snapshot.Digest uses raw [32]u8, whereas our streamed file
    # digest uses lower-hex. These two exact representations carry equal bytes.
    require(type(value) is dict and set(value) == {"size_bytes", "sha256"}, "invalid executable snapshot digest")
    require(type(value["size_bytes"]) is int and 0 < value["size_bytes"] <= 1024 * MIB, "invalid executable snapshot size")
    return {"size_bytes": value["size_bytes"], "sha256": byte_array(value["sha256"]).hex()}


def preparation():
    raw = read(ROOT / "preparation.json", 64 * 1024)
    require(hashlib.sha256(raw).hexdigest() == PREPARATION_SHA256, "preparation bytes changed")
    value = loads(raw)
    require(value["format"] == "antfly.gliner25-published-inactive-classifier-preparation/v1" and value["model_execution"] is False, "wrong preparation contract")
    require(digest(ROOT / "train.jsonl", MIB) == {key: value["data"][key] for key in ("size_bytes", "sha256")}, "authored rows changed")
    for name, pin in value["configs"].items():
        require(digest(ROOT / name, 64 * 1024) == pin, f"prospective config changed: {name}")
    return value


def verify_source(prep):
    base = Path(prep["source_dir"])
    for name, pin in zip(prep["source_file_names"], [prep["source"]["weight"], *prep["source"]["sidecars"]]):
        require(digest(base / name, 512 * MIB) == pin, f"source identity mismatch: {name}")


def resolve_config(mode, phase, prep):
    if phase != "resumed":
        return ROOT / f"{mode}-{phase}.json"
    path = ROOT / f"{mode}-resumed.json"
    require(not path.exists(), "resolved resume config already exists; preserve earlier invocation")
    paused = validate_phase(mode, "paused", prep)
    value = load(ROOT / f"{mode}-resumed.template.json", 64 * 1024)
    require(value["expected_restore_state_sha256"] == "RESOLVE_FROM_VERIFIED_PAUSED_RESULT_BEFORE_EXECUTION", "wrong resume template")
    value["expected_restore_state_sha256"] = list(byte_array(paused["result"]["state_sha256"]))
    write_new(path, value)
    return path


def config_subset(expected, actual, prefix="config"):
    if prefix in F32_CONFIG_FIELDS:
        finite(expected)
        finite(actual)
        try:
            canonical = struct.unpack("<f", struct.pack("<f", expected))[0]
        except (OverflowError, struct.error) as error:
            raise ValueError(f"out-of-range f32: {prefix}") from error
        require(math.isfinite(canonical), f"out-of-range f32: {prefix}")
        # Do not round the observed value: a changed adjacent or sub-ULP JSON
        # value must not become equal merely by another binary32 conversion.
        require(actual == canonical and (canonical != 0 or math.copysign(1, actual) == math.copysign(1, canonical)), f"changed consumed {prefix}")
    elif prefix in F64_CONFIG_FIELDS:
        require(finite(expected) == finite(actual), f"changed consumed {prefix}")
    elif type(expected) is dict:
        require(type(actual) is dict, f"wrong object: {prefix}")
        for key, value in expected.items():
            require(key in actual, f"missing {prefix}.{key}")
            config_subset(value, actual[key], prefix + "." + key)
    elif type(expected) is list:
        require(type(actual) is list and len(actual) == len(expected), f"changed consumed {prefix}")
        for index, (value, observed) in enumerate(zip(expected, actual)):
            config_subset(value, observed, f"{prefix}[{index}]")
    else:
        require(type(expected) is type(actual) and expected == actual, f"changed consumed {prefix}")


def consumed_helpers(process):
    """Resolve historical helper bytes without replacing the process receipt."""
    result = {}
    for field, name in (("driver", "validate.py"), ("supervision", "supervision.py")):
        pin = process[field]
        path = ROOT / name
        if digest(path) != pin:
            path = ROOT / "helper-archive" / "v1" / name
        require(digest(path) == pin, f"historical {field} bytes unavailable or changed")
        result[field] = {"path": str(path), "digest": pin}
    return result


def slot_shapes(mode, saved=False):
    result = {}
    for module, inputs, outputs in (("classifier.0", 384, 768), ("classifier.3", 768, 1)):
        prefix = "base_model.model." + module
        middle = "" if saved else ".default"
        result[prefix + ".lora_A" + middle + ".weight"] = [2, inputs]
        result[prefix + ".lora_B" + middle + ".weight"] = [outputs, 2]
        if mode == "dora":
            result[prefix + ".lora_magnitude_vector" + ("" if saved else ".default.weight")] = [outputs]
    return result


def tensors(path):
    raw = read(path, 4 * MIB)
    require(len(raw) >= 8, "truncated SafeTensors length")
    length = struct.unpack_from("<Q", raw)[0]
    require(1 < length <= MIB and 8 + length <= len(raw), "invalid SafeTensors header length")
    header = loads(raw[8:8 + length])
    result = {}
    ranges = []
    for key, item in header.items():
        if key == "__metadata__":
            continue
        require(type(item) is dict and item["dtype"] == "F32", "non-F32 training tensor")
        shape = item["shape"]
        require(type(shape) is list and 0 < len(shape) <= 8 and all(type(x) is int and x > 0 for x in shape), "invalid training tensor shape")
        offsets = item["data_offsets"]
        require(type(offsets) is list and len(offsets) == 2 and all(type(x) is int for x in offsets), "invalid tensor offsets")
        start, end = offsets
        require(0 <= start < end <= len(raw) - 8 - length and end - start == math.prod(shape) * 4, "out-of-range tensor payload")
        payload = raw[8 + length + start:8 + length + end]
        values = [x[0] for x in struct.iter_unpack("<f", payload)]
        require(all(math.isfinite(x) for x in values), "nonfinite checkpoint tensor")
        result[key] = {"shape": shape, "values": values}
        ranges.append((start, end))
    position = 0
    for start, end in sorted(ranges):
        require(start == position, "overlapping or missing tensor payload")
        position = end
    require(position == len(raw) - 8 - length, "trailing tensor payload")
    return result


def encoded_counter(values):
    require(len(values) == 4 and all(x.is_integer() and 0 <= x <= 255 for x in values), "invalid byte-encoded counter")
    return int.from_bytes(bytes(int(x) for x in values), "little")


def encoded_global_counter(values):
    # RealAutodiffTrainer global counters use four 16-bit limbs; per-slot
    # adam_step_u32 uses four 8-bit limbs. Keep their distinct typed contracts.
    require(len(values) == 4 and all(x.is_integer() and 0 <= x <= 65535 for x in values), "invalid word-encoded global counter")
    return sum(int(value) << (16 * index) for index, value in enumerate(values))


class JsonFloatToken(str):
    """A production JSON numeric lexeme retained for byte-exact SHA input."""


def compact_original_numbers(value):
    if type(value) is JsonFloatToken:
        return str(value)
    if type(value) is dict:
        return "{" + ",".join(json.dumps(key) + ":" + compact_original_numbers(item) for key, item in value.items()) + "}"
    if type(value) is list:
        return "[" + ",".join(compact_original_numbers(item) for item in value) + "]"
    require(type(value) in (str, int, bool, type(None)), "unexpected controller fingerprint value")
    return json.dumps(value, separators=(",", ":"))


def checkpoint_contract_fingerprint(mode, manifest_bytes):
    # seeded_gradient_trainer.Trainer.fingerprint adds the optimizer contract
    # to NativeTrainer's outer run digest. Reuse the already verified original
    # Zig numeric tokens so Python exponent formatting cannot alter the hash.
    manifest = json.loads(manifest_bytes, object_pairs_hook=unique, parse_float=JsonFloatToken,
                          parse_constant=lambda value: (_ for _ in ()).throw(ValueError(f"nonfinite JSON: {value}")))
    run = manifest["config"]["run"]
    require(manifest["backend"] == "native" and run["mode"] == mode and run["scheduler"] == "constant" and run["warmup_steps"] == 0 and run["epochs"] == 1 and run["batch_size"] == 1 and run["accumulation"] == 2 and run["max_optimizer_steps"] is None, "unsupported qualification checkpoint contract")
    optimizer = {"beta1": run["beta1"], "beta2": run["beta2"], "eps": run["epsilon"], "weight_decay": run["weight_decay"]}
    schedule = {"warmup_constant": {"initial_lr": run["task_lr"], "warmup_steps": 0, "total_steps": 3}}
    settings = {"groups": [{"optimizer": optimizer, "schedule": schedule}], "grad_accum_steps": run["accumulation"], "max_grad_norm": run["max_grad_norm"], "partial_window": "actual_microbatches"}
    hasher = hashlib.sha256(b"antfly.seeded-gradient-trainer.v1" + byte_array(manifest["run_fingerprint"]) + compact_original_numbers(settings).encode())
    for name, shape in slot_shapes(mode).items():
        hasher.update(compact_original_numbers({"name": name, "dimensions": shape, "group": 0}).encode())
    return list(hasher.digest())


def checkpoint_state_fingerprint(data, mode, fingerprint):
    # Exact independently reconstructed Controller.stateFingerprint payload.
    # Logical dimensions come from the verified adapter contract; checkpoint
    # payloads themselves are flattened F32 tensors.
    hasher = hashlib.sha256(b"antfly.seeded-gradient-state.v1\0" + byte_array(fingerprint))
    def integer(value):
        hasher.update(struct.pack("<Q", value))
    counters = data["__trainer_counters"]["values"]
    for value in (encoded_global_counter(counters[4:]), encoded_global_counter(counters[:4]), int(data["__extension.seeded.counters"]["values"][1]), len(slot_shapes(mode))):
        integer(value)
    for index, (name, shape) in enumerate(slot_shapes(mode).items()):
        encoded = name.encode()
        integer(len(encoded))
        hasher.update(encoded)
        integer(len(shape))
        for dim in shape:
            integer(dim)
        step = encoded_counter(data["adam_step_u32::" + name]["values"])
        for value in (step, step, int(data["__extension.seeded.presence"]["values"][index])):
            integer(value)
        for key in ("weight::" + name, "adam_m::" + name, "adam_v::" + name, "__extension.seeded.gradient." + str(index)):
            values = data[key]["values"]
            integer(len(values))
            hasher.update(struct.pack("<" + "f" * len(values), *values))
    return list(hasher.digest())


def validate_checkpoint(path, mode, paused, fingerprint, state=None):
    data = tensors(path)
    shapes = slot_shapes(mode)
    counters = data["__trainer_counters"]["values"]
    require(data["__trainer_counters"]["shape"] == [8] and len(counters) == 8 and encoded_global_counter(counters[:4]) == (1 if paused else 5) and encoded_global_counter(counters[4:]) == (0 if paused else 3), "wrong durable global counters")
    require(data["__run_fingerprint"]["values"] == list(byte_array(fingerprint)), "checkpoint run fingerprint changed")
    require(data["__extension.seeded.counters"]["values"] == [1, int(paused), 2], "wrong accumulation divisor/count")
    presence = data["__extension.seeded.presence"]["values"]
    require(presence == [int(paused)] * len(shapes), "wrong durable gradient presence")
    actual_names = {key.removeprefix("weight::") for key in data if key.startswith("weight::")}
    require(actual_names == set(shapes), "checkpoint omitted or added adapter slots")
    for index, (name, shape) in enumerate(shapes.items()):
        size = math.prod(shape)
        for key in ("weight::" + name, "adam_m::" + name, "adam_v::" + name, "__extension.seeded.gradient." + str(index)):
            require(data[key]["shape"] == [size], f"wrong flattened slot shape: {key}")
        require(data["adam_step_u32::" + name]["shape"] == [4], "wrong exact Adam counter shape")
        require(encoded_counter(data["adam_step_u32::" + name]["values"]) == (0 if paused else 3), f"inactive slot did not participate in all three updates: {name}")
        require(data["adam_step::" + name]["shape"] == [1] and data["adam_step::" + name]["values"] == [0 if paused else 3], "compatibility Adam counter disagrees")
        require(all(x >= 0 for x in data["adam_v::" + name]["values"]), "negative second moment")
    gradients = [v for key, v in data.items() if key.startswith("__extension.seeded.gradient.")]
    require(len(gradients) == len(shapes), "wrong gradient accumulator inventory")
    if not paused:
        require(all(x == 0 for value in gradients for x in value["values"]), "final accumulation was not cleared")
    calculated_state = checkpoint_state_fingerprint(data, mode, fingerprint)
    if state is not None:
        require(byte_array(state) == bytes(calculated_state), "checkpoint owned state digest mismatch")
    return {"slots": len(shapes), "global_microbatch": 1 if paused else 5, "all_slot_updates": 0 if paused else 3, "controller_fingerprint": fingerprint, "state_sha256": calculated_state}


def validate_reports(reports, phase):
    expected_rows = [0] if phase == "paused" else list(range(1, 5)) if phase == "resumed" else list(range(5))
    require(len(reports) == len(expected_rows) + (phase != "paused"), "missing/extra progress rows")
    for report, index in zip(reports, expected_rows):
        for field in SEMANTIC_REPORT:
            require(field in report, f"missing report field: {field}")
        require(report["epoch"] == 0 and report["batch"] == index and report["examples"] == 1, "wrong row order/cardinality")
        fallback = index in (1, 3, 4)
        require(report["zero_loss_fallback"] is fallback, "wrong inactive source policy")
        terms = report["terms"]
        require(type(terms) is dict and set(terms) == TERM_NAMES and finite(terms["total"]) > 0, "raw frozen-task diagnostics missing")
        require(report["coverage"] == {"gold_mentions": 1, "proposed_gold_mentions": 1, "gold_relations": 0, "proposed_gold_relations": 0, "matched_records": 0}, "authored gold mention was dropped")
        for value in terms.values():
            finite(value)
        optimizer = report["optimizer"]
        require(optimizer["identity"] == {"optimizer_step": (index + 1) // 2, "microbatch_step": index + 1}, "wrong microbatch/global update count")
        require(optimizer["optimizer_stepped"] is (index in (1, 3)), "wrong accumulation flush position")
        require(optimizer["accumulated_microbatches"] == (index + 1) % 2, "inactive batch was not included in accumulation")
        finite(optimizer["grad_norm"])
        if fallback:
            require(optimizer["loss"] == 0 and terms["classification"] == 0, "inactive objective must be zero while raw terms remain")
        else:
            require(finite(optimizer["loss"]) == terms["total"] and finite(terms["classification"]) > 0, "active objective or labels missing")
        byte_array(report["decision_fingerprint"])
    if phase != "paused":
        final = reports[-1]
        require(final["examples"] == 0 and final["terms"] is None, "missing explicit partial epoch flush")
        require(final["optimizer"]["identity"] == {"optimizer_step": 3, "microbatch_step": 5}, "wrong final partial flush identity")
        require(final["optimizer"]["optimizer_stepped"] is True and final["optimizer"]["accumulated_microbatches"] == 0, "inactive partial flush omitted")
        require(final["optimizer"]["grad_norm"] == 0, "last wholly inactive window must have zero norm")


def validate_phase(mode, phase, prep):
    invocation = ROOT / "executions" / f"{mode}-{phase}"
    process = load(invocation / "process.json")
    consumed_helpers(process)
    require(process["returncode"] == 0 and process["failure"] is None, "invocation did not complete cleanly")
    require(process["cleanup"]["complete"] is True and process["cleanup"]["direct_child_reaped"] is True and process["cleanup"]["known_children_gone"] is True, "owned process cleanup incomplete")
    require(process["psutil_version"] == "7.1.3" and process["max_child_tree_rss_bytes"] == 2 * 1024**3 and 0 < process["peak_child_tree_rss_bytes"] <= process["max_child_tree_rss_bytes"], "missing or exceeded RSS guard")
    require(any(entry["relation"] == "observed_descendant" for entry in process["tracked_processes"]), "public CLI worker was not observed")
    require(process["binary"]["sha256"] not in prep["rejected_pre_correction_binary_sha256"], "pre-correction executable")
    path = ROOT / f"{mode}-{phase}.json"
    config = load(path, 64 * 1024)
    require(digest(path, 64 * 1024) == process["config"], "consumed configuration was substituted")
    output = Path(config["output_dir"])
    result = load(output / "result.json", 64 * 1024)
    manifest_bytes = read(output / "run.json", MIB)
    manifest = loads(manifest_bytes)
    config_subset(config, manifest["config"])
    require(manifest["source"] == prep["source"] and manifest["backend"] == "native" and manifest["math_policy"] == "strict_f32_activations_v1", "wrong source/backend/math policy")
    require(executable_snapshot_digest(manifest["observed_executable"]["digest"]) == process["binary"], "runtime executable digest mismatch")
    require(byte_array(manifest["train_sha256"]).hex() == prep["data"]["sha256"], "wrong consumed dataset")
    require(manifest["evaluation_performed"] is False and manifest["calibration_sha256"] is None and manifest["test_sha256"] is None, "unexpected evaluation or dataset")
    expected_identity = {"optimizer_step": 0, "microbatch_step": 1} if phase == "paused" else {"optimizer_step": 3, "microbatch_step": 5}
    require(result["status"] == ("paused" if phase == "paused" else "complete") and result["identity"] == expected_identity, "wrong terminal state")
    require(result["accumulated_microbatches"] == int(phase == "paused"), "wrong terminal accumulator count")
    require(result["run_fingerprint"] == manifest["run_fingerprint"], "run identity drift")
    byte_array(result["state_sha256"])
    if phase == "resumed":
        paused = load(ROOT / f"{mode}-paused" / "result.json", 64 * 1024)
        require(config["expected_restore_state_sha256"] == paused["state_sha256"], "resume lacks exact paused state pin")
        require(manifest["initial_identity"] == {"optimizer_step": 0, "microbatch_step": 1} and manifest["restore_receipt"] is not None, "fresh owner did not restore unfinished first window")
    else:
        require(manifest["initial_identity"] == {"optimizer_step": 0, "microbatch_step": 0} and manifest["restore_receipt"] is None, "unexpected initial optimizer state")
    reports = [loads(line) for line in read(output / "progress.jsonl", MIB).splitlines()]
    validate_reports(reports, phase)
    events = [loads(line) for line in read(invocation / "stdout.jsonl", 4 * MIB).splitlines()]
    require(len(events) == len(reports) + 1, "redirected output lost or added events")
    require(all(event == {"event": "step", "report": report} for event, report in zip(events, reports)), "stdout/durable progress differs")
    require(events[-1] == {"event": "result", "result": result, "output_dir": str(output)}, "wrong final stdout event")
    for report in reports:
        require(0 <= report["host_peak_bytes"] <= config["memory"]["host_bytes"] and 0 <= report["backend_peak_bytes"] <= config["memory"]["backend_bytes"], "owner peak exceeds declared limit")
        require(report["resident_device_upper_bound_bytes"] == 0, "unexpected resident backend")
    slots = validate_checkpoint(output / "latest.safetensors", mode, phase == "paused", checkpoint_contract_fingerprint(mode, manifest_bytes), result["state_sha256"])
    if phase == "paused":
        require(result["portable_model"] is None and not (output / "model").exists(), "paused run published final model")
    else:
        require({p.name for p in (output / "model").iterdir()} == MODEL_FILES, "unexpected exported files")
        portable = result["portable_model"]
        require(type(portable) is dict and portable["mode"] == mode, "missing portable export receipt")
        require(portable["weights"] == digest(output / "model" / "adapter_model.safetensors", 4 * MIB), "exported weight receipt mismatch")
        require(portable["provenance"] == digest(output / "model" / "antfly_gliner25_training.json", 4 * MIB), "exported provenance receipt mismatch")
        require(portable["output_bytes"] == sum((output / "model" / name).stat().st_size for name in MODEL_FILES) <= config["export_limits"]["max_output_bytes"], "export output size mismatch")
        require(0 <= portable["peak_scratch_bytes"] <= config["export_limits"]["max_scratch_bytes"], "export scratch exceeded declaration")
        adapter = tensors(output / "model" / "adapter_model.safetensors")
        require({key: value["shape"] for key, value in adapter.items()} == slot_shapes(mode, saved=True), "wrong portable classifier adapter inventory")
        exported = load(output / "model" / "adapter_config.json", 64 * 1024)
        require(exported["target_modules"] == ["classifier.0", "classifier.3"] and exported["r"] == 2 and exported["lora_alpha"] == 3 and exported["lora_dropout"] == 0 and exported["use_dora"] is (mode == "dora"), "portable PEFT settings changed")
    return {"result": result, "manifest": manifest, "reports": reports, "process": process, "slots": slots}


def validate_phase_offline(mode, phase, prep, output):
    """Add new evidence for an existing invocation; never rerun or rewrite it."""
    require(not output.exists(), "validation output already exists; no overwrites")
    result = validate_phase(mode, phase, prep)
    verify_source(prep)
    invocation = ROOT / "executions" / f"{mode}-{phase}"
    config_path = ROOT / f"{mode}-{phase}.json"
    folder = Path(load(config_path, 64 * 1024)["output_dir"])
    files = {"process.json": digest(invocation / "process.json"), "start.json": digest(invocation / "start.json"), "stdout.jsonl": digest(invocation / "stdout.jsonl"), "config.json": digest(config_path)}
    for name in ("run.json", "result.json", "progress.jsonl", "latest.safetensors"):
        files[name] = digest(folder / name, 4 * MIB)
    if phase != "paused":
        for name in sorted(MODEL_FILES):
            files["model/" + name] = digest(folder / "model" / name, 4 * MIB)
    receipt = {"format": "antfly.gliner25-published-inactive-phase-validation/v2", "mode": mode, "phase": phase, "status": "pass", "model_execution": False, "config_comparison_policy": CONFIG_COMPARISON_POLICY, "checker": digest(Path(__file__)), "consumed_helpers": consumed_helpers(result["process"]), "preparation_sha256": PREPARATION_SHA256, "files": files, "result": result["result"], "slots": result["slots"], "stdout_events": len(result["reports"]) + 1}
    write_new(output, receipt)
    print(json.dumps(receipt, allow_nan=False))
    return receipt


def run_phase(args, prep):
    binary = args.binary.resolve(strict=True)
    observed = digest(binary)
    require(observed["sha256"] == args.binary_sha256 and args.binary_sha256 not in prep["rejected_pre_correction_binary_sha256"], "wrong or pre-correction binary pin")
    verify_source(prep)
    if args.phase == "resumed":
        require(validate_phase(args.mode, "paused", prep)["process"]["binary"] == observed, "resume executable differs from paused invocation")
    config_path = resolve_config(args.mode, args.phase, prep)
    config = load(config_path, 64 * 1024)
    require(not Path(config["output_dir"]).exists(), "output already exists; no retries or overwrites")
    folder = ROOT / "executions" / f"{args.mode}-{args.phase}"
    folder.parent.mkdir(exist_ok=True)
    folder.mkdir(mode=0o700)
    command = [str(binary), "finetune", "train", "gliner25", str(config_path), "--shutdown-grace-seconds", "30"]
    if args.phase == "paused":
        command += ["--stop-after-microbatches", "1"]
    receipt = {"format": "antfly.gliner25-published-inactive-cli-process/v1", "command": command, "binary": observed, "config": digest(config_path, 64 * 1024), "preparation_sha256": PREPARATION_SHA256, "driver": digest(Path(__file__)), "supervision": digest(Path(supervision.__file__)), "outer_timeout_seconds": config["timeout_seconds"] + 90, "max_child_tree_rss_bytes": 2 * 1024**3, "max_stdout_bytes": 4 * MIB, "max_stderr_bytes": 4 * MIB, "failure": None, "returncode": None}
    write_new(folder / "start.json", receipt)
    started = time.monotonic()
    try:
        receipt.update(supervision.run(command, folder / "stdout.jsonl", folder / "stderr.log", timeout_seconds=receipt["outer_timeout_seconds"]))
        require(receipt["failure"] is None and receipt["returncode"] == 0 and receipt["cleanup"]["complete"], receipt["failure"] or "incomplete owned process cleanup")
        require(digest(binary) == observed, "executable changed during invocation")
        require(digest(config_path, 64 * 1024) == receipt["config"], "config changed during invocation")
        require(digest(Path(__file__)) == receipt["driver"] and digest(Path(supervision.__file__)) == receipt["supervision"], "driver changed during invocation")
        verify_source(prep)
    except BaseException as error:
        if receipt["failure"] is None:
            receipt["failure"] = f"{type(error).__name__}: {error}"
        raise
    finally:
        receipt["elapsed_seconds"] = time.monotonic() - started
        write_new(folder / "process.json", receipt)
    result = validate_phase(args.mode, args.phase, prep)
    write_new(folder / "validation.json", {"format": "antfly.gliner25-published-inactive-phase/v1", "mode": args.mode, "phase": args.phase, "result": result["result"], "slots": result["slots"], "stdout_events": len(result["reports"]) + 1})
    print(json.dumps({"mode": args.mode, "phase": args.phase, "status": "pass", "result": result["result"]}, allow_nan=False))


def validate_all(mode, prep):
    phases = {phase: validate_phase(mode, phase, prep) for phase in PHASES}
    whole, pause, resume = (phases[x] for x in PHASES)
    require(whole["process"]["binary"] == pause["process"]["binary"] == resume["process"]["binary"], "cross-invocation executable changed")
    require(whole["result"] == resume["result"], "final result/state differs after fresh resume")
    require(pause["result"]["run_fingerprint"] == whole["result"]["run_fingerprint"], "paused run semantics changed")
    stitched = pause["reports"] + resume["reports"]
    require(len(stitched) == len(whole["reports"]), "missing resumed progress")
    for original, restored in zip(whole["reports"], stitched):
        require({k: original[k] for k in SEMANTIC_REPORT} == {k: restored[k] for k in SEMANTIC_REPORT}, "resumed semantic progress/decisions differ")
    artifacts = {}
    for relative in ["result.json", "latest.safetensors", *["model/" + name for name in sorted(MODEL_FILES)]]:
        before = digest(ROOT / f"{mode}-uninterrupted" / relative, 4 * MIB)
        require(before == digest(ROOT / f"{mode}-resumed" / relative, 4 * MIB), f"nonidentical final bytes: {relative}")
        artifacts[relative] = before
    verify_source(prep)
    receipt = {"format": "antfly.gliner25-published-inactive-resume-validation/v1", "mode": mode, "status": "pass", "binary": whole["process"]["binary"], "preparation_sha256": PREPARATION_SHA256, "source": prep["source"], "data": prep["data"], "microbatches": 5, "optimizer_updates": 3, "fallback_sequence": [False, True, False, True, True], "flush_after": [2, 4, 5], "state_sha256": whole["result"]["state_sha256"], "run_fingerprint": whole["result"]["run_fingerprint"], "artifacts": artifacts, "resource_peaks": {phase: {"host": max(x["host_peak_bytes"] for x in item["reports"]), "backend": max(x["backend_peak_bytes"] for x in item["reports"]), "child_tree_rss": item["process"]["peak_child_tree_rss_bytes"]} for phase, item in phases.items()}, "claim": prep["scope"], "quality_or_published_source_numeric_qualification": False}
    write_new(ROOT / f"{mode}-validation.json", receipt)
    print(json.dumps(receipt, allow_nan=False))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("inspect", help="verify authored/preparation bytes only; no model or binary launch")
    launch = sub.add_parser("run", help="explicitly execute exactly one public CLI invocation")
    launch.add_argument("--mode", choices=("lora", "dora"), required=True)
    launch.add_argument("--phase", choices=PHASES, required=True)
    launch.add_argument("--binary", type=Path, required=True)
    launch.add_argument("--binary-sha256", required=True)
    offline = sub.add_parser("validate", help="check completed three-phase receipts/artifacts without model execution")
    offline.add_argument("--mode", choices=("lora", "dora"), required=True)
    phase_check = sub.add_parser("validate-phase", help="add a new validation receipt for one completed invocation without running a process or replacing historical receipts")
    phase_check.add_argument("--mode", choices=("lora", "dora"), required=True)
    phase_check.add_argument("--phase", choices=PHASES, required=True)
    phase_check.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    def terminate(_signal, _frame):
        raise InterruptedError("driver termination requested")
    signal.signal(signal.SIGTERM, terminate)
    prep = preparation()
    if args.command == "inspect":
        print(json.dumps({"preparation": PREPARATION_SHA256, "model_execution": False, "recipe": prep["recipe"], "cpu_admission": prep["cpu_admission"]}, indent=2))
    elif args.command == "run":
        run_phase(args, prep)
    elif args.command == "validate-phase":
        validate_phase_offline(args.mode, args.phase, prep, args.output)
    else:
        validate_all(args.mode, prep)


if __name__ == "__main__":
    main()
