#!/usr/bin/env python3
"""Offline integrity and optional pinned-upstream loading of training exports.

The default path imports no numerical runtime. Runtime execution is explicit,
uses private verified file copies, and never qualifies model/task quality.
"""
from __future__ import annotations

import argparse
import contextlib
import hashlib
import importlib.metadata
import json
import math
import os
from pathlib import Path
import shutil
import socket
import stat
import struct
import sys
import tempfile
import time
import warnings
from unittest import mock

import oracle

HERE = Path(__file__).resolve().parent
CONTRACT = HERE / "training_export_contract.json"
SIDECARS = ("config.json", "encoder_config/config.json", "tokenizer.json", "tokenizer_config.json")
RECEIPT = "antfly_gliner25_training.json"
ADAPTER_RECEIPT = "antfly_gliner25_adapter.json"
CHUNK = 1024 * 1024
MAX_WEIGHT = 2 * 1024 * CHUNK
MAX_CHECKPOINT = 8 * 1024 * CHUNK
MAX_HEADER = 4 * CHUNK
MAX_JSON = CHUNK
SCOPE = "gliner25_training_export_compatibility/v1"
NATIVE_VERSIONS = {"version": 1, "architecture_version": 1, "config_version": 3, "tensor_policy_version": 1}


def checked(condition, reason):
    if not condition:
        raise oracle.ContractError(reason)


def decode(raw):
    def invalid(value):
        raise oracle.ContractError("nonfinite JSON: " + value)
    return json.loads(raw, object_pairs_hook=oracle._unique_object, parse_constant=invalid)


def digest_bytes(raw):
    return {"size_bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()}


def hex_digest(value):
    if isinstance(value, list):
        checked(len(value) == 32 and all(type(x) is int and 0 <= x <= 255 for x in value), "invalid byte digest")
        return bytes(value).hex()
    if isinstance(value, str) and len(value) == 64 and all(c in "0123456789abcdef" for c in value):
        return value
    # Zig serializes a [32]u8 as a string when those raw bytes happen to be
    # valid UTF-8, including escaped controls. Recover those exact bytes.
    checked(isinstance(value, str) and len(value.encode("utf-8")) == 32, "invalid raw/hex digest")
    return value.encode("utf-8").hex()


def normalized_pin(value):
    checked(isinstance(value, dict) and set(value) == {"size_bytes", "sha256"}, "invalid file pin")
    checked(type(value["size_bytes"]) is int and value["size_bytes"] >= 0, "invalid pinned size")
    return {"size_bytes": value["size_bytes"], "sha256": hex_digest(value["sha256"])}


def stable_stat(info):
    return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns)


class Opened:
    """Bounded reads and hashing through one regular descriptor, never mmap."""
    def __init__(self, path, maximum):
        self.path = Path(path)
        fd = os.open(self.path, os.O_RDONLY | os.O_NONBLOCK | os.O_CLOEXEC)
        try:
            self.before = os.fstat(fd)
            checked(stat.S_ISREG(self.before.st_mode), f"not a regular file: {path}")
            checked(0 <= self.before.st_size <= maximum, f"file exceeds limit: {path}")
            self.file = os.fdopen(fd, "rb")
        except BaseException:
            os.close(fd)
            raise

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        try:
            if exc_type is None:
                checked(stable_stat(os.fstat(self.file.fileno())) == stable_stat(self.before), f"file changed: {self.path}")
        finally:
            self.file.close()

    def chunks(self, offset=0, length=None):
        if length is None:
            length = self.before.st_size - offset
        checked(0 <= offset <= self.before.st_size and 0 <= length <= self.before.st_size - offset, "invalid file range")
        self.file.seek(offset)
        while length:
            block = self.file.read(min(CHUNK, length))
            checked(bool(block), f"short read: {self.path}")
            length -= len(block)
            yield block

    def pin(self):
        h = hashlib.sha256()
        for block in self.chunks():
            h.update(block)
        return {"size_bytes": self.before.st_size, "sha256": h.hexdigest()}

    def json(self):
        checked(self.before.st_size <= MAX_JSON, "JSON exceeds limit")
        return decode(b"".join(self.chunks()))


def read_json(path):
    with Opened(path, MAX_JSON) as source:
        return source.json(), source.pin()


def verify_pin(actual, expected, label):
    checked(actual == normalized_pin(expected), f"file identity differs: {label}")


def tensor_header(source):
    checked(source.before.st_size >= 8, "truncated safetensors prefix")
    source.file.seek(0)
    size = struct.unpack("<Q", source.file.read(8))[0]
    checked(0 < size <= MAX_HEADER and size <= source.before.st_size - 8, "invalid safetensors header length")
    header = decode(source.file.read(size))
    checked(isinstance(header, dict) and 0 < len(header) <= 16385, "invalid safetensors header")
    tensors, spans = {}, []
    for name, entry in header.items():
        if name == "__metadata__":
            checked(isinstance(entry, dict) and all(isinstance(k, str) and isinstance(v, str) for k, v in entry.items()), "invalid safetensors metadata")
            continue
        checked(isinstance(name, str) and 0 < len(name.encode()) <= 2048 and isinstance(entry, dict), "invalid tensor name/descriptor")
        checked(set(entry) == {"dtype", "shape", "data_offsets"} and entry["dtype"] == "F32", "only complete F32 descriptors are supported")
        shape, offsets = entry["shape"], entry["data_offsets"]
        checked(isinstance(shape, list) and len(shape) <= 8 and all(type(x) is int and 0 < x <= 2**31 - 1 for x in shape), "invalid tensor shape")
        checked(isinstance(offsets, list) and len(offsets) == 2 and all(type(x) is int for x in offsets), "invalid tensor offsets")
        start, end = offsets
        checked(0 <= start <= end <= source.before.st_size - 8 - size and end - start == math.prod(shape) * 4, "tensor shape/range differs")
        checked(start % 4 == 0 and end % 4 == 0, "unaligned tensor range")
        tensors[name] = {"shape": shape, "dtype": "F32", "offset": 8 + size + start, "size_bytes": end - start}
        spans.append((start, end))
    checked(bool(tensors), "empty tensor inventory")
    cursor = 0
    for start, end in sorted(spans):
        checked(start == cursor, "tensor overlap or hole")
        cursor = end
    checked(cursor == source.before.st_size - 8 - size, "unaccounted tensor payload")
    return tensors


def tensor_inventory(source, tensors):
    result = {}
    for name, tensor in sorted(tensors.items()):
        h = hashlib.sha256()
        for block in source.chunks(tensor["offset"], tensor["size_bytes"]):
            h.update(block)
        result[name] = {"shape": tensor["shape"], "dtype": "F32", "size_bytes": tensor["size_bytes"], "sha256": h.hexdigest()}
    return result


def expected_source(variant):
    entry = oracle.load_manifest()["models"][variant]
    return {name: {k: value[k] for k in ("size_bytes", "sha256")} for name, value in entry["files"].items()}


def published_inventory(variant):
    name = f"models/{variant}/tensor_inventory.json"
    manifest = oracle.read_json(oracle.FIXTURES / "reference_manifest.json")
    data, pin = read_json(oracle.FIXTURES / name)
    verify_pin(pin, manifest["files"][name], name)
    checked(data["model"] == variant and len(data["tensors"]) == 334, "published inventory identity differs")
    return {name: {"shape": value["shape"], "dtype": "F32"} for name, value in data["tensors"].items()}


def validate_inventory(actual, expected):
    checked(set(actual) == set(expected), "tensor inventory missing or unexpected names")
    for name in expected:
        checked(actual[name]["shape"] == expected[name]["shape"] and actual[name]["dtype"] == expected[name]["dtype"], "tensor shape/dtype differs: " + name)


def source_identity(variant, pins):
    return {"backbone": variant, "precision": "fp32", "weight": pins["model.safetensors"], "sidecars": [pins[name] for name in SIDECARS]}


def validate_tree(directory, allowed, *, source=False):
    checked(directory.is_dir(), "artifact directory missing")
    seen = set()
    for root, dirs, files in os.walk(directory, followlinks=False):
        for name in dirs:
            checked(not (Path(root) / name).is_symlink(), "symlink directory is unsupported")
        for name in files:
            path = Path(root) / name
            relative = str(path.relative_to(directory))
            checked(source or not path.is_symlink(), "export files must not be symlinks")
            checked(relative in allowed, "unmanifested loader-affecting file: " + relative)
            seen.add(relative)
    checked(set(allowed) - seen <= ({"README.md", ".gitattributes", "GitHub_new.jpg"} if source else set()), "artifact file missing")


def adapter_contract(config, inventory, mode):
    required = {"peft_type", "task_type", "base_model_name_or_path", "revision", "r", "lora_alpha", "lora_dropout", "target_modules", "bias", "use_dora", "fan_in_fan_out", "inference_mode"}
    checked(isinstance(config, dict) and set(config) == required, "training export PEFT fields differ")
    checked(config["peft_type"] == "LORA" and config["task_type"] is None and config["revision"] is None and config["bias"] == "none" and config["fan_in_fan_out"] is False and config["inference_mode"] is True and config["use_dora"] is (mode == "dora"), "unsupported PEFT behavior")
    checked(config["base_model_name_or_path"] is None or isinstance(config["base_model_name_or_path"], str), "invalid base locator")
    rank, alpha, dropout = config["r"], config["lora_alpha"], config["lora_dropout"]
    checked(type(rank) is int and 0 < rank <= 1024, "invalid PEFT rank")
    checked(type(alpha) in (float, int) and math.isfinite(alpha) and alpha > 0 and type(dropout) in (float, int) and math.isfinite(dropout) and 0 <= dropout < 1, "invalid PEFT scale/dropout")
    checked(alpha <= 3.4028234663852886e38, "PEFT alpha exceeds float32")
    rounded_alpha, rounded_dropout = struct.unpack("<ff", struct.pack("<ff", alpha, dropout))
    checked(rounded_alpha > 0 and rounded_dropout < 1, "PEFT scale/dropout invalid after float32 rounding")
    targets = config["target_modules"]
    checked(isinstance(targets, list) and 0 < len(targets) <= 4096 and all(isinstance(x, str) and 0 < len(x) <= 1024 for x in targets) and len(set(targets)) == len(targets), "invalid PEFT targets")
    expected, modules = {}, []
    h = hashlib.sha256(b"antfly-gliner25-peft-targets/v1\x00" + mode.encode() + struct.pack("<Iff", rank, alpha, dropout))
    for module in sorted(targets):
        weight = inventory.get(module + ".weight", {})
        shape = weight.get("shape", [])
        checked(len(shape) == 2 and inventory.get(module + ".bias", {}).get("shape") == [shape[0]], "PEFT target is not a published biased Linear: " + module)
        out_dim, in_dim = shape
        prefix = "base_model.model." + module
        keys = [prefix + ".lora_A.weight", prefix + ".lora_B.weight"]
        expected[keys[0]] = {"shape": [rank, in_dim], "dtype": "F32"}
        expected[keys[1]] = {"shape": [out_dim, rank], "dtype": "F32"}
        if mode == "dora":
            keys.append(prefix + ".lora_magnitude_vector")
            expected[keys[-1]] = {"shape": [out_dim], "dtype": "F32"}
        modules.append({"name": module, "keys": keys, "in_dim": in_dim, "out_dim": out_dim})
        raw = module.encode()
        h.update(struct.pack("<I", len(raw)) + raw + struct.pack("<II", in_dim, out_dim))
    return expected, modules, h.hexdigest()


def parameter_digest(source, tensors, modules):
    h = hashlib.sha256(b"antfly-gliner25-peft-parameters/v1\x00")
    for module in modules:
        h.update(module["name"].encode() + b"\x00")
        for key in module["keys"]:
            tensor = tensors[key]
            h.update(struct.pack("<Q", tensor["size_bytes"] // 4))
            for block in source.chunks(tensor["offset"], tensor["size_bytes"]):
                h.update(block)
        if len(module["keys"]) == 2:
            h.update(struct.pack("<Q", 0))
    return h.hexdigest()


def layout_digest(variant, mode, config, modules):
    h = hashlib.sha256(b"antfly-gliner25-model-peft-layout/v1\x00")
    def text(value):
        encoded = value.encode()
        h.update(struct.pack("<I", len(encoded)) + encoded)
    text(variant)
    text(mode)
    h.update(struct.pack("<IffI", config["r"], config["lora_alpha"], config["lora_dropout"], len(modules)))
    for module in modules:
        name = module["name"]
        canonical_weight = name + ".weight"
        native_weight = canonical_weight.removeprefix("encoder.") if name.startswith("encoder.encoder.layer.") else canonical_weight
        keys = module["keys"]
        for value in (name, native_weight, checkpoint_name(keys[0], mode), checkpoint_name(keys[1], mode), keys[0], keys[1]):
            text(value)
        if mode == "dora":
            text(checkpoint_name(keys[2], mode))
            text(keys[2])
        h.update(struct.pack("<II", module["in_dim"], module["out_dim"]))
    return h.hexdigest()


def checkpoint_name(name, mode):
    if mode in ("full", "heads"):
        return name.removeprefix("encoder.")
    if name.endswith(".lora_magnitude_vector"):
        return name + ".default.weight"
    return name.removesuffix(".weight") + ".default.weight"


def small_tensor(source, header, name, count):
    tensor = header.get(name)
    checked(tensor is not None and tensor["size_bytes"] == count * 4 and count <= 16384, "invalid checkpoint scalar state: " + name)
    return struct.unpack("<" + "f" * count, b"".join(source.chunks(tensor["offset"], tensor["size_bytes"])))


def integers(values, maximum):
    checked(all(math.isfinite(x) and 0 <= x <= maximum and x == int(x) for x in values), "invalid checkpoint integer encoding")
    return [int(x) for x in values]


def checkpoint_state_digest(source, header, exported, mode):
    selected = [(checkpoint_name(name, mode), tensor) for name, tensor in sorted(exported.items()) if mode != "heads" or not name.startswith("encoder.")]
    words = integers(small_tensor(source, header, "__trainer_counters", 8), 65535)
    microbatch = sum(words[i] << (16 * i) for i in range(4))
    optimizer = sum(words[i + 4] << (16 * i) for i in range(4))
    version, accumulated, accumulation = integers(small_tensor(source, header, "__extension.seeded.counters", 3), 65536)
    checked(version == 1 and accumulated == 0 and 0 < accumulation <= 65536 and optimizer <= microbatch, "checkpoint is not a complete flushed state")
    presence = integers(small_tensor(source, header, "__extension.seeded.presence", len(selected)), 1)
    checked(not any(presence), "completed checkpoint retains pending gradient presence")
    optimizer_contract = bytes(integers(small_tensor(source, header, "__run_fingerprint", 32), 255))
    h = hashlib.sha256(b"antfly.seeded-gradient-state.v1\x00" + optimizer_contract)
    def number(value):
        h.update(struct.pack("<Q", value))
    for value in (optimizer, microbatch, accumulated, len(selected)):
        number(value)
    for index, (name, tensor) in enumerate(selected):
        raw = name.encode()
        number(len(raw))
        h.update(raw)
        number(len(tensor["shape"]))
        for dimension in tensor["shape"]:
            number(dimension)
        step_bytes = integers(small_tensor(source, header, "adam_step_u32::" + name, 4), 255)
        step = int.from_bytes(bytes(step_bytes), "little")
        checked(step <= optimizer, "parameter Adam counter exceeds global updates")
        for value in (step, step, presence[index]):
            number(value)
        for key in ("weight::" + name, "adam_m::" + name, "adam_v::" + name, f"__extension.seeded.gradient.{index}"):
            saved = header.get(key)
            checked(saved is not None and saved["size_bytes"] == tensor["size_bytes"], "checkpoint state shape differs: " + key)
            number(saved["size_bytes"] // 4)
            for block in source.chunks(saved["offset"], saved["size_bytes"]):
                h.update(block)
    return h.hexdigest(), {"optimizer_step": optimizer, "microbatch_step": microbatch}


def check_job(directory, receipt, receipt_pin, exported, mode):
    manifest, manifest_pin = read_json(directory / "run.json")
    result, result_pin = read_json(directory / "result.json")
    provenance = receipt["provenance"]
    checked(manifest["format"] == "antfly.gliner25-training-run/v1" and manifest["source"] == receipt["source"] and manifest["config"]["run"]["mode"] == mode, "job source/mode differs")
    checked(type(result["version"]) is int and result["version"] == 1 and result["status"] == "complete" and type(result["accumulated_microbatches"]) is int and result["accumulated_microbatches"] == 0, "job has no complete flushed result")
    checked(result["identity"] == provenance["optimizer_identity"] and hex_digest(result["run_fingerprint"]) == hex_digest(provenance["run_fingerprint"]) == hex_digest(manifest["run_fingerprint"]), "job provenance differs")
    checked(hex_digest(manifest["train_sha256"]) == hex_digest(provenance["dataset_sha256"]) and hex_digest(manifest["schema_sha256"]) == hex_digest(provenance["schemas_sha256"]), "job data/schema differs")
    checked(result["portable_model"]["mode"] == mode, "job exported mode differs")
    verify_pin(result["portable_model"]["weights"], receipt["weights"], "job weights")
    verify_pin(result["portable_model"]["provenance"], receipt_pin, "job export receipt")
    expected = {"weight::" + checkpoint_name(name, mode): tensor for name, tensor in exported.items() if mode != "heads" or not name.startswith("encoder.")}
    with Opened(directory / "latest.safetensors", MAX_CHECKPOINT) as file:
        header = tensor_header(file)
        selected = {name: entry for name, entry in header.items() if name.startswith("weight::")}
        checked(set(selected) == set(expected), "final checkpoint owned parameter inventory differs")
        actual = tensor_inventory(file, selected)
        for name, tensor in expected.items():
            checked(actual[name]["size_bytes"] == tensor["size_bytes"] and actual[name]["sha256"] == tensor["sha256"], "export differs from final owned checkpoint: " + name)
        state_digest, checkpoint_identity = checkpoint_state_digest(file, header, exported, mode)
        checked(state_digest == hex_digest(result["state_sha256"]) and checkpoint_identity == result["identity"], "final checkpoint state receipt differs")
        checkpoint_pin = file.pin()
    return {"run": manifest_pin, "result": result_pin, "checkpoint": checkpoint_pin, "exact_owned_parameter_count": len(expected), "result_state_sha256": hex_digest(result["state_sha256"]), "state_digest_recomputed": True}


def audit_export(variant, source_dir, export_dir, run_dir=None):
    source_dir, export_dir = Path(source_dir), Path(export_dir)
    expected, inventory = expected_source(variant), published_inventory(variant)
    validate_tree(source_dir, set(expected) | {"README.md", ".gitattributes", "GitHub_new.jpg"}, source=True)
    source_pins = {}
    for name, pin in expected.items():
        with Opened(source_dir / name, MAX_WEIGHT if name == "model.safetensors" else 32 * CHUNK) as file:
            source_pins[name] = file.pin()
            verify_pin(source_pins[name], pin, "source/" + name)
            if name == "model.safetensors":
                source_header = tensor_header(file)
                validate_inventory(source_header, inventory)
                source_tensors = tensor_inventory(file, source_header)
    receipt, receipt_pin = read_json(export_dir / RECEIPT)
    checked(set(receipt) == {"family", "version", "architecture_version", "config_version", "tensor_policy_version", "mode", "source", "provenance", "adapter_layout_sha256", "weights", "sidecars", "adapter_config", "adapter_receipt"}, "training receipt fields differ")
    checked(receipt["family"] == "gliner_boundary_training_snapshot/v1" and all(type(receipt[k]) is int and receipt[k] == value for k, value in NATIVE_VERSIONS.items()), "unsupported training receipt")
    mode = receipt["mode"]
    checked(mode in ("full", "heads", "lora", "dora") and receipt["source"] == source_identity(variant, source_pins), "export source/mode differs")
    provenance = receipt["provenance"]
    checked(set(provenance) == {"run_fingerprint", "dataset_sha256", "schemas_sha256", "optimizer_identity", "accumulated_microbatches"}, "training provenance fields differ")
    for name in ("run_fingerprint", "dataset_sha256", "schemas_sha256"):
        hex_digest(provenance[name])
    identity = provenance["optimizer_identity"]
    checked(set(identity) == {"optimizer_step", "microbatch_step"} and all(type(x) is int and 0 <= x < 2**64 for x in identity.values()) and identity["optimizer_step"] <= identity["microbatch_step"] and type(provenance["accumulated_microbatches"]) is int and provenance["accumulated_microbatches"] == 0, "invalid or unflushed optimizer identity")
    adapter = mode in ("lora", "dora")
    weight_name = "adapter_model.safetensors" if adapter else "model.safetensors"
    files = {RECEIPT: receipt_pin}
    allowed = {RECEIPT, weight_name, "adapter_config.json", ADAPTER_RECEIPT} if adapter else {RECEIPT, weight_name, *SIDECARS}
    validate_tree(export_dir, allowed)
    modules = []
    with Opened(export_dir / weight_name, 512 * CHUNK if adapter else MAX_WEIGHT) as file:
        files[weight_name] = file.pin()
        verify_pin(files[weight_name], receipt["weights"], "export weights")
        header = tensor_header(file)
        if adapter:
            checked(receipt["sidecars"] is None, "adapter must bind source sidecars separately")
            hex_digest(receipt["adapter_layout_sha256"])
            config, files["adapter_config.json"] = read_json(export_dir / "adapter_config.json")
            adapter_receipt, files[ADAPTER_RECEIPT] = read_json(export_dir / ADAPTER_RECEIPT)
            verify_pin(files["adapter_config.json"], receipt["adapter_config"], "adapter config")
            verify_pin(files[ADAPTER_RECEIPT], receipt["adapter_receipt"], "adapter receipt")
            adapter_inventory, modules, target_digest = adapter_contract(config, inventory, mode)
            validate_inventory(header, adapter_inventory)
            checked(hex_digest(receipt["adapter_layout_sha256"]) == layout_digest(variant, mode, config, modules), "adapter global layout digest differs")
            checked(set(adapter_receipt) == {"family", "version", "architecture_version", "config_version", "source", "schema_sha256", "frozen_weight_sha256", "target_sha256", "parameter_sha256", "config", "weights"}, "adapter receipt fields differ")
            checked(adapter_receipt["family"] == "gliner_boundary_adapter/v1" and all(type(adapter_receipt[k]) is int and adapter_receipt[k] == NATIVE_VERSIONS[k] for k in ("version", "architecture_version", "config_version")), "unsupported adapter receipt")
            checked(adapter_receipt["source"] == receipt["source"] and hex_digest(adapter_receipt["schema_sha256"]) == hex_digest(provenance["schemas_sha256"]) and hex_digest(adapter_receipt["frozen_weight_sha256"]) == source_pins["model.safetensors"]["sha256"], "adapter source/schema binding differs")
            verify_pin(files[weight_name], adapter_receipt["weights"], "adapter weights")
            verify_pin(files["adapter_config.json"], adapter_receipt["config"], "adapter config binding")
            checked(hex_digest(adapter_receipt["target_sha256"]) == target_digest and hex_digest(adapter_receipt["parameter_sha256"]) == parameter_digest(file, header, modules), "adapter target/parameter digest differs")
        else:
            checked(receipt["adapter_layout_sha256"] is None and receipt["adapter_config"] is None and receipt["adapter_receipt"] is None and receipt["sidecars"] == receipt["source"]["sidecars"], "full/head receipt fields differ")
            validate_inventory(header, inventory)
            for name in SIDECARS:
                with Opened(export_dir / name, 32 * CHUNK) as sidecar:
                    files[name] = sidecar.pin()
                    verify_pin(files[name], source_pins[name], name)
        exported_tensors = tensor_inventory(file, header)
    changed = [] if adapter else [name for name in source_tensors if exported_tensors[name]["sha256"] != source_tensors[name]["sha256"]]
    if mode == "heads":
        checked(not any(name.startswith("encoder.") for name in changed), "head-only export changed a frozen encoder tensor")
    job = check_job(Path(run_dir), receipt, receipt_pin, exported_tensors, mode) if run_dir else None
    return {"scope": SCOPE, "status": "verified", "qualification": False, "variant": variant, "mode": mode,
            "source_files": source_pins, "export_files": files, "source_tensors": source_tensors, "export_tensors": exported_tensors,
            "changed_source_parameters": changed, "adapter_modules": modules, "provenance": provenance, "job": job,
            "static_scope": "exact_file_tensor_inventory_and_byte_integrity", "numerical_runtime_executed": False,
            "generator_sha256": oracle.sha256_file(Path(__file__)), "oracle_sha256": oracle.sha256_file(Path(oracle.__file__)),
            "contract_sha256": oracle.sha256_file(CONTRACT)}


@contextlib.contextmanager
def deny_network():
    def denied(*args, **kwargs):
        raise oracle.ContractError("network is forbidden during export verification")
    with mock.patch.object(socket.socket, "connect", denied), mock.patch.object(socket.socket, "connect_ex", denied), mock.patch.object(socket, "create_connection", denied):
        yield


def private_copy(source, destination, files, maximum):
    checked(sum(pin["size_bytes"] for pin in files.values()) <= maximum, "runtime copy exceeds disk admission")
    checked(shutil.disk_usage(destination.parent).free >= sum(pin["size_bytes"] for pin in files.values()) + 256 * CHUNK, "insufficient disk headroom for runtime snapshot")
    destination.mkdir(mode=0o700)
    for name, expected in files.items():
        path = destination / name
        path.parent.mkdir(parents=True, exist_ok=True)
        with Opened(source / name, maximum) as original, path.open("xb") as output:
            h, size = hashlib.sha256(), 0
            for block in original.chunks():
                output.write(block)
                h.update(block)
                size += len(block)
            verify_pin({"size_bytes": size, "sha256": h.hexdigest()}, expected, "runtime snapshot/" + name)


def verify_loaded_state(state, expected, torch):
    checked(set(state) == set(expected), "loaded state inventory differs; missing weights are forbidden")
    for name, pin in expected.items():
        tensor = state[name].detach()
        checked(tensor.dtype == torch.float32 and tensor.device.type == "cpu" and list(tensor.shape) == pin["shape"], "loaded tensor dtype/shape differs: " + name)
        values = tensor.contiguous().view(-1)
        for start in range(0, values.numel(), 262144):
            checked(bool(torch.isfinite(values[start:start + 262144]).all()), "nonfinite loaded tensor: " + name)
        raw = memoryview(values.numpy()).cast("B")
        checked(len(raw) == pin["size_bytes"] and hashlib.sha256(raw).hexdigest() == pin["sha256"], "loaded tensor bytes differ: " + name)


def verify_runtime_sources():
    contract = oracle.read_json(CONTRACT)
    checked(type(contract["version"]) is int and contract["version"] == 1 and contract["scope"] == SCOPE and contract["upstream_commit"] == oracle.UPSTREAM_COMMIT, "export runtime contract differs")
    package = importlib.metadata.distribution("peft")
    checked(package.version == contract["peft_version"], "PEFT loader version differs")
    for name, pin in contract["peft_loader_sources"].items():
        with Opened(package.locate_file(name), MAX_JSON) as source:
            verify_pin(source.pin(), pin, name)
    return contract


@contextlib.contextmanager
def runtime_context(upstream, output, profile, peft_wheel):
    if profile == "oracle-0.17.1":
        checked(peft_wheel is None, "a PEFT wheel requires the explicit export loader profile")
        provenance, torch = oracle.prepare_runtime(Path(upstream))
        verify_runtime_sources()
        yield provenance, torch
    else:
        from training_export_runtime import PROFILE, prepared_runtime
        checked(profile == PROFILE and peft_wheel is not None, "invalid export loader profile/wheel")
        with prepared_runtime(Path(upstream), output, peft_wheel) as prepared:
            yield prepared


def runtime_check(report, source_dir, export_dir, upstream, output, requests, copy_limit,
                  runtime_profile="oracle-0.17.1", peft_wheel=None):
    checked(sys.byteorder == "little", "runtime tensor-byte comparison requires a little-endian host")
    checked(not any(name in sys.modules for name in ("torch", "gliner2", "peft")), "start runtime verification in a fresh process")
    adapter = report["mode"] in ("lora", "dora")
    copies = sum(pin["size_bytes"] for pin in report["export_files"].values()) + (sum(pin["size_bytes"] for pin in report["source_files"].values()) if adapter else 0)
    if runtime_profile != "oracle-0.17.1":
        from training_export_runtime import load_profile
        copies += sum(pin["size_bytes"] for pin in load_profile()["wheel_files"].values())
    checked(copies <= copy_limit, "aggregate runtime file copies exceed admission")
    with tempfile.TemporaryDirectory(prefix=".training-export-runtime-", dir=output) as temporary, deny_network():
        stage = Path(temporary)
        private_copy(Path(export_dir), stage / "export", report["export_files"], copy_limit)
        if adapter:
            private_copy(Path(source_dir), stage / "source", report["source_files"], copy_limit)
        with runtime_context(upstream, output, runtime_profile, peft_wheel) as (provenance, torch):
            from gliner2 import AutoExtractor
            started = time.monotonic()
            model = AutoExtractor.from_pretrained(str(stage / ("source" if adapter else "export")), local_files_only=True,
                map_location="cpu", use_flashdeberta=False, quantize=False, compile=False)
            verify_loaded_state(model.state_dict(), report["source_tensors"] if adapter else report["export_tensors"], torch)
            if adapter:
                from peft import PeftModel
                from peft.utils.save_and_load import get_peft_model_state_dict
                with warnings.catch_warnings():
                    warnings.filterwarnings("error", message="Found missing adapter keys.*", category=UserWarning)
                    model = PeftModel.from_pretrained(model, str(stage / "export"), is_trainable=False,
                        local_files_only=True, autocast_adapter_dtype=False)
                verify_loaded_state(get_peft_model_state_dict(model), report["export_tensors"], torch)
                base = model.get_base_model().state_dict()
                modules = {entry["name"] for entry in report["adapter_modules"]}
                restored_base = {}
                for name in report["source_tensors"]:
                    module, leaf = name.rsplit(".", 1)
                    restored_base[name] = base[module + ".base_layer." + leaf if module in modules else name]
                verify_loaded_state(restored_base, report["source_tensors"], torch)
            model.eval()
            runtime = {"status": "loaded", "provenance": provenance, "load_seconds": time.monotonic() - started,
                       "loader_profile": runtime_profile,
                       "all_loaded_tensor_bytes_equal": True, "missing_weight_fallback": False, "network_allowed": False,
                       "private_copy_bytes": copies, "quality_evaluation": False}
            if requests is not None:
                request_doc, request_pin = read_json(requests)
                checked(request_doc.get("format_version") == 1 and 0 < len(request_doc.get("requests", [])) <= 32, "invalid bounded request fixture")
                runtime["requests"] = request_pin
                # Copy exact request bytes before capture, preserving schema order.
                private_copy(Path(requests).parent, stage / "requests", {Path(requests).name: request_pin}, MAX_JSON)
                runtime["outputs"] = oracle.capture_requests(model, stage / "requests" / Path(requests).name, output, torch)
            oracle.verify_upstream_checkout(Path(upstream))
        return runtime


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--variant", choices=("small", "base", "multi"), required=True)
    parser.add_argument("--source-dir", type=Path, required=True)
    parser.add_argument("--export-dir", type=Path, required=True)
    parser.add_argument("--run-dir", type=Path)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--runtime", action="store_true", help="explicitly load the actual model; requires the shared compute lane")
    parser.add_argument("--runtime-profile", choices=("oracle-0.17.1", "peft-0.18.0-export-v1"), default="oracle-0.17.1",
                        help="explicit separate PEFT loader profile; never changes the frozen training oracle")
    parser.add_argument("--peft-wheel", type=Path, help="exact official PEFT0.18.0 wheel for the isolated export profile")
    parser.add_argument("--requests", type=Path, help="optional version-1 bounded oracle extraction fixture; requires --runtime")
    parser.add_argument("--upstream", type=Path, default=Path("/private/tmp/antfly-gliner25-upstream"))
    parser.add_argument("--max-runtime-copy-bytes", type=int, default=3 * 1024 * CHUNK)
    args = parser.parse_args(argv)
    checked(args.runtime or args.requests is None, "--requests requires explicit --runtime")
    checked(args.runtime or (args.runtime_profile == "oracle-0.17.1" and args.peft_wheel is None), "loader selection requires explicit --runtime")
    checked((args.runtime_profile == "oracle-0.17.1") == (args.peft_wheel is None), "isolated loader profile requires exactly one pinned wheel")
    checked(0 < args.max_runtime_copy_bytes <= MAX_CHECKPOINT, "invalid runtime copy limit")
    for artifact in (args.source_dir, args.export_dir, args.run_dir):
        if artifact is not None:
            checked(not args.output_dir.resolve().is_relative_to(artifact.resolve()), "report output must be outside input artifacts")
    with oracle.atomic_output_directory(args.output_dir) as output:
        report = audit_export(args.variant, args.source_dir, args.export_dir, args.run_dir)
        if args.runtime:
            report["runtime"] = runtime_check(report, args.source_dir, args.export_dir, args.upstream, output, args.requests,
                                              args.max_runtime_copy_bytes, args.runtime_profile, args.peft_wheel)
            report["numerical_runtime_executed"] = True
        oracle.write_json(output / "report.json", report)
    print(json.dumps({"scope": SCOPE, "status": "verified", "mode": report["mode"], "runtime": args.runtime,
                      "qualification": False, "report_sha256": oracle.sha256_file(args.output_dir / "report.json")}))


if __name__ == "__main__":
    try:
        main()
    except (oracle.ContractError, OSError, KeyError, TypeError, ValueError) as exc:
        raise SystemExit("training export check failed: " + str(exc)) from exc
