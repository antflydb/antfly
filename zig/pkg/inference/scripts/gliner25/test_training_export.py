from __future__ import annotations

import contextlib
import copy
import hashlib
import json
import os
from pathlib import Path
import re
import struct
import tempfile
import unittest
from unittest import mock

import check_training_export as check


def write_json(path, value):
    path.write_text(json.dumps(value, separators=(",", ":")) + "\n")
    return check.digest_bytes(path.read_bytes())


def write_tensors(path, entries):
    header, payload = {}, bytearray()
    for name, (shape, values) in sorted(entries.items()):
        raw = struct.pack("<" + "f" * len(values), *values)
        header[name] = {"dtype": "F32", "shape": shape, "data_offsets": [len(payload), len(payload) + len(raw)]}
        payload.extend(raw)
    encoded = json.dumps(header, separators=(",", ":")).encode()
    encoded += b" " * (-len(encoded) % 8)
    path.write_bytes(struct.pack("<Q", len(encoded)) + encoded + payload)
    return check.digest_bytes(path.read_bytes())


def scaffold(root, mode):
    source, export, job = (root / name for name in ("source", "export", "job"))
    for directory in (source, export, job):
        directory.mkdir()
    original = {
        "encoder.encoder.layer.0.weight": ([2, 2], [1., 2., 3., 4.]),
        "encoder.encoder.layer.0.bias": ([2], [0., 1.]),
        "classifier.weight": ([2, 2], [2., 3., 4., 5.]),
        "classifier.bias": ([2], [0., 0.]),
    }
    pins = {"model.safetensors": write_tensors(source / "model.safetensors", original)}
    for name in check.SIDECARS:
        path = source / name
        path.parent.mkdir(parents=True, exist_ok=True)
        pins[name] = write_json(path, {"name": name})
    identity = check.source_identity("small", pins)
    provenance = {"run_fingerprint": list(range(32)), "dataset_sha256": [2] * 32, "schemas_sha256": [3] * 32,
                  "optimizer_identity": {"optimizer_step": 2, "microbatch_step": 4}, "accumulated_microbatches": 0}
    receipt = {"family": "gliner_boundary_training_snapshot/v1", "version": 1, "architecture_version": 1,
        "config_version": 3, "tensor_policy_version": 1, "mode": mode, "source": identity, "provenance": provenance,
        "adapter_layout_sha256": None, "weights": None, "sidecars": identity["sidecars"], "adapter_config": None, "adapter_receipt": None}
    inventory = {name: {"shape": value[0], "dtype": "F32"} for name, value in original.items()}
    if mode in ("full", "heads"):
        entries = copy.deepcopy(original)
        entries["classifier.weight"][1][0] += 0.5
        receipt["weights"] = write_tensors(export / "model.safetensors", entries)
        for name in check.SIDECARS:
            target = export / name
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes((source / name).read_bytes())
    else:
        config = {"peft_type": "LORA", "task_type": None, "base_model_name_or_path": "/old/source/location",
            "revision": None, "r": 1, "lora_alpha": 2., "lora_dropout": .125,
            "target_modules": ["encoder.encoder.layer.0"], "bias": "none", "use_dora": mode == "dora",
            "fan_in_fan_out": False, "inference_mode": True}
        expected, modules, targets = check.adapter_contract(config, inventory, mode)
        entries = {name: (item["shape"], [float(index + 1)] * (2 if len(item["shape"]) == 1 else item["shape"][0] * item["shape"][1])) for index, (name, item) in enumerate(expected.items())}
        receipt["weights"] = write_tensors(export / "adapter_model.safetensors", entries)
        receipt["sidecars"] = None
        receipt["adapter_layout_sha256"] = list(bytes.fromhex(check.layout_digest("small", mode, config, modules)))
        receipt["adapter_config"] = write_json(export / "adapter_config.json", config)
        with check.Opened(export / "adapter_model.safetensors", check.MAX_WEIGHT) as file:
            parameters = check.parameter_digest(file, check.tensor_header(file), modules)
        adapter_receipt = {"family": "gliner_boundary_adapter/v1", "version": 1, "architecture_version": 1, "config_version": 3,
            "source": identity, "schema_sha256": provenance["schemas_sha256"], "frozen_weight_sha256": pins["model.safetensors"]["sha256"],
            "target_sha256": targets, "parameter_sha256": parameters, "config": receipt["adapter_config"], "weights": receipt["weights"]}
        receipt["adapter_receipt"] = write_json(export / check.ADAPTER_RECEIPT, adapter_receipt)
    receipt_pin = write_json(export / check.RECEIPT, receipt)
    run = {"format": "antfly.gliner25-training-run/v1", "source": identity, "config": {"run": {"mode": mode}},
           "run_fingerprint": provenance["run_fingerprint"], "train_sha256": provenance["dataset_sha256"], "schema_sha256": provenance["schemas_sha256"]}
    result = {"version": 1, "status": "complete", "accumulated_microbatches": 0, "identity": provenance["optimizer_identity"],
        "run_fingerprint": provenance["run_fingerprint"], "state_sha256": [4] * 32,
        "portable_model": {"mode": mode, "weights": receipt["weights"], "provenance": receipt_pin}}
    write_json(job / "run.json", run)
    selected = {"weight::" + check.checkpoint_name(name, mode): ([len(value[1])], value[1]) for name, value in sorted(entries.items()) if mode != "heads" or not name.startswith("encoder.")}
    for index, (name, (_, values)) in enumerate(list(selected.items())):
        slot = name.removeprefix("weight::")
        selected["adam_m::" + slot] = ([len(values)], [0.] * len(values))
        selected["adam_v::" + slot] = ([len(values)], [0.] * len(values))
        selected[f"__extension.seeded.gradient.{index}"] = ([len(values)], [0.] * len(values))
        selected["adam_step_u32::" + slot] = ([4], [2., 0., 0., 0.])
    count = sum(name.startswith("weight::") for name in selected)
    selected["__trainer_counters"] = ([8], [4., 0., 0., 0., 2., 0., 0., 0.])
    selected["__extension.seeded.counters"] = ([3], [1., 0., 2.])
    selected["__extension.seeded.presence"] = ([count], [0.] * count)
    selected["__run_fingerprint"] = ([32], [5.] * 32)
    write_tensors(job / "latest.safetensors", selected)
    exported_inventory = {name: {"shape": shape, "size_bytes": 4 * len(values)} for name, (shape, values) in entries.items()}
    with check.Opened(job / "latest.safetensors", check.MAX_CHECKPOINT) as file:
        result["state_sha256"] = check.checkpoint_state_digest(file, check.tensor_header(file), exported_inventory, mode)[0]
    write_json(job / "result.json", result)
    return source, export, job, pins, inventory


class TrainingExportTests(unittest.TestCase):
    @contextlib.contextmanager
    def prepared(self, mode="heads"):
        with tempfile.TemporaryDirectory() as temporary:
            source, export, job, pins, inventory = scaffold(Path(temporary), mode)
            with mock.patch.object(check, "expected_source", return_value=pins), mock.patch.object(check, "published_inventory", return_value=inventory):
                yield source, export, job, pins, inventory

    def test_all_four_modes_verify_receipts_inventory_and_final_owned_weights_without_runtime(self):
        for mode in ("full", "heads", "lora", "dora"):
            with self.subTest(mode=mode), self.prepared(mode) as (source, export, job, _, _):
                result = check.audit_export("small", source, export, job)
                self.assertEqual(result["mode"], mode)
                self.assertFalse(result["qualification"])
                self.assertFalse(result["numerical_runtime_executed"])
                self.assertGreater(result["job"]["exact_owned_parameter_count"], 0)

    def test_raw_zig_digest_supports_arrays_hex_and_valid_utf8_string(self):
        for raw in (bytes(range(32)), b"\0" * 32, b"a" * 32, "\u00e9".encode() * 16):
            self.assertEqual(check.hex_digest(list(raw)), raw.hex())
            self.assertEqual(check.hex_digest(raw.hex()), raw.hex())
            self.assertEqual(check.hex_digest(raw.decode()), raw.hex())
        for invalid in ([True] * 32, [-1] * 32, "bad", [0] * 31):
            with self.assertRaises(check.oracle.ContractError):
                check.hex_digest(invalid)

    def test_all_published_metadata_inventory_is_pinned_and_complete(self):
        for variant in ("small", "base", "multi"):
            self.assertEqual(len(check.published_inventory(variant)), 334)

    def test_checker_versions_match_native_config_contract(self):
        source = (check.HERE.parent.parent / "src/models/gliner_boundary.zig").read_text()
        for key in ("config_version", "architecture_version"):
            actual = int(re.search(rf"pub const {key}: u32 = (\d+);", source).group(1))
            self.assertEqual(actual, check.NATIVE_VERSIONS[key])

    def test_missing_or_same_size_modified_source_is_rejected(self):
        with self.prepared() as (source, export, _, _, _):
            path = source / "config.json"
            raw = path.read_bytes()
            path.write_bytes(raw.replace(b"name", b"namo"))
            with self.assertRaisesRegex(check.oracle.ContractError, "identity differs"):
                check.audit_export("small", source, export)

    def test_extra_loader_file_is_rejected(self):
        with self.prepared() as (source, export, _, _, _):
            (export / "pytorch_model.bin").write_bytes(b"fallback")
            with self.assertRaisesRegex(check.oracle.ContractError, "unmanifested"):
                check.audit_export("small", source, export)

    def test_frozen_encoder_mutation_is_rejected_even_with_updated_receipt(self):
        with self.prepared() as (source, export, _, _, _):
            path = export / "model.safetensors"
            with check.Opened(path, check.MAX_WEIGHT) as opened:
                header = check.tensor_header(opened)
            raw = bytearray(path.read_bytes())
            struct.pack_into("<f", raw, header["encoder.encoder.layer.0.weight"]["offset"], 9.)
            path.write_bytes(raw)
            receipt, _ = check.read_json(export / check.RECEIPT)
            receipt["weights"] = check.digest_bytes(raw)
            write_json(export / check.RECEIPT, receipt)
            with self.assertRaisesRegex(check.oracle.ContractError, "frozen encoder"):
                check.audit_export("small", source, export)

    def test_checkpoint_overlay_mismatch_fails_without_reloading_model(self):
        with self.prepared() as (source, export, job, _, _):
            path = job / "latest.safetensors"
            raw = bytearray(path.read_bytes())
            raw[-1] ^= 1
            path.write_bytes(raw)
            with self.assertRaisesRegex(check.oracle.ContractError, "final owned checkpoint"):
                check.audit_export("small", source, export, job)

    def test_final_optimizer_state_tamper_fails_with_unchanged_exported_weights(self):
        for name, value, message in (("adam_m::classifier.weight", .25, "state receipt differs"),
                                      ("__extension.seeded.gradient.0", .25, "state receipt differs"),
                                      ("__extension.seeded.presence", 1., "pending gradient presence")):
            with self.subTest(tensor=name), self.prepared() as (source, export, job, _, _):
                path = job / "latest.safetensors"
                with check.Opened(path, check.MAX_CHECKPOINT) as file:
                    offset = check.tensor_header(file)[name]["offset"]
                raw = bytearray(path.read_bytes())
                struct.pack_into("<f", raw, offset, value)
                path.write_bytes(raw)
                # The portable model and its receipt still pass independently.
                self.assertEqual(check.audit_export("small", source, export)["mode"], "heads")
                with self.assertRaisesRegex(check.oracle.ContractError, message):
                    check.audit_export("small", source, export, job)

    def test_malformed_receipt_integer_and_rounded_peft_settings_fail(self):
        for name in ("version", "architecture_version", "config_version", "tensor_policy_version"):
            with self.subTest(version=name), self.prepared() as (source, export, _, _, _):
                receipt, _ = check.read_json(export / check.RECEIPT)
                receipt[name] = True
                write_json(export / check.RECEIPT, receipt)
                with self.assertRaisesRegex(check.oracle.ContractError, "unsupported training receipt"):
                    check.audit_export("small", source, export)
        with self.prepared("lora") as (_, export, _, _, inventory):
            config, _ = check.read_json(export / "adapter_config.json")
            for name, value in (("lora_alpha", 1e-50), ("lora_alpha", 1e39), ("lora_dropout", .999999999)):
                with self.subTest(setting=name, value=value), self.assertRaises(check.oracle.ContractError):
                    check.adapter_contract(config | {name: value}, inventory, "lora")

    def test_incomplete_adapter_inventory_and_non_linear_targets_fail(self):
        with self.prepared("dora") as (_, export, _, _, inventory):
            config, _ = check.read_json(export / "adapter_config.json")
            expected, _, _ = check.adapter_contract(config, inventory, "dora")
            with self.assertRaisesRegex(check.oracle.ContractError, "inventory"):
                check.validate_inventory(dict(list(expected.items())[:-1]), expected)
            del inventory["encoder.encoder.layer.0.bias"]
            with self.assertRaisesRegex(check.oracle.ContractError, "biased Linear"):
                check.adapter_contract(config, inventory, "dora")

    def test_unknown_peft_behavior_and_wrong_target_digest_fail(self):
        with self.prepared("lora") as (source, export, _, _, inventory):
            config, _ = check.read_json(export / "adapter_config.json")
            config["modules_to_save"] = ["classifier"]
            with self.assertRaisesRegex(check.oracle.ContractError, "fields differ"):
                check.adapter_contract(config, inventory, "lora")
            receipt, _ = check.read_json(export / check.RECEIPT)
            receipt["adapter_layout_sha256"] = [0] * 32
            write_json(export / check.RECEIPT, receipt)
            with self.assertRaisesRegex(check.oracle.ContractError, "layout digest"):
                check.audit_export("small", source, export)

    def test_truncated_overlapping_and_oversized_headers_fail(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "weights"
            for raw in (b"short", struct.pack("<Q", check.MAX_HEADER + 1)):
                path.write_bytes(raw)
                with check.Opened(path, check.MAX_WEIGHT) as file, self.assertRaises(check.oracle.ContractError):
                    check.tensor_header(file)
            header = {"a": {"dtype": "F32", "shape": [1], "data_offsets": [0, 4]}, "b": {"dtype": "F32", "shape": [1], "data_offsets": [0, 4]}}
            encoded = json.dumps(header).encode()
            path.write_bytes(struct.pack("<Q", len(encoded)) + encoded + b"\0" * 4)
            with check.Opened(path, check.MAX_WEIGHT) as file, self.assertRaisesRegex(check.oracle.ContractError, "overlap"):
                check.tensor_header(file)

    def test_fifo_is_rejected_before_read_and_network_is_denied(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "fifo"
            os.mkfifo(path)
            with self.assertRaisesRegex(check.oracle.ContractError, "regular"):
                check.Opened(path, 16)
        with check.deny_network(), self.assertRaisesRegex(check.oracle.ContractError, "network"):
            check.socket.create_connection(("example.invalid", 443))

    def test_runtime_private_copy_rechecks_exact_bytes_and_budget(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "source"
            source.mkdir()
            (source / "config.json").write_bytes(b"fixed")
            pins = {"config.json": check.digest_bytes(b"fixed")}
            with self.assertRaisesRegex(check.oracle.ContractError, "admission"):
                check.private_copy(source, root / "small", pins, 4)
            (source / "config.json").write_bytes(b"other")
            with self.assertRaisesRegex(check.oracle.ContractError, "identity"):
                check.private_copy(source, root / "copy", pins, 16)


if __name__ == "__main__":
    unittest.main()
