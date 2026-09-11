"""Pure driver regressions. No subprocesses, numerical runtimes, or models."""
import copy
import contextlib
import importlib.util
import io
import json
import math
from pathlib import Path
import struct
import tempfile
import unittest
from unittest import mock

spec = importlib.util.spec_from_file_location("inactive_driver", Path(__file__).with_name("validate.py"))
driver = importlib.util.module_from_spec(spec)
spec.loader.exec_module(driver)


def reports():
    values = []
    for index in range(5):
        inactive = index in (1, 3, 4)
        terms = {key: 0.0 for key in driver.TERM_NAMES}
        terms.update(start=1.0, total=1.0 if inactive else 1.25, classification=0.0 if inactive else 0.25)
        values.append({"epoch": 0, "batch": index, "examples": 1, "terms": terms,
                       "coverage": {"gold_mentions": 1, "proposed_gold_mentions": 1, "gold_relations": 0, "proposed_gold_relations": 0, "matched_records": 0},
                       "optimizer": {"identity": {"optimizer_step": (index + 1) // 2, "microbatch_step": index + 1}, "optimizer_stepped": index in (1, 3), "accumulated_microbatches": (index + 1) % 2, "loss": 0.0 if inactive else 1.25, "grad_norm": 0.5},
                       "decision_fingerprint": list(range(32)), "zero_loss_fallback": inactive})
    values.append({"epoch": 0, "batch": 5, "examples": 0, "terms": None,
                   "optimizer": {"identity": {"optimizer_step": 3, "microbatch_step": 5}, "optimizer_stepped": True, "accumulated_microbatches": 0, "grad_norm": 0.0}})
    return values


def checkpoint(mode, paused):
    rows = {}
    def add(name, shape, values):
        rows[name] = (shape, values)
    add("__trainer_counters", [8], [1 if paused else 5, 0, 0, 0, 0 if paused else 3, 0, 0, 0])
    add("__run_fingerprint", [32], list(range(32)))
    add("__extension.seeded.counters", [3], [1, int(paused), 2])
    shapes = driver.slot_shapes(mode)
    add("__extension.seeded.presence", [len(shapes)], [int(paused)] * len(shapes))
    for index, (name, shape) in enumerate(shapes.items()):
        size = driver.math.prod(shape)
        add("weight::" + name, [size], [0.1] * size)
        add("adam_m::" + name, [size], [0.0 if paused else 0.25] * size)
        add("adam_v::" + name, [size], [0.0 if paused else 0.125] * size)
        add("adam_step_u32::" + name, [4], [0 if paused else 3, 0, 0, 0])
        add("adam_step::" + name, [1], [0 if paused else 3])
        add("__extension.seeded.gradient." + str(index), [size], [0.25 if paused else 0.0] * size)
    return rows


def save_tensors(path, rows):
    header = {}
    data = bytearray()
    for name, (shape, values) in rows.items():
        start = len(data)
        data.extend(struct.pack("<" + "f" * len(values), *values))
        header[name] = {"dtype": "F32", "shape": shape, "data_offsets": [start, len(data)]}
    raw = json.dumps(header).encode()
    path.write_bytes(struct.pack("<Q", len(raw)) + raw + data)


class DriverTests(unittest.TestCase):
    def test_checkpoint_outer_run_binding_and_numeric_tokens_are_exact(self):
        raw = b'{"backend":"native","run_fingerprint":' + json.dumps(list(range(32))).encode() + b',"config":{"run":{"mode":"lora","epochs":1,"batch_size":1,"accumulation":2,"max_optimizer_steps":null,"scheduler":"constant","warmup_steps":0,"beta1":0.8999999761581421,"beta2":0.9990000128746033,"epsilon":0.00000000999999993922529,"weight_decay":0.009999999776482582,"task_lr":0.0005000000237487257,"max_grad_norm":0.699999988079071}}}'
        original = driver.checkpoint_contract_fingerprint("lora", raw)
        self.assertEqual(len(original), 32)
        self.assertNotEqual(original, list(range(32)))
        for changed in (raw.replace(b'0.8999999761581421', b'0.8'), raw.replace(b'0.0005000000237487257', b'0.0006000000284984708'), raw.replace(b'"run_fingerprint":[0,', b'"run_fingerprint":[1,')):
            self.assertNotEqual(driver.checkpoint_contract_fingerprint("lora", changed), original)
        self.assertEqual(driver.compact_original_numbers({"epsilon": driver.JsonFloatToken("0.00000000999999993922529")}), '{"epsilon":0.00000000999999993922529}')
        self.assertNotEqual(driver.checkpoint_contract_fingerprint("lora", raw.replace(b'0.00000000999999993922529', b'9.99999993922529e-9')), original)

    def test_global_counter_limb_encoding_and_exact_owned_state_digest(self):
        self.assertEqual(driver.encoded_global_counter([1.0, 2.0, 3.0, 4.0]), 1 + (2 << 16) + (3 << 32) + (4 << 48))
        with self.assertRaisesRegex(ValueError, "word-encoded"):
            driver.encoded_global_counter([65536.0, 0.0, 0.0, 0.0])
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "state.safetensors"
            rows = checkpoint("lora", True)
            save_tensors(path, rows)
            result = driver.validate_checkpoint(path, "lora", True, list(range(32)))
            driver.validate_checkpoint(path, "lora", True, list(range(32)), result["state_sha256"])
            key = next(name for name in rows if name.startswith("weight::"))
            rows[key][1][0] += 0.01
            save_tensors(path, rows)
            with self.assertRaisesRegex(ValueError, "owned state digest mismatch"):
                driver.validate_checkpoint(path, "lora", True, list(range(32)), result["state_sha256"])

    def test_executable_snapshot_compares_exact_digest_bytes_and_size(self):
        value = {"size_bytes": 123, "sha256": list(range(32))}
        expected = {"size_bytes": 123, "sha256": bytes(range(32)).hex()}
        self.assertEqual(driver.executable_snapshot_digest(value), expected)
        for key, changed in (("size_bytes", 124), ("sha256", [0] * 32)):
            mutated = copy.deepcopy(value)
            mutated[key] = changed
            self.assertNotEqual(driver.executable_snapshot_digest(mutated), expected)
        for changed in (bytes(range(32)).hex(), list(range(31)), [False] + list(range(1, 32)), [256] + list(range(1, 32))):
            with self.assertRaises(ValueError):
                driver.executable_snapshot_digest({"size_bytes": 123, "sha256": changed})
        with self.assertRaises(ValueError):
            driver.executable_snapshot_digest({"size_bytes": 123.0, "sha256": list(range(32))})

    def test_declared_f32_fields_match_production_roundtrip_exactly(self):
        for path in driver.F32_CONFIG_FIELDS:
            expected = 0.1
            actual = struct.unpack("<f", struct.pack("<f", expected))[0]
            self.assertNotEqual(expected, actual)
            driver.config_subset(expected, actual, path)
        driver.config_subset({"run": {"encoder_lr": 1e-5, "seed": 257713, "warmup_ratio": 0.1}, "peft": {"alpha": 3}},
                             {"run": {"encoder_lr": struct.unpack("<f", struct.pack("<f", 1e-5))[0], "seed": 257713, "warmup_ratio": 0.1}, "peft": {"alpha": 3}})

    def test_changed_f32_including_subulp_observed_json_is_rejected(self):
        canonical = struct.unpack("<f", struct.pack("<f", 1e-5))[0]
        for actual in (0.00002, math.nextafter(canonical, math.inf), 1e-5):
            with self.assertRaisesRegex(ValueError, "changed consumed config.run.encoder_lr"):
                driver.config_subset(1e-5, actual, "config.run.encoder_lr")
        for actual in (True, "0.1", float("inf")):
            with self.assertRaises(ValueError):
                driver.config_subset(0.1, actual, "config.peft.dropout")
        with self.assertRaisesRegex(ValueError, "out-of-range f32"):
            driver.config_subset(1e100, 1e100, "config.run.epsilon")

    def test_integers_booleans_arrays_and_f64_are_not_relaxed(self):
        for expected, actual in ((2, 2.0), (1, True), (True, 1), (2**53 + 1, float(2**53 + 1)), ([1, 2], [True, 2])):
            with self.assertRaisesRegex(ValueError, "changed consumed"):
                driver.config_subset(expected, actual, "config.run.seed")
        driver.config_subset(2**53 + 1, 2**53 + 1, "config.run.seed")
        rounded = struct.unpack("<f", struct.pack("<f", 0.1))[0]
        with self.assertRaisesRegex(ValueError, "changed consumed"):
            driver.config_subset(0.1, rounded, "config.run.warmup_ratio")
        with self.assertRaisesRegex(ValueError, "changed consumed"):
            driver.config_subset(0.1, rounded, "config.unlisted_field")
        driver.config_subset(0, 0, "config.run.warmup_ratio")

    def test_historical_helpers_must_match_recorded_bytes(self):
        with tempfile.TemporaryDirectory() as folder, mock.patch.object(driver, "ROOT", Path(folder)):
            archive = Path(folder) / "helper-archive" / "v1"
            archive.mkdir(parents=True)
            for name in ("validate.py", "supervision.py"):
                (Path(folder) / name).write_bytes(b"current helper")
                (archive / name).write_bytes(b"historical helper")
            process = {"driver": driver.digest(archive / "validate.py"), "supervision": driver.digest(archive / "supervision.py")}
            found = driver.consumed_helpers(process)
            self.assertEqual(found["driver"]["digest"], process["driver"])
            self.assertEqual(found["driver"]["path"], str(archive / "validate.py"))
            (archive / "validate.py").write_bytes(b"substituted bytes")
            with self.assertRaisesRegex(ValueError, "historical driver bytes"):
                driver.consumed_helpers(process)

    def test_offline_phase_adds_receipt_without_process_or_historical_writes(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            invocation = root / "executions" / "lora-paused"
            invocation.mkdir(parents=True)
            output = root / "lora-paused"
            output.mkdir()
            config_path = root / "lora-paused.json"
            config_path.write_text(json.dumps({"output_dir": str(output)}))
            paths = [config_path]
            for parent, names in ((invocation, ("process.json", "start.json", "stdout.jsonl")), (output, ("run.json", "result.json", "progress.jsonl", "latest.safetensors"))):
                for name in names:
                    path = parent / name
                    path.write_bytes(b"historical bytes")
                    paths.append(path)
            before = {str(path): path.read_bytes() for path in paths}
            result = {"result": {"status": "paused"}, "slots": {"slots": 4}, "reports": [{}], "process": {"driver": {"sha256": "historical"}}}
            receipt_path = invocation / "validation-v2.json"
            with mock.patch.object(driver, "ROOT", root), mock.patch.object(driver, "validate_phase", return_value=result), mock.patch.object(driver, "verify_source"), mock.patch.object(driver, "consumed_helpers", return_value=result["process"]), mock.patch.object(driver.supervision, "run", side_effect=AssertionError("offline validation launched a process")), contextlib.redirect_stdout(io.StringIO()):
                receipt = driver.validate_phase_offline("lora", "paused", {}, receipt_path)
                self.assertFalse(receipt["model_execution"])
                self.assertEqual(receipt["consumed_helpers"]["driver"]["sha256"], "historical")
                self.assertEqual(receipt["checker"], driver.digest(Path(driver.__file__)))
                with self.assertRaisesRegex(ValueError, "no overwrites"):
                    driver.validate_phase_offline("lora", "paused", {}, receipt_path)
            self.assertEqual(before, {str(path): path.read_bytes() for path in paths})

    def test_preparation_and_three_progress_phases_without_execution(self):
        prep = driver.preparation()
        self.assertFalse(prep["model_execution"])
        self.assertLess(prep["cpu_admission"]["upper_bound_using_source_ceiling_bytes"], prep["cpu_admission"]["combined_limit_bytes"])
        value = reports()
        driver.validate_reports(value, "uninterrupted")
        driver.validate_reports(value[:1], "paused")
        driver.validate_reports(value[1:], "resumed")

    def test_inactive_raw_terms_cannot_replace_optimizer_zero(self):
        value = reports()
        value[1]["optimizer"]["loss"] = value[1]["terms"]["total"]
        with self.assertRaisesRegex(ValueError, "inactive objective"):
            driver.validate_reports(value, "uninterrupted")

    def test_raw_frozen_objective_must_survive_fallback(self):
        value = reports()
        value[1]["terms"]["total"] = 0
        with self.assertRaisesRegex(ValueError, "raw frozen-task"):
            driver.validate_reports(value, "uninterrupted")

    def test_old_report_missing_fallback_flag_rejected(self):
        value = reports()
        del value[1]["zero_loss_fallback"]
        with self.assertRaisesRegex(ValueError, "missing report field"):
            driver.validate_reports(value, "uninterrupted")

    def test_no_skipped_inactive_microbatch_or_final_partial_flush(self):
        for value in (reports()[:4] + reports()[5:], reports()[:-1]):
            with self.assertRaisesRegex(ValueError, "missing/extra progress"):
                driver.validate_reports(value, "uninterrupted")
        value = reports()
        value[-1]["optimizer"]["grad_norm"] = 0.1
        with self.assertRaisesRegex(ValueError, "zero norm"):
            driver.validate_reports(value, "uninterrupted")

    def test_inactive_counts_and_authored_gold_are_required(self):
        value = reports()
        value[1]["optimizer"]["identity"]["microbatch_step"] = 1
        with self.assertRaisesRegex(ValueError, "wrong microbatch"):
            driver.validate_reports(value, "uninterrupted")
        value = reports()
        value[3]["coverage"]["proposed_gold_mentions"] = 0
        with self.assertRaisesRegex(ValueError, "gold mention"):
            driver.validate_reports(value, "uninterrupted")

    def test_exact_lora_dora_checkpoint_counts_and_presence(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "state.safetensors"
            for mode in ("lora", "dora"):
                for paused in (False, True):
                    save_tensors(path, checkpoint(mode, paused))
                    result = driver.validate_checkpoint(path, mode, paused, list(range(32)))
                    self.assertEqual(result["all_slot_updates"], 0 if paused else 3)
                    self.assertEqual(result["slots"], 4 if mode == "lora" else 6)

    def test_inactive_slot_update_or_extra_target_rejected(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "state.safetensors"
            data = checkpoint("dora", False)
            key = next(name for name in data if name.startswith("adam_step_u32::"))
            data[key] = ([4], [2, 0, 0, 0])
            save_tensors(path, data)
            with self.assertRaisesRegex(ValueError, "all three updates"):
                driver.validate_checkpoint(path, "dora", False, list(range(32)))
            data = checkpoint("lora", False)
            data["weight::base_model.model.encoder.unwanted"] = ([1], [1.0])
            save_tensors(path, data)
            with self.assertRaisesRegex(ValueError, "omitted or added"):
                driver.validate_checkpoint(path, "lora", False, list(range(32)))

    def test_wrong_resume_pin_and_uncleared_presence_rejected(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "state.safetensors"
            data = checkpoint("lora", False)
            save_tensors(path, data)
            with self.assertRaisesRegex(ValueError, "fingerprint changed"):
                driver.validate_checkpoint(path, "lora", False, [0] * 32)
            data["__extension.seeded.presence"] = ([4], [1] * 4)
            save_tensors(path, data)
            with self.assertRaisesRegex(ValueError, "gradient presence"):
                driver.validate_checkpoint(path, "lora", False, list(range(32)))

    def test_duplicate_nonfinite_json_and_oversized_payload_rejected(self):
        for raw in ('{"a":1,"a":2}', '{"x":NaN}'):
            with self.assertRaises(ValueError):
                driver.loads(raw)
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "state.safetensors"
            path.write_bytes(struct.pack("<Q", driver.MIB + 1) + b"{}")
            with self.assertRaisesRegex(ValueError, "header length"):
                driver.tensors(path)

    def test_strict_original_targets_and_initializer_are_prepared(self):
        for mode in ("lora", "dora"):
            value = driver.load(driver.ROOT / f"{mode}-uninterrupted.json", 65536)
            self.assertEqual(value["peft"]["targets"], ["classification_head"])
            self.assertEqual(value["run"]["epochs"], 1)
            self.assertEqual(value["run"]["accumulation"], 2)
            self.assertNotIn("synthetic", json.dumps(value))
            template = driver.load(driver.ROOT / f"{mode}-resumed.template.json", 65536)
            self.assertIsInstance(template["expected_restore_state_sha256"], str)
            self.assertEqual(driver.slot_shapes(mode, saved=True)["base_model.model.classifier.3.lora_B.weight"], [1, 2])


if __name__ == "__main__":
    unittest.main()
