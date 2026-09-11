import copy
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import benchmark_cpu
import check_bundles as bundles
from check_bundles import compare
import oracle


def lifted(output):
    """Synthetic complete native DTO from the smaller FP32 fixture contract."""
    output = copy.deepcopy(output)
    for group in output["entities"]:
        group["dtype"] = "list"
    for group in output["classifications"]:
        group["multi_label"] = len(group["labels"]) != 1
    for group in output["structures"]:
        for record in group["instances"]:
            record.update(confidence=0.75, anchor=None)
            for field in record["fields"]:
                field["dtype"] = "list"

    def visit(value):
        if isinstance(value, dict):
            if "source" in value and "text" in value:
                value.setdefault("derived", False)
                for group in value.get("attributes", []):
                    group["multi_label"] = len(group["labels"]) != 1
            for item in value.values():
                visit(item)
        elif isinstance(value, list):
            for item in value:
                visit(item)
    visit(output)
    return output


def ready(receipt, fixture_sha256, backend="native"):
    return {"event": "ready", "scope": "converted_bundle_diagnostic", "backend": backend,
            "math_policy": bundles.MATH_POLICY, "qualification": False, "receipt": receipt,
            "fixture_sha256": fixture_sha256, "weight_precision": receipt["precision"],
            "activation_precision": "f32", "accumulation_precision": "f32", "head_precision": "f32"}


def reference():
    expected = {"entities": [{"name": "person", "values": [{"text": "John", "confidence": 0.9, "source": None, "attributes": []}]}],
                "classifications": [], "relations": [], "structures": []}
    actual = copy.deepcopy(expected)
    actual["entities"][0]["values"][0]["text"] = "Jane"
    backend = bundles.backend_result(lifted(actual))
    receipt = {"precision": "q8_0", "backbone": "small", "source_files": [{"sha256": "1" * 64}], "files": [{"sha256": "2" * 64}]}
    fixture = {"cases": [{"id": f"case{i}", "expected": expected} for i in range(10)]}
    tokens = {case["id"]: [1, 2, 3] for case in fixture["cases"]}
    entry = {"variant": "small", "precision": "q8_0", "status": "complete", "qualification": False,
             "backend": "native", "math_policy": bundles.MATH_POLICY, "receipt_sha256": "3" * 64,
             "fixture_sha256": "4" * 64, "ready": ready(receipt, "4" * 64),
             "cases": [{"case_id": case["id"], "token_ids_equal": True, "input_ids": tokens[case["id"]],
                        "expected": benchmark_cpu.canonical_result(expected), "actual": benchmark_cpu.canonical_result(actual),
                        "backend_output": backend,
                        "source_fp32": compare(benchmark_cpu.canonical_result(expected), benchmark_cpu.canonical_result(actual))}
                       for case in fixture["cases"]]}
    report = {"format_version": 2, "scope": bundles.REPORT_SCOPE, "status": "complete", "qualification": False,
              "backend": "native", "math_policy": bundles.MATH_POLICY, "source_commit": oracle.UPSTREAM_COMMIT,
              "token_reference_report_sha256": "5" * 64, "driver_sha256": "6" * 64,
              "binary_sha256": "7" * 64, "bundles": [entry]}
    return report, receipt, fixture, tokens


class BundleComparisonTests(unittest.TestCase):
    def test_confidence_changes_do_not_hide_changed_decisions(self):
        expected = [{"text": "John", "confidence": 0.9}, {"text": "Mary", "confidence": 0.8}]
        reordered = [expected[1], expected[0]]
        result = compare(expected, reordered)
        self.assertFalse(result["decisions_equal"])
        self.assertFalse(result["fp32_reference_tolerance_pass"])
        self.assertEqual(result["aligned_confidence_count"], 0)
        missing = compare(expected, expected[:1])
        self.assertEqual(missing["decision_differences"], ["output: count"])

    def test_aligned_confidence_delta_is_separate_from_decisions(self):
        result = compare({"text": "John", "confidence": 0.9}, {"text": "John", "confidence": 0.899})
        self.assertTrue(result["decisions_equal"])
        self.assertFalse(result["fp32_reference_tolerance_pass"])
        self.assertAlmostEqual(result["max_aligned_confidence_absolute_error"], 0.001)
        self.assertEqual(result["aligned_confidence_count"], 1)

    def test_nonfinite_and_boolean_confidences_are_invalid(self):
        for score in (float("nan"), float("inf"), True):
            with self.assertRaises(benchmark_cpu.BenchmarkError):
                compare({"confidence": 0.9}, {"confidence": score})

    def test_unmatched_values_cannot_hide_invalid_confidence(self):
        for value in (float("nan"), True):
            with self.assertRaises(benchmark_cpu.BenchmarkError):
                compare([], [{"text": "new", "confidence": value}])

    def test_same_bundle_parity_is_independent_of_source_quality(self):
        report, receipt, fixture, tokens = reference()
        bundles.validate_native_reference(report, "5" * 64, "6" * 64)
        found = bundles.matching_native_bundle(report, receipt, "3" * 64, "4" * 64, fixture, tokens)
        self.assertFalse(found["cases"][0]["source_fp32"]["decisions_equal"])
        result = bundles.compare_backends(found["cases"][0]["backend_output"], found["cases"][0]["backend_output"])
        self.assertTrue(result["parity_pass"])
        self.assertNotIn("fp32_reference_tolerance_pass", result)
        changed = copy.deepcopy(found["cases"][0]["backend_output"])
        changed["entities"][0]["values"][0]["confidence"] -= 0.001
        self.assertFalse(bundles.compare_backends(found["cases"][0]["backend_output"], changed)["parity_pass"])

    def test_stale_native_profile_or_driver_is_rejected(self):
        report, _, _, _ = reference()
        for key, value in (("format_version", 1), ("status", "incomplete"), ("backend", "metal"),
                           ("math_policy", "legacy"), ("driver_sha256", "0" * 64),
                           ("token_reference_report_sha256", "0" * 64), ("binary_sha256", "invalid")):
            bad = copy.deepcopy(report)
            bad[key] = value
            with self.subTest(key=key), self.assertRaises(benchmark_cpu.BenchmarkError):
                bundles.validate_native_reference(bad, "5" * 64, "6" * 64)
        for key in ("math_policy", "activation_precision", "head_precision", "accumulation_precision"):
            bad = copy.deepcopy(report)
            bad["bundles"][0]["ready"].pop(key)
            with self.subTest(ready=key), self.assertRaises(benchmark_cpu.BenchmarkError):
                bundles.validate_native_reference(bad, "5" * 64, "6" * 64)

    def test_receipt_tokens_case_identity_and_output_evidence_are_bound(self):
        report, receipt, fixture, tokens = reference()
        for name in ("receipt", "fixture", "source", "output", "tokens", "case", "metadata", "metrics"):
            bad, bad_receipt = copy.deepcopy(report), copy.deepcopy(receipt)
            receipt_hash, fixture_hash = "3" * 64, "4" * 64
            if name == "receipt":
                receipt_hash = "9" * 64
            elif name == "fixture":
                fixture_hash = "9" * 64
            elif name == "source":
                bad_receipt["source_files"][0]["sha256"] = "9" * 64
            elif name == "output":
                bad_receipt["files"][0]["sha256"] = "9" * 64
            elif name == "tokens":
                bad["bundles"][0]["cases"][0]["input_ids"] = [1, 3, 2]
            elif name == "case":
                bad["bundles"][0]["cases"][0]["case_id"] = "case1"
            elif name == "metadata":
                bad["bundles"][0]["cases"][0]["backend_output"]["entities"][0]["values"][0]["text"] = "forged"
            else:
                bad["bundles"][0]["cases"][0]["source_fp32"]["decisions_equal"] = True
            with self.subTest(name=name), self.assertRaises(benchmark_cpu.BenchmarkError):
                bundles.matching_native_bundle(bad, bad_receipt, receipt_hash, fixture_hash, fixture, tokens)

    def test_record_metadata_is_part_of_backend_parity(self):
        raw = {"entities": [], "classifications": [], "relations": [], "structures": [
            {"name": "record", "instances": [{"confidence": 0.8, "anchor": {"start": 1, "end": 2, "unit": "unicode_codepoints"},
                                                "fields": [{"name": "name", "dtype": "str", "values": []}]}]}]}
        original = bundles.backend_result(raw)
        for key, value in (("confidence", 0.79), ("anchor", None)):
            changed = copy.deepcopy(original)
            changed["structures"][0]["instances"][0][key] = value
            self.assertFalse(bundles.compare_backends(original, changed)["parity_pass"])

    def test_serial_worker_protocol_routes_backend_and_requires_fresh_native_evidence(self):
        fixture_path = benchmark_cpu.case_fixture("small")
        fixture = oracle.read_json(fixture_path)
        token_report = oracle.read_json(oracle.FIXTURES / "token_evidence.json")
        token_rows = next(item for item in token_report["models"] if item["model"] == "small")["validation"]
        commands, workers = [], []
        receipt = {"family": "gliner_boundary_bundle/v1", "version": 1, "backbone": "small", "precision": "q8_0",
                   "source_files": [{"path": name, **pin} for name, pin in fixture["model_files"].items()], "files": []}

        class FakeWorker:
            corruption = None

            def __init__(self, arm, command, env, directory, guard):
                commands.append(command)
                workers.append(self)
                self.closed = False
                self.frames = [ready(receipt, bundles.sha256(fixture_path), arm)]
                self.frames += [{"event": "result", "case_id": case["id"], "input_ids": token_rows[case["id"]]["input_ids"],
                                 "output": lifted(case["expected"])} for case in fixture["cases"]]
                self.frames += [{"event": "complete", "cases": 10, "qualification": False}]
                if self.corruption == "policy":
                    self.frames[0].pop("math_policy")
                elif self.corruption == "tokens":
                    self.frames[1]["input_ids"] = [0]
                elif self.corruption == "backend":
                    self.frames[0]["backend"] = "metal"
                self.buffer = bytearray()
                self.process = type("Process", (), {"poll": lambda _: 0, "returncode": 0, "stdout": io.BytesIO()})()

            def receive(self, _timeout):
                return self.frames.pop(0)

            def close(self):
                self.closed = True

        with tempfile.TemporaryDirectory() as directory, patch.object(benchmark_cpu, "Worker", FakeWorker):
            root = Path(directory)
            model = root / "model"
            model.mkdir()
            (model / "antfly_inference_bundle.json").write_text(json.dumps(receipt))
            native_dir, metal_dir = root / "native", root / "metal"
            native_dir.mkdir()
            metal_dir.mkdir()
            native = bundles.run_one(Path("synthetic-runner"), model, native_dir, token_report, 10, 256)
            self.assertEqual(native["status"], "complete")
            self.assertEqual(native["source_fp32"]["decisions_equal_cases"], 10)
            metal = bundles.run_one(Path("synthetic-runner"), model, metal_dir, token_report, 10, 256,
                                    "metal", {"bundles": [native]})
            self.assertTrue(metal["same_bundle_native"]["parity_pass"])
            self.assertEqual(commands[0][-2:], ["--backend", "native"])
            self.assertEqual(commands[1][-2:], ["--backend", "metal"])
            self.assertTrue(all(worker.closed and not worker.frames for worker in workers))
            with self.assertRaises(benchmark_cpu.BenchmarkError):
                bundles.run_one(Path("synthetic-runner"), model, root, token_report, 10, 256, "metal")
            for corruption in ("policy", "tokens", "backend"):
                failed_dir = root / corruption
                failed_dir.mkdir()
                FakeWorker.corruption = corruption
                with self.subTest(corruption=corruption), self.assertRaises(benchmark_cpu.BenchmarkError):
                    bundles.run_one(Path("synthetic-runner"), model, failed_dir, token_report, 10, 256)
                self.assertTrue(workers[-1].closed)
                saved = oracle.read_json(failed_dir / "small-q8_0" / "report.json")
                self.assertEqual(saved["status"], "incomplete")
                self.assertIs(saved["qualification"], False)


if __name__ == "__main__":
    unittest.main()
