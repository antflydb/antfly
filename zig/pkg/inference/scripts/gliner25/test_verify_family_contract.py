# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations

import hashlib
import base64
import json
import struct
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import benchmark_family_python as benchmark
import verify_family_contract as family


def tensor(dtype: str, shape: list[int], start: int) -> dict[str, object]:
    size = 4
    for dim in shape:
        size *= dim
    return {"dtype": dtype, "shape": shape, "data_offsets": [start, start + size]}


class FamilyContractTest(unittest.TestCase):
    def fixture(self, root: Path, profile: str) -> tuple[Path, dict[str, object]]:
        original = family.strict_json(family.CONTRACT_PATH)
        expected = original["models"][profile]
        model = root / profile
        (model / "encoder_config").mkdir(parents=True)
        config = {
            "architecture": expected["architecture"],
            "architectures": [expected["architecture_class"]],
            "config_version": 3,
            "model_name": expected["model_name"],
            "model_type": "extractor",
            **expected["top_level"],
        }
        (model / "config.json").write_text(json.dumps(config), encoding="utf-8")
        encoder = dict(expected["encoder"])
        if profile == "decide_1b":
            encoder["rope_parameters"] = {
                "full_attention": {"rope_theta": 160000.0, "rope_type": "default"},
                "sliding_attention": {"rope_theta": 160000.0, "rope_type": "default"},
            }
        (model / "encoder_config" / "config.json").write_text(
            json.dumps(encoder), encoding="utf-8"
        )
        (model / "tokenizer_config.json").write_text(
            json.dumps(expected["tokenizer"]), encoding="utf-8"
        )
        (model / "tokenizer.json").write_text(
            json.dumps({"version": "1.0", "profile": profile}), encoding="utf-8"
        )
        header = {
            name: tensor(expected["dtype"], shape, index * 4)
            for index, (name, shape) in enumerate(expected["critical_tensors"].items())
        }
        raw = json.dumps(header, separators=(",", ":")).encode()
        (model / "model.safetensors").write_bytes(struct.pack("<Q", len(raw)) + raw)
        generated = json.loads(json.dumps(original))
        item = generated["models"][profile]
        item["model_size_bytes"] = 8 + len(raw)
        item["tensor_header_length"] = len(raw)
        item["tensor_header_sha256"] = hashlib.sha256(raw).hexdigest()
        item["tensor_count"] = len(header)
        item["parameter_count"] = sum(
            __import__("math").prod(spec["shape"]) for spec in header.values()
        )
        item["sidecars"] = {}
        for relative in (
            "config.json",
            "encoder_config/config.json",
            "tokenizer.json",
            "tokenizer_config.json",
        ):
            path = model / relative
            item["sidecars"][relative] = {
                "size_bytes": path.stat().st_size,
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            }
        contract = root / "contract.json"
        contract.write_text(json.dumps(generated), encoding="utf-8")
        return model, generated

    def test_all_profiles_validate_their_distinct_architecture_contracts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for profile in ("multi_v1", "multi_decide", "decide_1b"):
                with self.subTest(profile=profile):
                    model, generated = self.fixture(root, profile)
                    contract = root / "contract.json"
                    contract.write_text(json.dumps(generated), encoding="utf-8")
                    result = family.verify_model(profile, model, contract_path=contract)
                    self.assertEqual("verified", result["status"])
                    self.assertFalse(result["qualification"])
                    self.assertEqual(
                        profile != "decide_1b",
                        result["compatibility"]["gliner25_boundary_converter"],
                    )

    def test_wrong_profile_and_tensor_schema_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            model, generated = self.fixture(root, "multi_v1")
            contract = root / "contract.json"
            contract.write_text(json.dumps(generated), encoding="utf-8")
            with self.assertRaisesRegex(family.ContractError, "sidecar"):
                family.verify_model("decide_1b", model, contract_path=contract)

            weights = model / "model.safetensors"
            content = weights.read_bytes()
            raw = content[8:].replace(b"1536", b"1537", 1)
            self.assertNotEqual(content[8:], raw)
            weights.write_bytes(struct.pack("<Q", len(raw)) + raw)
            with self.assertRaisesRegex(family.ContractError, "tensor schema differs"):
                family.verify_model("multi_v1", model, contract_path=contract)

    def test_truncated_or_absurd_header_is_rejected_before_allocation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "model.safetensors"
            path.write_bytes(struct.pack("<Q", family.MAX_HEADER_BYTES + 1))
            with self.assertRaisesRegex(
                family.ContractError, "invalid safetensors header"
            ):
                family.safetensors_header(path)

    def test_unselected_rope_metadata_and_tokenizer_bytes_are_identity_bound(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            model, generated = self.fixture(root, "decide_1b")
            contract = root / "contract.json"
            contract.write_text(json.dumps(generated), encoding="utf-8")

            encoder_path = model / "encoder_config" / "config.json"
            encoder = json.loads(encoder_path.read_text(encoding="utf-8"))
            encoder["rope_parameters"]["full_attention"]["rope_theta"] = 160001.0
            encoder_path.write_text(json.dumps(encoder), encoding="utf-8")
            with self.assertRaisesRegex(
                family.ContractError, "sidecar SHA-256 differs"
            ):
                family.verify_model("decide_1b", model, contract_path=contract)

            model, generated = self.fixture(root, "multi_v1")
            contract.write_text(json.dumps(generated), encoding="utf-8")
            tokenizer = model / "tokenizer.json"
            tokenizer.write_bytes(tokenizer.read_bytes() + b" ")
            with self.assertRaisesRegex(family.ContractError, "sidecar size differs"):
                family.verify_model("multi_v1", model, contract_path=contract)

    def test_json_field_comparison_rejects_bool_as_integer(self) -> None:
        with self.assertRaisesRegex(family.ContractError, "encoder.hidden_size"):
            family._require_fields({"hidden_size": True}, {"hidden_size": 1}, "encoder")


class FamilyBenchmarkContractTest(unittest.TestCase):
    def wheel_tree(self, root: Path) -> tuple[Path, Path]:
        code = root / "package.py"
        code.write_bytes(b"trusted\n")
        info = root / "package-1.0.dist-info"
        info.mkdir()
        digest = (
            base64.urlsafe_b64encode(hashlib.sha256(code.read_bytes()).digest())
            .decode()
            .rstrip("=")
        )
        record = info / "RECORD"
        record.write_text(
            f"package.py,sha256={digest},{code.stat().st_size}\n"
            "package-1.0.dist-info/RECORD,,\n",
            encoding="utf-8",
        )
        return code, record

    def test_runtime_tree_rejects_code_tampering_and_unrecorded_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            code, _ = self.wheel_tree(root)
            identity = benchmark.verify_record_tree(root, None)
            self.assertEqual(2, identity["files"])
            benchmark.verify_record_tree(root, identity["tree_sha256"])
            extra = root / "unrecorded.py"
            extra.write_bytes(b"untrusted\n")
            with self.assertRaisesRegex(benchmark.BenchmarkError, "unrecorded"):
                benchmark.verify_record_tree(root, identity["tree_sha256"])
            extra.unlink()
            code.write_bytes(b"changed\n")
            with self.assertRaisesRegex(benchmark.BenchmarkError, "hash differs"):
                benchmark.verify_record_tree(root, identity["tree_sha256"])

    def test_runtime_records_reject_path_escape_and_duplicate_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _, record = self.wheel_tree(root)
            original = record.read_text()
            for extra in (
                original.splitlines()[0] + "\n",
                "../escape.py,sha256=invalid,1\n",
            ):
                record.write_text(original + extra)
                with self.assertRaisesRegex(
                    benchmark.BenchmarkError, "escapes or is duplicated"
                ):
                    benchmark.verify_record_tree(root, None)

    def test_isolated_runtime_rejects_wrong_distribution_inventory(self) -> None:
        contract = benchmark.strict_json(benchmark.RUNTIME_CONTRACT_1B)
        expected = contract["oracle_runtime_decide_1b"]
        with (
            tempfile.TemporaryDirectory() as tmp,
            mock.patch.object(
                benchmark.platform, "python_version", return_value=expected["python"]
            ),
            mock.patch.object(
                benchmark.unicodedata, "unidata_version", expected["unicode"]
            ),
            mock.patch.object(
                benchmark.importlib.metadata, "distributions", return_value=[]
            ),
        ):
            with self.assertRaisesRegex(benchmark.BenchmarkError, "inventory differs"):
                benchmark.verify_runtime_dir(Path(tmp), contract)

    def test_peft_shim_only_supplies_inactive_types(self) -> None:
        names = ("peft", "peft.tuners", "peft.tuners.lora", "peft.tuners.lora.layer")
        saved = {name: sys.modules.get(name) for name in names}
        try:
            for name in names:
                sys.modules.pop(name, None)
            with mock.patch.object(
                benchmark.importlib.util, "find_spec", return_value=None
            ):
                identity = benchmark.install_inference_peft_shim()
            self.assertFalse(identity["executed_peft_code"])
            self.assertFalse(isinstance(object(), sys.modules["peft"].PeftModel))
            self.assertFalse(
                isinstance(object(), sys.modules["peft.tuners.lora.layer"].LoraLayer)
            )
        finally:
            for name in names:
                sys.modules.pop(name, None)
                if saved[name] is not None:
                    sys.modules[name] = saved[name]

    def test_capture_selection_rejects_unqualified_profile_task(self) -> None:
        for profile, task in benchmark.CAPTURES:
            _, capture, rows = benchmark.load_capture(profile, task)
            self.assertFalse(capture["qualification"])
            self.assertTrue(rows)
        with self.assertRaises(benchmark.UnsupportedBenchmark):
            benchmark.load_capture("multi_v1", "decide")

    def test_decide_schema_restores_label_order_and_rejects_missing_label(self) -> None:
        _, _, rows = benchmark.load_capture("multi_decide", "decide")
        row = json.loads(json.dumps(rows[0]))
        for task in row["schema"]["tasks"].values():
            task["labels"] = dict(reversed(list(task["labels"].items())))
        schema = benchmark.ordered_decide_schema(row)
        evidence = row["native_classification"]["tasks"]
        self.assertEqual([task["name"] for task in evidence], list(schema["tasks"]))
        for task in evidence:
            self.assertEqual(
                task["labels"], list(schema["tasks"][task["name"]]["labels"])
            )
        first = next(iter(row["schema"]["tasks"].values()))
        first["labels"].pop(next(iter(first["labels"])))
        with self.assertRaisesRegex(benchmark.BenchmarkError, "inventory differs"):
            benchmark.ordered_decide_schema(row)

    def test_extraction_schema_restores_entity_order(self) -> None:
        _, _, rows = benchmark.load_capture("multi_decide", "extract")
        row = json.loads(json.dumps(rows[0]))
        row["schema"]["entities"] = dict(
            reversed(list(row["schema"]["entities"].items()))
        )
        self.assertEqual(
            row["native_schema"]["entities"],
            list(benchmark.ordered_extract_schema(row)["entities"]),
        )
        row["schema"]["entities"].pop(next(iter(row["schema"]["entities"])))
        with self.assertRaisesRegex(benchmark.BenchmarkError, "inventory differs"):
            benchmark.ordered_extract_schema(row)

    def test_preflight_word_limit_is_preserved_before_collation(self) -> None:
        model = mock.Mock()
        model.processor.word_splitter.return_value = ["word"] * 129
        with self.assertRaisesRegex(benchmark.BenchmarkError, "128 words"):
            benchmark.encoded_evidence(model, "text", {}, boundary=True)
        model.processor.collate_fn_inference.assert_not_called()

    def test_canonical_output_preserves_selected_labels_and_source_offsets(
        self,
    ) -> None:
        schema = {"classifications": [{"name": "intent"}], "entities": ["person"]}
        output = {
            "intent": {
                "value": "refund",
                "probabilities": {"refund": 0.8, "sales": 0.2},
            },
            "entities": {
                "person": [{"text": "María", "confidence": 0.9, "start": 0, "end": 5}]
            },
        }
        expected = benchmark.canonical_expected({}, schema, output)
        self.assertEqual(
            [{"label": "refund", "confidence": 0.8}],
            expected["classifications"][0]["labels"],
        )
        self.assertEqual(
            {"start": 0, "end": 5}, expected["entities"][0]["values"][0]["source"]
        )

    def test_decide_dispatch_uses_decide_preprocessor(self) -> None:
        extract = mock.Mock()
        decide = mock.Mock(return_value="decision")
        prepare = benchmark.select_prepare("decide", extract, decide)
        self.assertEqual("decision", prepare({"id": "choice"}))
        extract.assert_not_called()
        decide.assert_called_once_with({"id": "choice"})


if __name__ == "__main__":
    unittest.main()
