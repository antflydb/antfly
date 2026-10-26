#!/usr/bin/env python3
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

"""Ensure invalid provenance cannot start an installed-model capture."""
import argparse
import copy
import hashlib
import json
from pathlib import Path
import struct
import tempfile
import unittest
from unittest import mock

import attention_matching_calibrate as calibration
import capture_attention_matching as driver
import release_build_fixture


class CaptureGuards(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        (self.root / "zig").mkdir()
        self.source = self.root / "zig/fixture.zig"
        self.source.write_bytes(b"fixture source")
        self.binary = self.root / "context-tests"
        self.binary.write_bytes(b"fixture binary")
        self.artifact = self.root / "model.gguf"
        self.artifact.write_bytes(b"fixture artifact")
        self.document = self.root / "document.txt"
        self.document.write_text("fixture")
        self.receipt = self.root / "build.json"
        release_build_fixture.write(self.root, self.receipt, self.binary)
        self.args = argparse.Namespace(binary=self.binary, build_receipt=self.receipt, artifact=self.artifact,
            model_sha256=calibration.digest(self.artifact), document=self.document, output_dir=self.root / "output", source_tokens=2817)
        self.addCleanup(mock.patch.stopall)
        mock.patch.object(driver, "ROOT", self.root).start()
        mock.patch.dict(driver.os.environ, {}, clear=True).start()
        self.git = mock.patch.object(driver.subprocess, "check_output", side_effect=["a" * 40 + "\n", b"zig/fixture.zig\0"]).start()
        self.run = mock.patch.object(driver, "run_guarded").start()

    def reject(self, message):
        with self.assertRaisesRegex(ValueError, message):
            driver.capture(self.args)
        self.run.assert_not_called()
        self.assertFalse(self.args.output_dir.exists())

    def test_changed_binary_stops_before_gpu_or_output(self):
        self.binary.write_bytes(b"changed binary")
        self.reject("binary file digest mismatch")

    def test_changed_source_archive_stops_before_gpu_or_output(self):
        receipt = calibration.read_json(self.receipt)
        (self.root / receipt["source_archive"]["path"]).write_bytes(b"changed archive")
        self.reject("source archive")

    def test_historical_build_receipt_stops_before_gpu_or_output(self):
        self.receipt.write_text(json.dumps({"binary_sha256": calibration.digest(self.binary),
            "source_verified_after_build": True}) + "\n")
        self.reject("historical, scoped")

    def test_changed_artifact_stops_before_gpu_or_output(self):
        self.artifact.write_bytes(b"changed artifact")
        self.reject("artifact identity mismatch")

    def test_ambiguous_model_directory_stops_before_gpu_or_output(self):
        (self.root / "other.gguf").write_bytes(b"another model")
        self.reject("one unambiguous core GGUF")

    def test_projector_bundle_names_match_loader_classification(self):
        for name in ("mmproj.gguf", "mmproj-model.gguf", "mmproj_model.gguf", "gemma-4-E4B-it-mmproj.gguf", "model_mmproj.gguf"):
            self.assertTrue(driver.is_projector(Path(name)))
        self.assertFalse(driver.is_projector(Path("model.gguf")))
        self.assertFalse(driver.is_projector(Path("model-mmproj-other.gguf")))

    def test_external_tokenizer_stops_before_gpu_or_output(self):
        (self.root / "tokenizer.json").write_text("{}")
        self.reject("external tokenizer")

    def test_replay_bytes_must_match_original_payload_and_provenance(self):
        prefix = self.root / "raw"
        np = calibration.np
        np.asarray([2, 11, 12, 13], dtype="<i8").tofile(str(prefix) + "-prompt.i64")
        np.asarray([100, 11, 12, 13], dtype="<i8").tofile(str(prefix) + "-replay.i64")
        positions = np.asarray([0xffffffff, 1, 2, 3], dtype="<u4")
        positions.tofile(str(prefix) + "-replay-source-positions.u32")
        driver.validate_replay_sequences(prefix, 4, 4)
        np.asarray([100, 11, 12, 99], dtype="<i8").tofile(str(prefix) + "-replay.i64")
        with self.assertRaisesRegex(ValueError, "token identity mismatch"):
            driver.validate_replay_sequences(prefix, 4, 4)

    def test_renderer_capture_inputs_bind_prompt_question_and_study_authority(self):
        np = calibration.np

        def write(name, value):
            path = self.root / name
            path.write_bytes(value)
            return {"path": name, "sha256": calibration.digest(path)}

        prompt_values = np.arange(32, dtype="<i8")
        prompt = write("prompt.i64", prompt_values.tobytes())
        question = write("question.i64", prompt_values[20:24].tobytes())
        coverage = write("coverage.json", b"{}")
        rendered = write("rendered.txt", b"genuine renderer output")
        repeated = struct.pack("<II", 0, len(b"genuine renderer output"))
        offset_bytes = repeated + repeated + b"\0" * (30 * 8)
        offsets = write("prompt-offsets.u32le", offset_bytes)
        contract = write("generation.json", b'{"mode":"greedy_fixed_length_ignore_eos"}')
        fit_template = write("fit.txt", b"fit compiled template")
        gate_template = write("gate.txt", b"gate compiled template")
        fit_tokens = write("fit.i64", np.arange(8, dtype="<i8").tobytes())
        gate_tokens = write("gate.i64", np.arange(8, 16, dtype="<i8").tobytes())
        loaded = {"load_generation": 1, "artifact_sha256": "a" * 64,
                  "tokenizer_sha256": "b" * 64, "template_sha256": "c" * 64,
                  "layout_sha256": "d" * 64, "implementation_sha256": "e" * 64}
        provenance = {"binary_sha256": "f" * 64, "source_build_sha256": "1" * 64}
        request = {"session_id": "2" * 64, "request_index": 0,
                   "question_sha256": "3" * 64, "request_content_sha256": "4" * 64}

        def study(tokens, template):
            return {"tokens": {**tokens, "count": 8}, "prompt_tokens_sha256": prompt["sha256"],
                    "rendered_suffix_sha256": "5" * 64, "template": template,
                    "generation_contract": contract, "response_tokens": 8,
                    "generation_mode": "greedy_fixed_length_ignore_eos"}

        manifest = {"schema_version": 1, "stage": "context_numerical_capture_inputs",
                    "request": request, "coverage_receipt": coverage,
                    "rendered_prompt": rendered,
                    "prompt": {**prompt, "count": 32},
                    "prompt_token_offsets": {**offsets, "count": 32},
                    "question": {"tokens": {**question, "count": 4},
                                 "source_start": 20, "source_end": 24},
                    "fit": study(fit_tokens, fit_template), "gate": study(gate_tokens, gate_template),
                    "loaded_identity": loaded, "provenance": provenance,
                    "completed": True, "diagnostic_only": False,
                    "admitted_prefill_chunk_rows": 64}
        driver.validate_capture_inputs(manifest, self.root, coverage, prompt, offsets, question,
                                       loaded, provenance, request)
        changed = copy.deepcopy(manifest)
        changed["question"]["source_start"] = 19
        with self.assertRaisesRegex(ValueError, "question mismatch"):
            driver.validate_capture_inputs(changed, self.root, coverage, prompt, offsets, question,
                                           loaded, provenance, request)
        changed = copy.deepcopy(manifest)
        changed["gate"]["template"] = fit_template
        with self.assertRaisesRegex(ValueError, "study separation"):
            driver.validate_capture_inputs(changed, self.root, coverage, prompt, offsets, question,
                                           loaded, provenance, request)
        changed = copy.deepcopy(manifest)
        changed["diagnostic_only"] = True
        with self.assertRaisesRegex(ValueError, "invalid numerical capture input"):
            driver.validate_capture_inputs(changed, self.root, coverage, prompt, offsets, question,
                                           loaded, provenance, request)
        changed = copy.deepcopy(manifest)
        changed["admitted_prefill_chunk_rows"] = 128
        with self.assertRaisesRegex(ValueError, "invalid numerical capture input"):
            driver.validate_capture_inputs(changed, self.root, coverage, prompt, offsets, question,
                                           loaded, provenance, request)
        (self.root / rendered["path"]).write_bytes(b"substituted renderer output")
        with self.assertRaisesRegex(ValueError, "rendered prompt identity mismatch"):
            driver.validate_capture_inputs(manifest, self.root, coverage, prompt, offsets, question,
                                           loaded, provenance, request)

    def v2_fixture(self):
        np = calibration.np
        base_dir = self.root / "base"
        base_dir.mkdir()
        np.asarray([0, 0, 0, 0], dtype="<f2").reshape(4, 1, 1).tofile(base_dir / "keys.raw")
        np.asarray([-2, -1, 1, 2], dtype="<f2").reshape(4, 1, 1).tofile(base_dir / "values.raw")
        np.zeros((1, 5, 1), dtype="<f4").tofile(base_dir / "queries.raw")
        np.concatenate([calibration.replay_positions(500, 20, 3, False),
                        calibration.replay_positions(500, 20, 2, True)]).reshape(1, 5).astype("<u4").tofile(base_dir / "positions.raw")
        base = {"schema_version": 1, "purpose": "calibration",
                "model": {"id": "fixture", "sha256": "a" * 64, "tokenizer_sha256": "b" * 64,
                          "template_sha256": "c" * 64, "layout_sha256": "8" * 64,
                          "implementation_sha256": "9" * 64,
                          "global_owner_shapes": [{"layer": 4, "heads": 1, "dim": 1}]},
                "document": {"id": "doc", "sha256": "d" * 64, "bucket": "document_qa"},
                "source": {"git_head": "e" * 40, "files": {"capture.zig": "f" * 64},
                           "binary_sha256": "7" * 64, "build_receipt_sha256": "6" * 64},
                "capture": {"rows": 4, "heads": 1, "dim": 1, "training_rows": 3, "validation_rows": 2,
                            "block_start": 0, "owner_layer": 4, "scale": 1.0, "query_origin": "repeat_prefill",
                            "query_sampling": "uniform_partition_v1", "replay_start": 500, "replay_tokens": 20,
                            "consumers": [{"layer": 4, "query_heads": 1}],
                            "strata": [{"consumer_layer": 4, "query_head": 0, "kv_head": 0,
                                        "training_start": 0, "training_count": 3,
                                        "validation_start": 0, "validation_count": 2}]},
                "tensors": {name: name + ".raw" for name in ("keys", "values", "queries", "positions")}}
        calibration.write_json(base_dir / "spec.json", base)
        definitions = [
            ("request-question", "request_question_replay", "training", "private_model_replay", "completed_prompt_private_replay", 100, 1, False),
            ("fit-study", "fixed_study_prefill", "training", "private_model_replay", "completed_prompt_private_replay", 200, 1, False),
            ("fit-response", "fixed_study_generated_response", "training", "private_model_replay", "completed_prompt_study_generated_response", 220, 1, False),
            ("gate-study", "fixed_study_prefill", "validation", "private_model_replay", "completed_prompt_private_replay", 300, 1, True),
            ("gate-response", "fixed_study_generated_response", "validation", "private_model_replay", "completed_prompt_study_generated_response", 320, 1, True),
        ]
        sources = []
        loaded_identity = {"load_generation": 1, "artifact_sha256": "a" * 64,
                           "tokenizer_sha256": "b" * 64, "template_sha256": "c" * 64,
                           "layout_sha256": "8" * 64, "implementation_sha256": "9" * 64}
        for index, (source_id, kind, partition, route, conditioning, logical, count, held) in enumerate(definitions):
            physical_branch_id = ("fit-pair" if source_id in {"fit-study", "fit-response"} else
                                  "gate-pair" if source_id in {"gate-study", "gate-response"} else source_id)
            branch_index = {"request-question": 0, "fit-pair": 1, "gate-pair": 2}[physical_branch_id]
            branch_start = {"request-question": 100, "fit-pair": 200, "gate-pair": 300}[physical_branch_id]
            branch_count = 20 if physical_branch_id == "request-question" else 40
            branch_rows = 1 if physical_branch_id == "request-question" else 2
            stem = self.root / source_id
            tokens = self.root / (source_id + "-tokens.i64")
            template = self.root / (source_id + "-template.txt")
            template.write_text(source_id + " template")
            (np.arange(20, dtype="<i8") + index * 100).tofile(tokens)
            selected = calibration.replay_positions(logical, 20, count, held)
            count = len(selected)
            positions = selected.reshape(1, count)
            queries = np.full((1, count, 1), index + 1, dtype="<f4")
            positions.astype("<u4").tofile(str(stem) + "-positions.u32")
            queries.tofile(str(stem) + "-queries.f32")
            descriptor = {"source_id": source_id, "kind": kind, "partition": partition,
                          "capture_route": route, "conditioning": conditioning,
                          "logical_start": logical, "logical_count": 20, "owner_layer": 4,
                          "physical_branch_id": physical_branch_id,
                          "branch_logical_start": branch_start, "branch_logical_count": branch_count,
                          "branch_rows_per_stratum": branch_rows,
                          "heads": 1, "dim": 1, "rows_per_head": count, "rows_per_stratum": count,
                          "physical_bytes": 10000 + branch_index * 100, "query_bytes": 1000 + branch_index * 10,
                          "elapsed_ns": 100 + branch_index, "status_readback_bytes": 4, "kv_readback_bytes": 0,
                          "completed": True, "cancelled": False}
            descriptor["tokens_sha256"] = calibration.digest(tokens)
            descriptor["queries_sha256"] = calibration.digest(Path(str(stem) + "-queries.f32"))
            descriptor["positions_sha256"] = calibration.digest(Path(str(stem) + "-positions.u32"))
            descriptor["loaded_identity"] = loaded_identity
            if kind in {"fixed_study_prefill", "fixed_study_generated_response"}:
                descriptor["study_pair_id"] = "fit-pair" if partition == "training" else "gate-pair"
            sources.append({"id": source_id, "kind": kind, "partition": partition,
                            "physical_branch_id": physical_branch_id,
                            "capture_route": route, "conditioning": conditioning,
                            "template": template.name, "template_sha256": calibration.digest(template),
                            "tokens": tokens.name, "owners": [{"layer": 4,
                                "descriptor": Path(str(stem) + "-source.json").name,
                                "queries": Path(str(stem) + "-queries.f32").name,
                                "positions": Path(str(stem) + "-positions.u32").name}]})
            if kind in {"fixed_study_prefill", "fixed_study_generated_response"}:
                sources[-1]["study_pair_id"] = descriptor["study_pair_id"]
            if kind == "fixed_study_generated_response":
                contract = self.root / (source_id + "-generation.json")
                contract.write_text('{"mode":"greedy","max_tokens":20}')
                descriptor["generation_contract_sha256"] = calibration.digest(contract)
                descriptor["generation_mode"] = "greedy_fixed_length_ignore_eos"
                sources[-1]["generation_contract"] = contract.name
                sources[-1]["generation_contract_sha256"] = descriptor["generation_contract_sha256"]
                sources[-1]["generation_mode"] = descriptor["generation_mode"]
            descriptor_path = Path(str(stem) + "-source.json")
            calibration.write_json(descriptor_path, descriptor)
            sources[-1]["tokens_sha256"] = descriptor["tokens_sha256"]
            sources[-1]["owners"][0].update({
                "descriptor_sha256": calibration.digest(descriptor_path),
                "queries_sha256": descriptor["queries_sha256"],
                "positions_sha256": descriptor["positions_sha256"],
            })
        prompt_tokens = self.root / "prompt-tokens.i64"
        np.arange(1280, dtype="<i8").tofile(prompt_tokens)
        coverage_path = self.root / "block-coverage.json"
        coverage = {"schema_version": 2, "stage": "context_payload_block_coverage",
                    "request": {"session_id": "1" * 64, "request_index": 0,
                                "question_sha256": "2" * 64,
                                "request_content_sha256": "3" * 64},
                    "document": base["document"], "qualified_context_tokens": 1284,
                    "question": {"message_index": 0, "part_index": 1, "source_start": 1100,
                                 "source_end": 1104, "token_count": 4,
                                 "tokens_sha256": hashlib.sha256(
                                     np.arange(1280, dtype="<i8").tobytes()[1100 * 8:1104 * 8]).hexdigest()},
                    "prompt_tokens": 1280, "reserved_output_tokens": 4,
                    "admitted_prefill_chunk_rows": 64,
                    "prompt_tokens_sha256": calibration.digest(prompt_tokens),
                    "prompt_token_offsets_sha256": "8" * 64,
                    "rendered_prompt_sha256": "4" * 64,
                    "policy": {"keep_prefix_tokens": 0, "keep_recent_tokens": 256,
                               "block_tokens": 512,
                               "compaction_targets": [{"message_index": 0, "part_index": 0}]},
                    "payload_parts": [{"message_index": 0, "part_index": 0, "label": 1,
                                       "token_start": 0, "token_end": 1024}],
                    "eligible_token_labels": [1] * 512 + [0] * 768,
                    "eligible_ranges": [{"message_index": 0, "part_index": 0, "label": 1,
                                         "start": 0, "end": 512}],
                    "eligible_blocks": [{"message_index": 0, "part_index": 0, "label": 1,
                                         "start": 0, "end": 512}],
                    "loaded_identity": loaded_identity,
                    "provenance": {"binary_sha256": "7" * 64, "source_build_sha256": "6" * 64},
                    "completed": True, "cancelled": False, "elapsed_ns": 1,
                    "status_readback_bytes": 4, "kv_readback_bytes": 0}
        calibration.write_json(coverage_path, coverage)
        plan = {"schema_version": 2, "purpose": "calibration", "global_owners": [4],
                "coverage_receipts": [{"receipt": coverage_path.name,
                                       "sha256": calibration.digest(coverage_path),
                                       "prompt_tokens": prompt_tokens.name,
                                       "prompt_tokens_sha256": calibration.digest(prompt_tokens)}],
                "base_specs": ["base/spec.json"], "sources": sources}
        calibration.write_json(self.root / "plan.json", plan)
        return self.root / "plan.json"

    def test_schema2_assembler_reorders_sources_and_seals_raw_evidence(self):
        receipt = driver.assemble_v2(self.v2_fixture(), self.root / "assembled")
        self.assertFalse(receipt["serving_qualified"])
        manifest = self.root / "assembled" / receipt["captures"][0]
        meta, arrays = calibration.load_capture(manifest)
        self.assertEqual(meta["schema_version"], 2)
        self.assertEqual(meta["capture"]["training_rows"], 3)
        self.assertEqual(meta["capture"]["validation_rows"], 2)
        self.assertEqual(arrays["queries"][:, :, 0].tolist(), [[1, 2, 3, 4, 5]])
        self.assertEqual([x["id"] for x in meta["capture"]["query_sources"]],
                         ["request-question", "fit-study", "fit-response", "gate-study", "gate-response"])
        self.assertEqual([x["source_ids"] for x in meta["capture"]["query_campaign"]["branch_runs"]],
                         [["request-question"], ["fit-study", "fit-response"],
                          ["gate-study", "gate-response"]])
        self.assertTrue(meta["capture"]["runtime_query_pool"]["runtime_supported"])
        self.assertEqual(meta["capture"]["runtime_block_coverage"], {
            "receipt_sha256": calibration.digest(self.root / "block-coverage.json"),
            "prompt_tokens_sha256": calibration.digest(self.root / "prompt-tokens.i64"),
            "prompt_token_offsets_sha256": "8" * 64,
            "rendered_prompt_sha256": "4" * 64,
            "admitted_prefill_chunk_rows": 64,
            "qualified_context_tokens": 1284,
            "prompt_tokens": 1280,
            "reserved_output_tokens": 4,
        })

    def test_schema2_explicit_legacy_diagnostic_cannot_be_calibration_evidence(self):
        plan_path = self.v2_fixture()
        plan = calibration.read_json(plan_path)
        plan.pop("coverage_receipts")
        plan["diagnostic_without_block_coverage"] = True
        for source in plan["sources"]:
            source.pop("physical_branch_id")
            for owner in source["owners"]:
                descriptor_path = plan_path.parent / owner["descriptor"]
                descriptor = calibration.read_json(descriptor_path)
                for field in ("physical_branch_id", "branch_logical_start", "branch_logical_count",
                              "branch_rows_per_stratum"):
                    descriptor.pop(field)
                descriptor_path.write_text(json.dumps(descriptor))
                positions_path = plan_path.parent / owner["positions"]
                positions = calibration.replay_positions(
                    descriptor["logical_start"], descriptor["logical_count"],
                    descriptor["rows_per_stratum"], descriptor["partition"] != "training")
                positions.reshape(descriptor["heads"], descriptor["rows_per_head"]).astype("<u4").tofile(positions_path)
        plan_path.write_text(json.dumps(plan))
        receipt = driver.assemble_v2(plan_path, self.root / "diagnostic")
        self.assertTrue(receipt["diagnostic_only"])
        self.assertEqual(receipt["missing_gates"], ["runtime_block_coverage"])
        manifest = self.root / "diagnostic" / receipt["captures"][0]
        meta = calibration.read_json(manifest)
        self.assertEqual(meta["purpose"], "diagnostic")
        self.assertEqual(meta["capture"]["runtime_block_coverage"]["status"], "diagnostic_unavailable")
        with self.assertRaisesRegex(ValueError, "only calibration"):
            calibration.freeze(calibration.run_captures([manifest], (2,), (calibration.RECIPES[0],)), "f" * 64)

    def test_schema2_owner_sampling_quotas_are_independent_but_branch_complete(self):
        plan_path = self.v2_fixture()
        plan = calibration.read_json(plan_path)
        base_path = self.root / "base/spec.json"
        base = calibration.read_json(base_path)
        base["model"]["global_owner_shapes"].append({"layer": 9, "heads": 1, "dim": 1})
        base_path.write_text(json.dumps(base))
        second = copy.deepcopy(base)
        second["capture"]["owner_layer"] = 9
        second["capture"]["consumers"][0]["layer"] = 9
        second["capture"]["strata"][0]["consumer_layer"] = 9
        calibration.write_json(self.root / "base/second.json", second)
        plan["base_specs"].append("base/second.json")
        plan["global_owners"].append(9)
        for source in plan["sources"]:
            original = source["owners"][0]
            descriptor = calibration.read_json(self.root / original["descriptor"])
            descriptor.update(owner_layer=9, rows_per_head=2, rows_per_stratum=2)
            descriptor["branch_rows_per_stratum"] *= 2
            stem = source["id"] + "-owner9"
            cell = {"layer": 9, "descriptor": stem + ".json",
                    "queries": stem + ".f32", "positions": stem + ".u32"}
            calibration.np.ones((1, 2, 1), dtype="<f4").tofile(self.root / cell["queries"])
            calibration.replay_positions(descriptor["logical_start"], descriptor["logical_count"],
                                         2, descriptor["partition"] != "training").astype("<u4").tofile(
                                             self.root / cell["positions"])
            descriptor["queries_sha256"] = calibration.digest(self.root / cell["queries"])
            descriptor["positions_sha256"] = calibration.digest(self.root / cell["positions"])
            calibration.write_json(self.root / cell["descriptor"], descriptor)
            cell.update({"descriptor_sha256": calibration.digest(self.root / cell["descriptor"]),
                         "queries_sha256": descriptor["queries_sha256"],
                         "positions_sha256": descriptor["positions_sha256"]})
            source["owners"].append(cell)
        plan_path.write_text(json.dumps(plan))
        receipt = driver.assemble_v2(plan_path, self.root / "different-owner-quotas")
        captures = [calibration.read_json(self.root / "different-owner-quotas" / name)
                    for name in receipt["captures"]]
        self.assertEqual([meta["capture"]["training_rows"] for meta in captures], [3, 6])
        self.assertEqual([meta["capture"]["validation_rows"] for meta in captures], [2, 4])
        # A wrong row count within one physical branch still cannot be sealed.
        path = self.root / plan["sources"][1]["owners"][1]["descriptor"]
        descriptor = calibration.read_json(path)
        descriptor["branch_rows_per_stratum"] += 1
        path.write_text(json.dumps(descriptor))
        plan["sources"][1]["owners"][1]["descriptor_sha256"] = calibration.digest(path)
        plan_path.write_text(json.dumps(plan))
        with self.assertRaisesRegex(ValueError, "physical query branch geometry disagrees"):
            driver.assemble_v2(plan_path, self.root / "inconsistent-owner-branch")

    def test_runtime_coverage_binds_parent_session_and_monotonic_request(self):
        self.v2_fixture()
        receipt = calibration.read_json(self.root / "block-coverage.json")
        expected = {"session_id": "1" * 64, "request_index": 0,
                    "question_sha256": "2" * 64,
                    "request_content_sha256": "3" * 64}
        driver.validate_block_coverage(receipt, request_binding=expected)
        for replacement in ({**expected, "session_id": "2" * 64},
                            {**expected, "request_index": 1},
                            {**expected, "question_sha256": "4" * 64},
                            {**expected, "request_content_sha256": "5" * 64}):
            with self.subTest(replacement=replacement), self.assertRaisesRegex(
                    ValueError, "request binding mismatch"):
                driver.validate_block_coverage(receipt, request_binding=replacement)
        for replacement in (None, {"session_id": "0" * 64, "request_index": 0},
                            {"session_id": "1" * 64, "request_index": True},
                            {"session_id": "1" * 64, "request_index": 2**32}):
            changed = copy.deepcopy(receipt)
            changed["request"] = replacement
            with self.subTest(replacement=replacement), self.assertRaisesRegex(
                    ValueError, "invalid block coverage request binding"):
                driver.validate_block_coverage(changed)

    def test_schema2_assembler_rejects_missing_owner_and_forged_positions(self):
        plan_path = self.v2_fixture()
        plan = calibration.read_json(plan_path)
        plan["sources"][0]["owners"] = []
        plan_path.write_text(json.dumps(plan))
        with self.assertRaisesRegex(ValueError, "missing query source owners"):
            driver.assemble_v2(plan_path, self.root / "missing")
        plan_path = self.v2_fixture_from_new_root()
        with self.assertRaisesRegex(ValueError, "position"):
            driver.assemble_v2(plan_path, self.root / "forged")

    def test_schema2_assembler_binds_descriptor_query_position_and_token_artifacts(self):
        mutations = (
            ("descriptor", "source descriptor artifact identity mismatch"),
            ("queries", "source query artifact identity mismatch"),
            ("positions", "source position artifact identity mismatch"),
            ("tokens", "query source token artifact identity mismatch"),
        )
        for target, message in mutations:
            with self.subTest(target=target):
                old = self.root
                self.root = old / target
                self.root.mkdir()
                try:
                    plan_path = self.v2_fixture()
                finally:
                    self.root = old
                plan = calibration.read_json(plan_path)
                source = plan["sources"][0]
                path = plan_path.parent / (source["owners"][0][target] if target != "tokens" else source["tokens"])
                path.write_bytes(path.read_bytes() + b"\x00")
                with self.assertRaisesRegex(ValueError, message):
                    driver.assemble_v2(plan_path, self.root / (target + "-tampered"))

    def test_schema2_assembler_rejects_unbound_generation_mode(self):
        plan_path = self.v2_fixture()
        plan = calibration.read_json(plan_path)
        response = next(source for source in plan["sources"] if source["kind"] == "fixed_study_generated_response")
        response["generation_mode"] = "greedy_until_eos"
        plan_path.write_text(json.dumps(plan))
        with self.assertRaisesRegex(ValueError, "generation mode"):
            driver.assemble_v2(plan_path, self.root / "wrong-mode")

    def test_schema2_assembler_rejects_incomplete_planned_blocks(self):
        plan_path = self.v2_fixture()
        plan = calibration.read_json(plan_path)
        coverage_path = plan_path.parent / plan["coverage_receipts"][0]["receipt"]
        coverage = calibration.read_json(coverage_path)
        coverage["eligible_ranges"][0]["end"] = 1024
        coverage["eligible_blocks"].append({"message_index": 0, "part_index": 0, "label": 1,
                                             "start": 512, "end": 1024})
        coverage_path.write_text(json.dumps(coverage))
        plan["coverage_receipts"][0]["sha256"] = calibration.digest(coverage_path)
        plan_path.write_text(json.dumps(plan))
        with self.assertRaisesRegex(ValueError, "runtime (?:eligible ranges|block coverage)"):
            driver.assemble_v2(plan_path, self.root / "missing-block")

    def test_schema2_assembler_rejects_extra_and_duplicate_owner_blocks(self):
        plan_path = self.v2_fixture()
        plan = calibration.read_json(plan_path)
        spec_path = plan_path.parent / plan["base_specs"][0]
        extra = calibration.read_json(spec_path)
        extra["capture"]["block_start"] = 512
        extra_path = spec_path.parent / "extra-spec.json"
        calibration.write_json(extra_path, extra)
        plan["base_specs"].append(str(extra_path.relative_to(plan_path.parent)))
        plan_path.write_text(json.dumps(plan))
        with self.assertRaisesRegex(ValueError, "runtime block coverage"):
            driver.assemble_v2(plan_path, self.root / "extra-block")

        old = self.root
        self.root = old / "duplicate-fixture"
        self.root.mkdir()
        try:
            plan_path = self.v2_fixture()
        finally:
            self.root = old
        plan = calibration.read_json(plan_path)
        plan["base_specs"].append(plan["base_specs"][0])
        plan_path.write_text(json.dumps(plan))
        with self.assertRaisesRegex(ValueError, "duplicate base owner block"):
            driver.assemble_v2(plan_path, self.root / "duplicate-block")

    def test_runtime_block_receipt_rejects_missing_and_extra_declared_blocks(self):
        plan_path = self.v2_fixture()
        plan = calibration.read_json(plan_path)
        receipt = calibration.read_json(plan_path.parent / plan["coverage_receipts"][0]["receipt"])
        for mutate in (
            lambda value: value["eligible_blocks"].clear(),
            lambda value: value["eligible_blocks"].append(
                {"message_index": 0, "part_index": 0, "label": 1, "start": 512, "end": 1024}),
        ):
            changed = copy.deepcopy(receipt)
            mutate(changed)
            with self.subTest(blocks=changed["eligible_blocks"]), self.assertRaisesRegex(
                    ValueError, "incomplete or extra runtime eligible block coverage"):
                driver.validate_block_coverage(changed)

    def test_runtime_block_receipt_recomputes_ranges_from_complete_token_labels(self):
        plan_path = self.v2_fixture()
        plan = calibration.read_json(plan_path)
        receipt = calibration.read_json(plan_path.parent / plan["coverage_receipts"][0]["receipt"])
        changed = copy.deepcopy(receipt)
        changed["eligible_token_labels"][255] = 0
        with self.assertRaisesRegex(ValueError, "eligible ranges"):
            driver.validate_block_coverage(changed)
        changed = copy.deepcopy(receipt)
        changed["eligible_token_labels"][-1] = 1
        with self.assertRaisesRegex(ValueError, "unprotected prefix or recent token"):
            driver.validate_block_coverage(changed)

    def test_runtime_block_receipt_binds_chunk_and_rendered_prompt(self):
        plan_path = self.v2_fixture()
        plan = calibration.read_json(plan_path)
        receipt = calibration.read_json(plan_path.parent / plan["coverage_receipts"][0]["receipt"])
        for field, value, message in (
            ("admitted_prefill_chunk_rows", 128, "admitted prefill chunk"),
            ("rendered_prompt_sha256", "not-a-digest", "rendered prompt"),
        ):
            changed = copy.deepcopy(receipt)
            changed[field] = value
            with self.subTest(field=field), self.assertRaisesRegex(ValueError, message):
                driver.validate_block_coverage(changed)

    def campaign_fixture(self):
        np = calibration.np
        root = self.root

        def write(name, data):
            path = root / name
            path.write_bytes(data)
            return {"path": name, "sha256": calibration.digest(path)}

        loaded = {"load_generation": 9, "artifact_sha256": "a" * 64,
                  "tokenizer_sha256": "b" * 64, "template_sha256": "c" * 64,
                  "layout_sha256": "d" * 64, "implementation_sha256": "e" * 64}
        source_receipt_path = self.receipt
        source_receipt_value = calibration.read_json(source_receipt_path)
        source_receipt = {"path": source_receipt_path.name, "sha256": calibration.digest(source_receipt_path)}
        provenance = {"binary_sha256": source_receipt_value["binary"]["sha256"],
                      "source_build_sha256": source_receipt["sha256"]}
        prompt_values = np.arange(1024, dtype="<i8")
        prompt = write("prompt.i64", prompt_values.tobytes())
        question = write("question.i64", prompt_values[900:904].tobytes())
        rendered = write("rendered.txt", b"renderer-owned structured request")
        offsets = write("prompt-offsets.u32le",
                        struct.pack("<II", 0, len(b"renderer-owned structured request")) + b"\0" * (1023 * 8))
        fit_template = write("fit.txt", b"compiled fit template")
        gate_template = write("gate.txt", b"compiled gate template")
        contract = write("generation.json", b'{"mode":"greedy_fixed_length_ignore_eos"}')
        fit = write("fit.i64", np.arange(20, 28, dtype="<i8").tobytes())
        gate = write("gate.i64", np.arange(30, 38, dtype="<i8").tobytes())
        request = {"session_id": "3" * 64, "request_index": 0,
                   "question_sha256": "4" * 64, "request_content_sha256": "5" * 64}
        document = {"id": "doc", "sha256": "6" * 64, "bucket": "document_qa"}
        coverage = {"schema_version": 2, "stage": "context_payload_block_coverage",
            "request": request, "document": document,
            "question": {"message_index": 0, "part_index": 1, "source_start": 900,
                         "source_end": 904, "token_count": 4, "tokens_sha256": question["sha256"]},
            "qualified_context_tokens": 1040, "prompt_tokens": 1024, "reserved_output_tokens": 16,
            "admitted_prefill_chunk_rows": 64, "prompt_tokens_sha256": prompt["sha256"],
            "prompt_token_offsets_sha256": offsets["sha256"],
            "rendered_prompt_sha256": rendered["sha256"],
            "policy": {"keep_prefix_tokens": 0, "keep_recent_tokens": 512, "block_tokens": 512,
                       "compaction_targets": [{"message_index": 0, "part_index": 0}]},
            "payload_parts": [{"message_index": 0, "part_index": 0, "label": 1,
                               "token_start": 0, "token_end": 512}],
            "eligible_token_labels": [1] * 512 + [0] * 512,
            "eligible_ranges": [{"message_index": 0, "part_index": 0, "label": 1,
                                 "start": 0, "end": 512}],
            "eligible_blocks": [{"message_index": 0, "part_index": 0, "label": 1,
                                 "start": 0, "end": 512}],
            "loaded_identity": loaded, "provenance": provenance, "completed": True,
            "cancelled": False, "elapsed_ns": 1, "status_readback_bytes": 4, "kv_readback_bytes": 0}
        coverage_path = root / "coverage.json"; calibration.write_json(coverage_path, coverage)
        coverage_ref = {"path": coverage_path.name, "sha256": calibration.digest(coverage_path)}

        def study(tokens, template):
            return {"tokens": {**tokens, "count": 8}, "prompt_tokens_sha256": prompt["sha256"],
                    "rendered_suffix_sha256": "7" * 64, "template": template,
                    "generation_contract": contract, "response_tokens": 8,
                    "generation_mode": "greedy_fixed_length_ignore_eos"}

        manifest = {"schema_version": 1, "stage": "context_numerical_capture_inputs",
            "request": request, "coverage_receipt": coverage_ref, "rendered_prompt": rendered,
            "prompt": {**prompt, "count": 1024},
            "prompt_token_offsets": {**offsets, "count": 1024},
            "question": {"tokens": {**question, "count": 4}, "source_start": 900, "source_end": 904},
            "fit": study(fit, fit_template), "gate": study(gate, gate_template),
            "loaded_identity": loaded, "provenance": provenance, "completed": True,
            "diagnostic_only": False, "admitted_prefill_chunk_rows": 64}
        manifest_path = root / "capture-inputs.json"; calibration.write_json(manifest_path, manifest)
        manifest_ref = {"path": manifest_path.name, "sha256": calibration.digest(manifest_path)}

        source_rows = (
            ("request-question-replay", "request_question_replay", "training", "question",
             "completed_prompt_private_replay", question, 1024, 4, 1024, 4, None),
            ("fit-study-prefill", "fixed_study_prefill", "training", "fit-study",
             "completed_prompt_private_replay", fit, 1024, 8, 1024, 16, None),
            ("fit-study-response", "fixed_study_generated_response", "training", "fit-study",
             "completed_prompt_study_generated_response", write("fit-response.i64", np.arange(40, 48, dtype="<i8").tobytes()), 1032, 8, 1024, 16, contract),
            ("gate-study-prefill", "fixed_study_prefill", "validation", "gate-study",
             "completed_prompt_private_replay", gate, 1024, 8, 1024, 16, None),
            ("gate-study-response", "fixed_study_generated_response", "validation", "gate-study",
             "completed_prompt_study_generated_response", write("gate-response.i64", np.arange(50, 58, dtype="<i8").tobytes()), 1032, 8, 1024, 16, contract),
        )
        artifacts = []
        for source_id, kind, partition, branch, conditioning, tokens, start, count, branch_start, branch_count, generation in source_rows:
            token_name = source_id + "-tokens.i64"
            (root / token_name).write_bytes((root / tokens["path"]).read_bytes())
            token_ref = {"path": token_name, "sha256": calibration.digest(root / token_name)}
            artifacts.append({"kind": "source_tokens", "source_id": source_id, "owner_layer": None, **token_ref})
            query_name, position_name = source_id + "-layer4-queries.f32", source_id + "-layer4-positions.u32"
            np.asarray([1], dtype="<f4").tofile(root / query_name)
            calibration.replay_positions(start, count, 1, partition != "training").astype("<u4").tofile(root / position_name)
            descriptor = {"source_id": source_id, "kind": kind, "partition": partition,
                "capture_route": "private_model_replay", "campaign_route": "production_query_campaign",
                "conditioning": conditioning, "physical_branch_id": branch,
                "study_pair_id": branch if "study" in branch else None,
                "generation_contract_sha256": generation["sha256"] if generation else None,
                "generation_mode": "greedy_fixed_length_ignore_eos" if generation else None,
                "prefix_storage": "packed_exact", "private_kernel_route": "fixed_serial_private_study",
                "loaded_identity": loaded, "tokens_sha256": token_ref["sha256"],
                "queries_sha256": calibration.digest(root / query_name),
                "positions_sha256": calibration.digest(root / position_name),
                "logical_start": start, "logical_count": count, "branch_logical_start": branch_start,
                "branch_logical_count": branch_count, "branch_rows_per_stratum": 1 if branch == "question" else 2,
                "owner_layer": 4, "heads": 1, "dim": 1, "rows_per_head": 1,
                "rows_per_stratum": 1, "physical_bytes": 4096, "peak_bytes": 4096,
                "query_bytes": 4, "elapsed_ns": 1, "status_readback_bytes": 4,
                "replay_split_scratch_bytes": 0,
                "serial_attention_dispatches": {"question": 1, "fit-study": 2, "gate-study": 3}[branch],
                "split_attention_dispatches": 0,
                "kv_readback_bytes": 0, "completed": True, "cancelled": False}
            descriptor_name = source_id + "-layer4-source.json"
            calibration.write_json(root / descriptor_name, descriptor)
            for artifact_kind, artifact_path in (("source_descriptor", descriptor_name),
                                                  ("source_queries", query_name),
                                                  ("source_positions", position_name)):
                artifacts.append({"kind": artifact_kind, "source_id": source_id, "owner_layer": 4,
                                  "path": artifact_path, "sha256": calibration.digest(root / artifact_path)})
        keys = write("base-keys.f16", np.zeros((512, 1, 1), dtype="<f2").tobytes())
        values = write("base-values.f16", np.ones((512, 1, 1), dtype="<f2").tobytes())
        base_descriptor = {"schema_version": 1, "stage": "context_packed_exact_block_capture",
            "completed": True, "prefix_storage": "packed_exact", "prefix_kernel_route": "metal_context_flash_prefill_abi2",
            "owner_layer": 4, "block_start": 0, "rows": 512, "heads": 1, "dim": 1, "scale": 1.0,
            "consumers": [{"layer": 4, "query_heads": 1}],
            "strata": [{"consumer_layer": 4, "query_head": 0, "kv_head": 0}],
            "tensor_order": "logical_row_kv_head_head_dim", "tensor_dtype": "f16_le",
            "keys": keys, "values": values, "prompt": {"tokens": 1024, "sha256": prompt["sha256"]},
            "document": document, "loaded_identity": loaded, "provenance": provenance,
            "kv_export_readback_bytes": 2048}
        base_descriptor_path = root / "base.json"; calibration.write_json(base_descriptor_path, base_descriptor)
        base_index = {"schema_version": 1, "stage": "context_packed_exact_block_index", "completed": True,
            "prompt": base_descriptor["prompt"], "document": document, "eligible_block_starts": [0],
            "descriptors": [{"owner_layer": 4, "block_start": 0, "path": base_descriptor_path.name,
                             "sha256": calibration.digest(base_descriptor_path)}]}
        base_index_path = root / "base-index.json"; calibration.write_json(base_index_path, base_index)
        base_index_ref = {"path": base_index_path.name, "sha256": calibration.digest(base_index_path)}
        artifacts.append({"kind": "packed_base_index", "source_id": None, "owner_layer": None, **base_index_ref})
        artifact_index = {"schema_version": 1, "stage": "context_query_campaign_artifact_index",
                          "completed": True, "artifacts": artifacts}
        artifact_index_path = root / "artifact-index.json"; calibration.write_json(artifact_index_path, artifact_index)
        artifact_index_ref = {"path": artifact_index_path.name, "sha256": calibration.digest(artifact_index_path)}
        summary = {"schema_version": 2, "stage": "context_query_campaign_capture", "completed": True,
            "publication_requested": False, "branch_order": ["question", "fit", "gate"],
            "prefix_storage": "packed_exact", "prefix_kernel_route": "metal_context_flash_prefill_abi2",
            "admitted_prefill_chunk_rows": 64, "prefix_flash_prefill_dispatches": 1,
            "private_kernel_route": "fixed_serial_private_replay",
            "private_replay_attention": {"serial_dispatches": 6, "split_dispatches": 0,
                "split_scratch_bytes": 0, "split_max_query_rows": None, "split_count": None},
            "fit_template_sha256": fit_template["sha256"],
            "gate_template_sha256": gate_template["sha256"], "generation_contract_sha256": contract["sha256"],
            "capture_inputs_manifest_sha256": manifest_ref["sha256"], "coverage_receipt_sha256": coverage_ref["sha256"],
            "request_content_sha256": request["request_content_sha256"], "prompt_tokens": 1024,
            "prompt_tokens_sha256": prompt["sha256"], "question": {"source_start": 900, "source_end": 904,
                "token_count": 4, "tokens_sha256": question["sha256"]}, "document": document,
            "eligible_block_starts": [0], "load_generation": 9, "live_epoch_before": 1,
            "live_epoch_after": 1, "live_bytes_before": 4096, "live_bytes_after": 4096,
            "query_export_bytes": 20, "campaign_query_bytes": 20, "campaign_backend_bytes": 4096,
            "campaign_host_bytes": 4096, "campaign_physical_bytes": 8192,
            "observer_output_admitted_bytes": 4096, "base_index": base_index_ref,
            "artifact_index": artifact_index_ref, "kv_export_readback_bytes": 2048,
            "loaded_identity": loaded, "provenance": provenance}
        summary_path = root / "campaign.json"; calibration.write_json(summary_path, summary)
        return ({"path": summary_path.name, "sha256": calibration.digest(summary_path)}, manifest_ref,
                source_receipt)

    def rewrite_campaign_attention_receipts(self, summary, *, legacy=False, split=False):
        index_path = self.root / "artifact-index.json"
        index = calibration.read_json(index_path)
        branch_counts = {"question": 4, "fit-study": 8, "gate-study": 12}
        for row in index["artifacts"]:
            if row["kind"] != "source_descriptor":
                continue
            path = self.root / row["path"]
            descriptor = calibration.read_json(path)
            fields = ("replay_split_scratch_bytes", "serial_attention_dispatches",
                      "split_attention_dispatches")
            if legacy:
                for field in fields:
                    descriptor.pop(field, None)
            elif split:
                descriptor["replay_split_scratch_bytes"] = 2113536
                descriptor["serial_attention_dispatches"] = 2 * branch_counts[descriptor["physical_branch_id"]]
                descriptor["split_attention_dispatches"] = branch_counts[descriptor["physical_branch_id"]]
            path.write_text(json.dumps(descriptor))
            row["sha256"] = calibration.digest(path)
        index_path.write_text(json.dumps(index))
        summary_path = self.root / summary["path"]
        record = calibration.read_json(summary_path)
        record["artifact_index"]["sha256"] = calibration.digest(index_path)
        if legacy:
            record["schema_version"] = 1
            record.pop("private_replay_attention", None)
        elif split:
            record["private_kernel_route"] = "bounded_split_global_replay_v1"
            record["private_replay_attention"] = {"serial_dispatches": 48, "split_dispatches": 24,
                "split_scratch_bytes": 2113536, "split_max_query_rows": 8, "split_count": 16}
        summary_path.write_text(json.dumps(record))
        summary["sha256"] = calibration.digest(summary_path)

    @staticmethod
    def provider_delta():
        return {name: 0 for name in (
            "frame_begins", "frame_submits", "frame_wait_ns", "frame_gpu_ns",
            "compute_encoders", "blit_encoders", "precomputed_tail_blocks",
            "precomputed_tail_failures", "paged_attention_1x_calls", "decode_gqa_split_calls",
            "generated_attention_decode_1x_calls", "generated_attention_flash_prefill_calls",
            "attention_prefill_direct_kv_calls", "attention_prefill_paged_kv_calls",
            "to_host_calls", "to_host_device_calls", "host_download_bytes")}

    def upgrade_campaign_schema6(self, summary):
        summary_path = self.root / summary["path"]
        record = calibration.read_json(summary_path)
        provider = self.provider_delta()
        provider.update(frame_begins=1, frame_submits=1, compute_encoders=1)
        timing = {"prefill_hidden_ns": 1, "initial_logits_ns": 1, "generated_loop_ns": 1}
        record.update(schema_version=6,
            private_study_observation={branch: {
                "physical_branch_id": branch + "-study", "timing": timing,
                "provider_delta": dict(provider), "singleton_paged_decode_forwards": 0,
                "singleton_paged_decode_expected_layer_dispatches": 0}
                for branch in ("fit", "gate")},
            study_singleton_paged_decode_requested=False,
            private_question_observation={"physical_branch_id": "question", "elapsed_ns": 7,
                                          "provider_delta": dict(provider)},
            live_session_identity_before=77, live_session_identity_after=77,
            live_fingerprint_publication_epoch_before=record["live_epoch_before"],
            live_fingerprint_publication_epoch_after=record["live_epoch_after"],
            live_fingerprint_logical_tokens_before=record["prompt_tokens"],
            live_fingerprint_logical_tokens_after=record["prompt_tokens"],
            live_payload_sha256_before="a" * 64, live_payload_sha256_after="a" * 64,
            live_descriptor_sha256_before="b" * 64, live_descriptor_sha256_after="b" * 64,
            live_fingerprint_diagnostic_readback_bytes_before=4096,
            live_fingerprint_diagnostic_readback_bytes_after=4096)
        summary_path.write_text(json.dumps(record))
        summary["sha256"] = calibration.digest(summary_path)
        return record

    def test_campaign_plan_is_derived_from_indexed_same_generation_artifacts(self):
        summary, capture_inputs, source = self.campaign_fixture()
        plan = driver.prepare_campaign_plan(self.root, summary, capture_inputs, source,
                                            "fixture", [{"layer": 4, "heads": 1, "dim": 1}],
                                            self.root / "campaign-plan.json")
        self.assertEqual(plan["global_owners"], [4])
        self.assertEqual(len(plan["base_specs"]), 1)
        self.assertEqual([row["id"] for row in plan["sources"]],
                         ["request-question-replay", "fit-study-prefill", "fit-study-response",
                          "gate-study-prefill", "gate-study-response"])
        self.assertEqual(plan["global_owner_shapes"], [{"layer": 4, "heads": 1, "dim": 1}])
        with self.assertRaisesRegex(ValueError, "predeclared layout"):
            driver.prepare_campaign_plan(self.root, summary, capture_inputs, source, "fixture",
                [{"layer": 4, "heads": 1, "dim": 1}, {"layer": 9, "heads": 1, "dim": 1}],
                self.root / "missing-owner-plan.json")
        changed = calibration.read_json(self.root / "artifact-index.json")
        changed["artifacts"][0]["sha256"] = "0" * 64
        (self.root / "artifact-index.json").write_text(json.dumps(changed))
        summary_json = calibration.read_json(self.root / "campaign.json")
        summary_json["artifact_index"]["sha256"] = calibration.digest(self.root / "artifact-index.json")
        (self.root / "campaign.json").write_text(json.dumps(summary_json))
        with self.assertRaisesRegex(ValueError, "campaign source_tokens identity mismatch"):
            driver.prepare_campaign_plan(self.root,
                {"path": "campaign.json", "sha256": calibration.digest(self.root / "campaign.json")},
                capture_inputs, source, "fixture", [{"layer": 4, "heads": 1, "dim": 1}],
                self.root / "tampered-plan.json")

    def test_campaign_offsets_cannot_be_substituted_with_rehashed_input(self):
        summary, capture_inputs, source = self.campaign_fixture()
        manifest_path = self.root / capture_inputs["path"]
        manifest = calibration.read_json(manifest_path)
        offsets_path = self.root / manifest["prompt_token_offsets"]["path"]
        offsets_path.write_bytes(struct.pack("<II", 0, 2) * manifest["prompt"]["count"])
        manifest["prompt_token_offsets"]["sha256"] = calibration.digest(offsets_path)
        manifest_path.write_text(json.dumps(manifest))
        capture_inputs["sha256"] = calibration.digest(manifest_path)
        summary_path = self.root / summary["path"]
        record = calibration.read_json(summary_path)
        record["capture_inputs_manifest_sha256"] = capture_inputs["sha256"]
        summary_path.write_text(json.dumps(record))
        summary["sha256"] = calibration.digest(summary_path)
        with self.assertRaisesRegex(ValueError, "offsets do not match runtime coverage"):
            driver.prepare_campaign_plan(self.root, summary, capture_inputs, source, "fixture",
                [{"layer": 4, "heads": 1, "dim": 1}], self.root / "changed-offset-plan.json")

    def test_legacy_serial_campaign_prepare_assemble_load_keeps_attention_unknown(self):
        summary, capture_inputs, source = self.campaign_fixture()
        self.rewrite_campaign_attention_receipts(summary, legacy=True)
        plan_path = self.root / "legacy-plan.json"
        plan = driver.prepare_campaign_plan(self.root, summary, capture_inputs, source, "fixture",
            [{"layer": 4, "heads": 1, "dim": 1}], plan_path)
        self.assertIsNone(plan["private_kernel_route"])
        self.assertIsNone(plan["private_replay_attention"])
        receipt = driver.assemble_v2(plan_path, self.root / "legacy-assembled")
        metadata, _ = calibration.load_capture(self.root / "legacy-assembled" / receipt["captures"][0])
        self.assertIsNone(metadata["capture"]["query_campaign"]["private_kernel_route"])
        self.assertIsNone(metadata["capture"]["query_campaign"]["private_replay_attention"])

    def test_campaign_plan_accepts_only_complete_bounded_split_route_receipt(self):
        summary, capture_inputs, source = self.campaign_fixture()
        self.rewrite_campaign_attention_receipts(summary, split=True)
        summary_path = self.root / summary["path"]
        record = calibration.read_json(summary_path)
        plan = driver.prepare_campaign_plan(self.root, summary, capture_inputs, source, "fixture",
            [{"layer": 4, "heads": 1, "dim": 1}], self.root / "split-plan.json")
        self.assertEqual(plan["global_owners"], [4])
        receipt = driver.assemble_v2(self.root / "split-plan.json", self.root / "split-assembled")
        metadata, arrays = calibration.load_capture(self.root / "split-assembled" / receipt["captures"][0])
        self.assertEqual(metadata["capture"]["query_campaign"]["private_kernel_route"],
                         "bounded_split_global_replay_v1")
        self.assertEqual(metadata["capture"]["query_campaign"]["private_replay_attention"]["split_count"], 16)
        self.assertEqual(arrays["queries"].shape[0], 1)

        record["private_replay_attention"]["split_scratch_bytes"] = 0
        summary_path.write_text(json.dumps(record))
        summary["sha256"] = calibration.digest(summary_path)
        with self.assertRaisesRegex(ValueError, "bounded route proof"):
            driver.prepare_campaign_plan(self.root, summary, capture_inputs, source, "fixture",
                [{"layer": 4, "heads": 1, "dim": 1}], self.root / "forged-split-plan.json")

        record["private_replay_attention"]["split_scratch_bytes"] = 2113536
        record["private_replay_attention"]["serial_dispatches"] += 1
        summary_path.write_text(json.dumps(record))
        summary["sha256"] = calibration.digest(summary_path)
        with self.assertRaisesRegex(ValueError, "does not match physical branches"):
            driver.prepare_campaign_plan(self.root, summary, capture_inputs, source, "fixture",
                [{"layer": 4, "heads": 1, "dim": 1}], self.root / "forged-count-plan.json")

    def test_campaign_study_timing_binds_unique_physical_branches_through_load(self):
        summary, capture_inputs, source = self.campaign_fixture()
        provider = {name: 0 for name in (
            "frame_begins", "frame_submits", "frame_wait_ns", "frame_gpu_ns",
            "compute_encoders", "blit_encoders", "precomputed_tail_blocks",
            "precomputed_tail_failures", "paged_attention_1x_calls", "decode_gqa_split_calls",
            "generated_attention_decode_1x_calls", "generated_attention_flash_prefill_calls",
            "attention_prefill_direct_kv_calls", "attention_prefill_paged_kv_calls",
            "to_host_calls", "to_host_device_calls", "host_download_bytes")}
        provider.update(frame_begins=257, frame_submits=257, frame_wait_ns=11,
                        frame_gpu_ns=7, compute_encoders=8995, precomputed_tail_blocks=8960,
                        generated_attention_decode_1x_calls=8960, to_host_calls=256,
                        to_host_device_calls=256, host_download_bytes=256 * 4)
        timing = {"prefill_hidden_ns": 3, "initial_logits_ns": 5, "generated_loop_ns": 11}
        summary_path = self.root / summary["path"]
        record = calibration.read_json(summary_path)
        record["schema_version"] = 3
        record["private_study_observation"] = {
            "fit": {"physical_branch_id": "fit-study", "timing": timing,
                    "provider_delta": provider},
            "gate": {"physical_branch_id": "gate-study", "timing": timing,
                     "provider_delta": provider},
        }
        summary_path.write_text(json.dumps(record))
        summary["sha256"] = calibration.digest(summary_path)
        plan_path = self.root / "timed-plan.json"
        plan = driver.prepare_campaign_plan(self.root, summary, capture_inputs, source, "fixture",
            [{"layer": 4, "heads": 1, "dim": 1}], plan_path)
        self.assertEqual(set(plan["private_study_observation"]), {"fit", "gate"})
        receipt = driver.assemble_v2(plan_path, self.root / "timed-assembled")
        metadata, _ = calibration.load_capture(self.root / "timed-assembled" / receipt["captures"][0])
        observed = metadata["capture"]["query_campaign"]["private_study_observation"]
        self.assertEqual(observed["fit"]["physical_branch_id"], "fit-study")
        self.assertEqual(observed["gate"]["physical_branch_id"], "gate-study")
        self.assertEqual(observed["fit"]["provider_delta"]["frame_begins"], 257)

        forged = copy.deepcopy(plan)
        forged["private_study_observation"]["fit"]["physical_branch_id"] = "gate-study"
        forged_path = self.root / "forged-timing-plan.json"
        calibration.write_json(forged_path, forged)
        with self.assertRaisesRegex(ValueError, "private study branch observation"):
            driver.assemble_v2(forged_path, self.root / "forged-timing-assembled")

        # Schema 4 binds the explicit singleton paged-decode request to both
        # physical study branches and requires backend-owned frame/tail proof.
        record["schema_version"] = 5
        record["study_singleton_paged_decode_requested"] = True
        for branch in ("fit", "gate"):
            row = record["private_study_observation"][branch]
            row["singleton_paged_decode_forwards"] = 257
            row["singleton_paged_decode_expected_layer_dispatches"] = 8995
            row["provider_delta"]["precomputed_tail_blocks"] = 8995
            row["provider_delta"]["generated_attention_decode_1x_calls"] = 8995
        summary_path.write_text(json.dumps(record))
        summary["sha256"] = calibration.digest(summary_path)
        paged_plan_path = self.root / "paged-study-plan.json"
        paged_plan = driver.prepare_campaign_plan(
            self.root, summary, capture_inputs, source, "fixture",
            [{"layer": 4, "heads": 1, "dim": 1}], paged_plan_path)
        self.assertIs(paged_plan["study_singleton_paged_decode_requested"], True)
        self.assertEqual(paged_plan["private_study_observation"]["fit"]
                         ["singleton_paged_decode_expected_layer_dispatches"], 8995)
        paged_receipt = driver.assemble_v2(
            paged_plan_path, self.root / "paged-study-assembled")
        paged_metadata, _ = calibration.load_capture(
            self.root / "paged-study-assembled" / paged_receipt["captures"][0])
        self.assertIs(paged_metadata["capture"]["query_campaign"]
                      ["study_singleton_paged_decode_requested"], True)

        record["private_study_observation"]["fit"]["provider_delta"][
            "generated_attention_decode_1x_calls"] = 0
        summary_path.write_text(json.dumps(record))
        summary["sha256"] = calibration.digest(summary_path)
        with self.assertRaisesRegex(ValueError, "exact backend-owned route proof"):
            driver.prepare_campaign_plan(
                self.root, summary, capture_inputs, source, "fixture",
                [{"layer": 4, "heads": 1, "dim": 1}],
                self.root / "forged-paged-study-plan.json")

    def test_schema6_binds_question_provider_and_live_fingerprint(self):
        summary, capture_inputs, source = self.campaign_fixture()
        good = self.upgrade_campaign_schema6(summary)
        plan = driver.prepare_campaign_plan(self.root, summary, capture_inputs, source, "fixture",
            [{"layer": 4, "heads": 1, "dim": 1}], self.root / "schema6-plan.json")
        self.assertEqual(plan["private_question_observation"]["physical_branch_id"], "question")
        self.assertEqual(plan["live_context_fingerprint"]["prompt_tokens"], 1024)
        receipt = driver.assemble_v2(self.root / "schema6-plan.json", self.root / "schema6-assembled")
        metadata, _ = calibration.load_capture(
            self.root / "schema6-assembled" / receipt["captures"][0])
        campaign = metadata["capture"]["query_campaign"]
        self.assertEqual(campaign["live_context_fingerprint"]["live_payload_sha256_before"], "a" * 64)

        forged_plan = copy.deepcopy(plan)
        forged_plan["live_context_fingerprint"]["live_epoch_after"] = 2
        forged_plan["live_context_fingerprint"]["live_fingerprint_publication_epoch_after"] = 2
        forged_plan_path = self.root / "forged-live-epoch-plan.json"
        calibration.write_json(forged_plan_path, forged_plan)
        with self.assertRaisesRegex(ValueError, "live context fingerprint plan evidence"):
            driver.assemble_v2(forged_plan_path, self.root / "forged-live-epoch-assembled")

        corruptions = (
            ("live_session_identity_before", 0, "fingerprint counters"),
            ("live_session_identity_after", 78, "fingerprint counters"),
            ("live_payload_sha256_after", "c" * 64, "live payload changed"),
            ("live_descriptor_sha256_after", "c" * 64, "live descriptor changed"),
            ("live_fingerprint_diagnostic_readback_bytes_after", 0, "fingerprint counters"),
            ("live_fingerprint_publication_epoch_before", 2, "fingerprint counters"),
            ("live_fingerprint_logical_tokens_after", 1023, "fingerprint counters"),
        )
        summary_path = self.root / summary["path"]
        for index, (field, value, message) in enumerate(corruptions):
            forged = copy.deepcopy(good)
            forged[field] = value
            summary_path.write_text(json.dumps(forged))
            summary["sha256"] = calibration.digest(summary_path)
            with self.assertRaisesRegex(ValueError, message):
                driver.prepare_campaign_plan(self.root, summary, capture_inputs, source, "fixture",
                    [{"layer": 4, "heads": 1, "dim": 1}],
                    self.root / f"forged-schema6-{index}.json")

        forged = copy.deepcopy(good)
        forged["private_question_observation"]["provider_delta"]["frame_begins"] = True
        summary_path.write_text(json.dumps(forged))
        summary["sha256"] = calibration.digest(summary_path)
        with self.assertRaisesRegex(ValueError, "question provider"):
            driver.prepare_campaign_plan(self.root, summary, capture_inputs, source, "fixture",
                [{"layer": 4, "heads": 1, "dim": 1}], self.root / "forged-question-provider.json")

    def test_schema7_requires_exact_contextual_decode_and_precomputed_tail_witnesses(self):
        summary, capture_inputs, source = self.campaign_fixture()
        baseline = self.upgrade_campaign_schema6(summary)
        forged_v6 = copy.deepcopy(baseline)
        forged_v6["private_study_observation"]["fit"]["provider_delta"][
            "context_session_paged_decode_framed_dispatches"] = 0
        summary_path = self.root / summary["path"]
        summary_path.write_text(json.dumps(forged_v6))
        summary["sha256"] = calibration.digest(summary_path)
        with self.assertRaisesRegex(ValueError, "private study provider"):
            driver.prepare_campaign_plan(
                self.root, summary, capture_inputs, source, "fixture",
                [{"layer": 4, "heads": 1, "dim": 1}], self.root / "forged-schema6-v7-counter.json")

        baseline["schema_version"] = 7
        baseline["private_question_observation"]["provider_delta"][
            "context_session_paged_decode_framed_dispatches"] = 0
        for branch in ("fit", "gate"):
            baseline["private_study_observation"][branch]["provider_delta"][
                "context_session_paged_decode_framed_dispatches"] = 0
        summary_path.write_text(json.dumps(baseline))
        summary["sha256"] = calibration.digest(summary_path)
        plan = driver.prepare_campaign_plan(
            self.root, summary, capture_inputs, source, "fixture",
            [{"layer": 4, "heads": 1, "dim": 1}], self.root / "schema7-baseline-plan.json")
        self.assertIs(plan["study_singleton_paged_decode_requested"], False)

        qualified = copy.deepcopy(baseline)
        qualified["study_singleton_paged_decode_requested"] = True
        for branch in ("fit", "gate"):
            row = qualified["private_study_observation"][branch]
            row["singleton_paged_decode_forwards"] = 257
            row["singleton_paged_decode_expected_layer_dispatches"] = 8995
            row["provider_delta"]["frame_begins"] = 257
            row["provider_delta"]["frame_submits"] = 257
            row["provider_delta"]["context_session_paged_decode_framed_dispatches"] = 8995
            row["provider_delta"]["precomputed_tail_blocks"] = 8995
            row["provider_delta"]["precomputed_tail_failures"] = 0
            # The ordinary paged-KV kernel is bypassed by context sessions and
            # remains informational rather than serving as the route witness.
            row["provider_delta"]["paged_attention_1x_calls"] = 0
        summary_path.write_text(json.dumps(qualified))
        summary["sha256"] = calibration.digest(summary_path)
        accepted = driver.prepare_campaign_plan(
            self.root, summary, capture_inputs, source, "fixture",
            [{"layer": 4, "heads": 1, "dim": 1}], self.root / "schema7-qualified-plan.json")
        self.assertEqual(accepted["private_study_observation"]["fit"]
                         ["provider_delta"]["context_session_paged_decode_framed_dispatches"], 8995)

        corruptions = (
            ("context_session_paged_decode_framed_dispatches", 8994),
            ("precomputed_tail_blocks", 8994),
            ("precomputed_tail_failures", 1),
        )
        for index, (field, value) in enumerate(corruptions):
            forged = copy.deepcopy(qualified)
            forged["private_study_observation"]["fit"]["provider_delta"][field] = value
            summary_path.write_text(json.dumps(forged))
            summary["sha256"] = calibration.digest(summary_path)
            with self.assertRaisesRegex(ValueError, "exact contextual route proof"):
                driver.prepare_campaign_plan(
                    self.root, summary, capture_inputs, source, "fixture",
                    [{"layer": 4, "heads": 1, "dim": 1}],
                    self.root / f"forged-schema7-route-{index}.json")

        forged = copy.deepcopy(qualified)
        forged["private_question_observation"]["provider_delta"][
            "context_session_paged_decode_framed_dispatches"] = 1
        summary_path.write_text(json.dumps(forged))
        summary["sha256"] = calibration.digest(summary_path)
        with self.assertRaisesRegex(ValueError, "question replay"):
            driver.prepare_campaign_plan(
                self.root, summary, capture_inputs, source, "fixture",
                [{"layer": 4, "heads": 1, "dim": 1}],
                self.root / "forged-schema7-question-route.json")

    def v2_fixture_from_new_root(self):
        # A new per-test directory lets the create-once fixture be reused for
        # a second independent adversarial assembly.
        old = self.root
        self.root = old / "second"
        self.root.mkdir()
        try:
            path = self.v2_fixture()
        finally:
            self.root = old
        positions = path.parent / "request-question-positions.u32"
        values = calibration.np.fromfile(positions, dtype="<u4")
        values[0] += 1
        values.tofile(positions)
        return path


    def test_schema2_assembler_uses_kv_head_local_strata_for_gqa(self):
        plan_path = self.v2_fixture()
        plan = calibration.read_json(plan_path)
        base_path = self.root / "base/spec.json"
        base = calibration.read_json(base_path)
        np = calibration.np
        base["model"]["global_owner_shapes"] = [{"layer": 4, "heads": 2, "dim": 1}]
        capture = base["capture"]
        capture.update(heads=2, training_rows=6, validation_rows=4,
                       consumers=[{"layer": 4, "query_heads": 4}],
                       strata=[
                           {"consumer_layer": 4, "query_head": query_head,
                            "kv_head": query_head // 2,
                            "training_start": (query_head % 2) * 3,
                            "training_count": 3,
                            "validation_start": (query_head % 2) * 2,
                            "validation_count": 2}
                           for query_head in range(4)])
        np.zeros((4, 2, 1), dtype="<f2").tofile(self.root / "base/keys.raw")
        np.asarray([-2, -1, 1, 2] * 2, dtype="<f2").reshape(4, 2, 1).tofile(
            self.root / "base/values.raw")
        np.zeros((2, 10, 1), dtype="<f4").tofile(self.root / "base/queries.raw")
        np.zeros((2, 10), dtype="<u4").tofile(self.root / "base/positions.raw")
        base_path.write_text(json.dumps(base))

        expected_by_head = [[], []]
        for source_index, source in enumerate(plan["sources"]):
            cell = source["owners"][0]
            descriptor_path = self.root / cell["descriptor"]
            descriptor = calibration.read_json(descriptor_path)
            descriptor.update(heads=2, rows_per_head=2, rows_per_stratum=1)
            positions = calibration.replay_positions(
                descriptor["logical_start"], descriptor["logical_count"], 1,
                descriptor["partition"] != "training")
            query_values = np.asarray([
                source_index + 1, source_index + 101,
                source_index + 11, source_index + 111,
            ], dtype="<f4").reshape(2, 2, 1)
            query_values.tofile(self.root / cell["queries"])
            np.tile(positions, (2, 2)).astype("<u4").tofile(self.root / cell["positions"])
            descriptor["queries_sha256"] = calibration.digest(self.root / cell["queries"])
            descriptor["positions_sha256"] = calibration.digest(self.root / cell["positions"])
            descriptor_path.write_text(json.dumps(descriptor))
            cell.update(descriptor_sha256=calibration.digest(descriptor_path),
                        queries_sha256=descriptor["queries_sha256"],
                        positions_sha256=descriptor["positions_sha256"])
            for head in range(2):
                expected_by_head[head].append((source_index, query_values[head, :, 0].tolist()))
        plan_path.write_text(json.dumps(plan))

        receipt = driver.assemble_v2(plan_path, self.root / "gqa-two-kv-heads")
        manifest = self.root / "gqa-two-kv-heads" / receipt["captures"][0]
        meta, arrays = calibration.load_capture(manifest)
        self.assertEqual((meta["capture"]["training_rows"],
                          meta["capture"]["validation_rows"]), (6, 4))
        self.assertEqual([(row["kv_head"], row["training_start"], row["validation_start"])
                          for row in meta["capture"]["strata"]],
                         [(0, 0, 0), (0, 3, 2), (1, 0, 0), (1, 3, 2)])
        # Independent source order: three training sources then two validation
        # sources, repeated for each of the two local GQA strata per KV head.
        for head in range(2):
            source_rows = [rows for _, rows in expected_by_head[head]]
            expected = ([source_rows[i][0] for i in range(3)] +
                        [source_rows[i][1] for i in range(3)] +
                        [source_rows[i][0] for i in range(3, 5)] +
                        [source_rows[i][1] for i in range(3, 5)])
            self.assertEqual(arrays["queries"][head, :, 0].tolist(), expected)
            self.assertEqual(arrays["positions"][head].tolist(), [
                int(calibration.replay_positions(
                    calibration.read_json(self.root / plan["sources"][i]["owners"][0]["descriptor"])["logical_start"],
                    20, 1, i >= 3)[0])
                for i in [0, 1, 2, 0, 1, 2, 3, 4, 3, 4]
            ])

    def test_schema2_explicit_legacy_diagnostic_cannot_be_calibration_evidence(self):
        plan_path = self.v2_fixture()
        plan = calibration.read_json(plan_path)

if __name__ == "__main__":
    unittest.main()
