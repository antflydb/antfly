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

import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import exact_context_qualify as exact
import release_attention_matching as release
import release_build_fixture


class ExactContextQualificationTests(unittest.TestCase):
    def loaded_identity(self, generation=1, **overrides):
        value = {"load_generation": generation,
                 "artifact_sha256": "a" * 64, "tokenizer_sha256": "a" * 64,
                 "template_sha256": "a" * 64, "layout_sha256": "a" * 64,
                 "implementation_sha256": "a" * 64}
        value.update(overrides)
        return value

    def row(self, storage="automatic", elapsed_ns=1_000_000, loaded_identity=None):
        return {"schema_version": 2, "route": "native_pipeline", "storage": storage,
                "prompt_tokens": 7680, "reserved_output_tokens": 512,
                "produced_output_tokens": 512, "total_positions": 8192,
                "prefill_chunk_rows": 512, "output_sha256": hashlib.sha256(bytes(512 * 4)).hexdigest(),
                "output_token_ids": [0] * 512,
                "elapsed_ns": elapsed_ns, "first_callback_ns": 900_000,
                "callback_count": 32, "phases_ms": {"prefill": .8, "decode": .2, "first_token": None},
                "context_live_bytes": 100, "context_peak_bytes": 200,
                "backend_kv_readback_bytes": 0,
                "loaded_identity": loaded_identity or self.loaded_identity()}

    def identity(self):
        return {**{key: "a" * 64 for key in release.SEMANTIC_BINDINGS},
                "dtype": "f16_kv_f32_bias", "kernel_route": "metal_context_optimized"}

    def origin(self):
        return {key: "b" * 64 for key in release.PROVENANCE_BINDINGS}

    def receipt(self):
        observations = []
        for index in range(5):
            for storage in exact.STORAGES:
                observations.append({"id": f"warm-{index}-{storage}", "pair_id": f"warm-{index}", "warmup": True,
                                     "observation": self.row(storage), "min_free_percent": 30,
                                     "swapout_growth_bytes": 0})
        for index in range(release.MIN_PERFORMANCE_PAIRS):
            observations.append({"id": f"exact-{index}", "pair_id": f"pair-{index}", "warmup": False,
                                 "observation": self.row("automatic"), "min_free_percent": 30,
                                 "swapout_growth_bytes": 0})
            observations.append({"id": f"packed-{index}", "pair_id": f"pair-{index}", "warmup": False,
                                 "observation": self.row("packed_exact", 1_040_000), "min_free_percent": 30,
                                 "swapout_growth_bytes": 0})
            if index % 2:
                observations[-2:] = observations[-2:][::-1]
        for entry in observations:
            entry["observation"].update(workload="independent_document",
                                        prompt_sha256=hashlib.sha256(entry["pair_id"].encode()).hexdigest())
        return {"schema_version": 1, "completed": True, "loaded_identity_verified": True,
                "file_provenance": {"artifact_sha256": "a" * 64, "binary_sha256": "b" * 64,
                                    "source_build_receipt_sha256": "b" * 64},
                "observations": observations}

    def test_incomplete_collection_and_executable_provenance_reject(self):
        receipt = self.receipt()
        receipt["completed"] = False
        with self.assertRaisesRegex(ValueError, "incomplete exact-context collection"):
            exact.audit(receipt, self.identity(), self.origin(), [8192])
        for field in ("binary_sha256", "source_build_receipt_sha256"):
            receipt = self.receipt()
            receipt["file_provenance"][field] = "c" * 64
            with self.subTest(field=field), self.assertRaisesRegex(ValueError, "executable provenance mismatch"):
                exact.audit(receipt, self.identity(), self.origin(), [8192])

    def test_warmup_measurement_reclassification_rejects(self):
        receipt = self.receipt()
        receipt["observations"][0]["warmup"] = "true"
        with self.assertRaisesRegex(ValueError, "warmup classification"):
            exact.audit(receipt, self.identity(), self.origin(), [8192])
        receipt = self.receipt()
        for entry in receipt["observations"]:
            if entry["pair_id"] == "warm-0":
                entry["pair_id"] = "pair-0"
        with self.assertRaisesRegex(ValueError, "pair reused across warmup and measurement"):
            exact.audit(receipt, self.identity(), self.origin(), [8192])

    def test_partial_decode_and_warmup_divergence_do_not_qualify(self):
        receipt = self.receipt()
        row = receipt["observations"][-1]["observation"]
        row["produced_output_tokens"] = 32
        row["total_positions"] = row["prompt_tokens"] + 32
        with self.assertRaisesRegex(ValueError, "full output reservation"):
            exact.audit(receipt, self.identity(), self.origin(), [8192])
        receipt = self.receipt()
        receipt["observations"][0]["observation"]["output_sha256"] = "c" * 64
        with self.assertRaisesRegex(ValueError, "exact output mismatch"):
            exact.audit(receipt, self.identity(), self.origin(), [8192])

    def test_paired_exact_campaign_passes_and_embeds_bounds(self):
        receipt = self.receipt()
        receipt["loaded_identity_verified"] = False  # Audit derives this from raw rows.
        for index, entry in enumerate(receipt["observations"]):
            entry["observation"]["loaded_identity"]["load_generation"] = index + 1
        result = exact.audit(receipt, self.identity(), self.origin(), [8192])
        self.assertTrue(result["evidence_passed"])
        self.assertTrue(result["loaded_identity_verified"])
        self.assertLessEqual(result["results"][0]["packed_exact_elapsed_ratio_upper_95"], 1.05)
        self.assertFalse(result["serving_qualified"])

    def test_opposing_prompt_tails_cannot_hide_paired_regressions(self):
        receipt = self.receipt()
        for entry in receipt["observations"]:
            if entry["warmup"]:
                continue
            pair = int(entry["pair_id"].split("-")[-1])
            automatic = entry["observation"]["storage"] == "automatic"
            entry["observation"]["elapsed_ns"] = 2_000_000 if automatic == (pair % 2 == 0) else 1_000_000
        with self.assertRaisesRegex(ValueError, "performance regression"):
            exact.audit(receipt, self.identity(), self.origin(), [8192])

    def test_decode_regression_cannot_hide_in_prefill_time(self):
        receipt = self.receipt()
        for entry in receipt["observations"]:
            if entry["observation"]["storage"] == "packed_exact":
                entry["observation"]["phases_ms"]["decode"] = .3
        with self.assertRaisesRegex(ValueError, "decode throughput regression"):
            exact.audit(receipt, self.identity(), self.origin(), [8192])

    def test_raw_token_forgery_and_missing_decode_timing_reject(self):
        receipt = self.receipt()
        receipt["observations"][0]["observation"]["output_token_ids"][0] = 1
        with self.assertRaisesRegex(ValueError, "raw token digest"):
            exact.audit(receipt, self.identity(), self.origin(), [8192])
        receipt = self.receipt()
        del receipt["observations"][0]["observation"]["phases_ms"]["decode"]
        with self.assertRaisesRegex(ValueError, "exact decode timing"):
            exact.audit(receipt, self.identity(), self.origin(), [8192])

    def test_duplicate_observation_json_fields_reject(self):
        with self.assertRaisesRegex(ValueError, "duplicate JSON key"):
            exact.parse_observations(exact.PREFIX + '{"elapsed_ns":1,"elapsed_ns":2}')

    def test_capacity_repeated_prompts_and_fixed_order_do_not_qualify(self):
        receipt = self.receipt()
        receipt["observations"][0]["observation"]["workload"] = "repeated_token_capacity"
        with self.assertRaisesRegex(ValueError, "not an independent corpus"):
            exact.audit(receipt, self.identity(), self.origin(), [8192])
        receipt = self.receipt()
        for entry in receipt["observations"]:
            entry["observation"]["prompt_sha256"] = "e" * 64
        with self.assertRaisesRegex(ValueError, "corpus is reused"):
            exact.audit(receipt, self.identity(), self.origin(), [8192])
        receipt = self.receipt()
        receipt["observations"].sort(key=lambda entry: (entry["pair_id"], entry["observation"]["storage"]))
        with self.assertRaisesRegex(ValueError, "execution order is imbalanced"):
            exact.audit(receipt, self.identity(), self.origin(), [8192])

    def test_missing_counter_output_mismatch_and_sparse_pairs_reject(self):
        receipt = self.receipt()
        receipt["observations"][0]["observation"]["context_peak_bytes"] = None
        with self.assertRaisesRegex(ValueError, "missing exact-context allocation"):
            exact.audit(receipt, self.identity(), self.origin(), [8192])
        receipt = self.receipt()
        receipt["observations"][-1]["observation"]["output_sha256"] = "c" * 64
        with self.assertRaisesRegex(ValueError, "output mismatch"):
            exact.audit(receipt, self.identity(), self.origin(), [8192])
        receipt = self.receipt()
        receipt["observations"] = receipt["observations"][:-2]
        with self.assertRaisesRegex(ValueError, "too few paired"):
            exact.audit(receipt, self.identity(), self.origin(), [8192])

    def test_incomplete_duplicate_and_unscheduled_samples_reject(self):
        receipt = self.receipt()
        receipt["observations"].append({"id": "incomplete", "pair_id": "incomplete", "warmup": False,
                                        "observation": self.row("automatic"), "min_free_percent": 30,
                                        "swapout_growth_bytes": 0})
        with self.assertRaisesRegex(ValueError, "incomplete measured pair variants"):
            exact.audit(receipt, self.identity(), self.origin(), [8192])

        receipt = self.receipt()
        receipt["observations"].append({"id": "duplicate-variant", "pair_id": "pair-0", "warmup": False,
                                        "observation": self.row("automatic"), "min_free_percent": 30,
                                        "swapout_growth_bytes": 0})
        with self.assertRaisesRegex(ValueError, "duplicate measured pair variant"):
            exact.audit(receipt, self.identity(), self.origin(), [8192])

        receipt = self.receipt()
        receipt["observations"][1]["id"] = receipt["observations"][0]["id"]
        with self.assertRaisesRegex(ValueError, "duplicate exact-context observation id"):
            exact.audit(receipt, self.identity(), self.origin(), [8192])

        receipt = self.receipt()
        extra = {"id": "unscheduled", "pair_id": "other", "warmup": False,
                 "observation": self.row("automatic"), "min_free_percent": 30,
                 "swapout_growth_bytes": 0}
        extra["observation"]["prompt_tokens"] = 8192
        extra["observation"]["total_positions"] = 8704
        receipt["observations"].append(extra)
        with self.assertRaisesRegex(ValueError, "raw/sample count mismatch"):
            exact.audit(receipt, self.identity(), self.origin(), [8192])

    def test_audit_rederives_identity_instead_of_trusting_boolean(self):
        receipt = self.receipt()
        receipt["loaded_identity_verified"] = True
        receipt["observations"][1]["observation"]["loaded_identity"]["tokenizer_sha256"] = "b" * 64
        with self.assertRaisesRegex(ValueError, "loaded identity changed across observations"):
            exact.audit(receipt, self.identity(), self.origin(), [8192])

        receipt = self.receipt()
        for entry in receipt["observations"]:
            entry["observation"]["loaded_identity"]["implementation_sha256"] = "b" * 64
        with self.assertRaisesRegex(ValueError, "semantic identity: implementation"):
            exact.audit(receipt, self.identity(), self.origin(), [8192])

        receipt = self.receipt()
        receipt["file_provenance"]["artifact_sha256"] = "b" * 64
        with self.assertRaisesRegex(ValueError, "requested artifact"):
            exact.audit(receipt, self.identity(), self.origin(), [8192])

    def test_loaded_identity_contract_rejects_malformed_or_forged_fields(self):
        for value in (
            self.loaded_identity(generation=0),
            self.loaded_identity(generation=True),
            self.loaded_identity(generation=2**64),
            self.loaded_identity(tokenizer_sha256="A" * 64),
            {**self.loaded_identity(), "caller_verified": True},
        ):
            row = self.row(loaded_identity=value)
            with self.subTest(value=value), self.assertRaisesRegex(ValueError, "invalid loaded identity"):
                exact.observation_contract(row)

    @patch.object(exact, "memory_free_percent", return_value=10)
    @patch.object(exact, "swapout_bytes", return_value=0)
    def test_launch_guard_refuses_before_spawning(self, *_):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary, artifact, source = root / "driver", root / "model.gguf", root / "build.json"
            binary.write_text("#!/bin/sh\nexit 0\n")
            binary.chmod(0o755)
            artifact.write_bytes(b"model")
            release_build_fixture.write(root, source, binary)
            plan = {"schema_version": 1, "binary": str(binary), "artifact": str(artifact),
                    "source_build_receipt": str(source),
                    "runs": [{"id": "one", "argv": [str(binary)],
                              "expected": {"storage": "automatic", "prompt_tokens": 7680,
                                           "reserved_output_tokens": 512}}]}
            with patch.object(exact.subprocess, "Popen") as launch:
                with self.assertRaisesRegex(ValueError, "free-memory guard"):
                    exact.run_plan(plan, root / "receipt.json")
                launch.assert_not_called()
            self.assertFalse(release.read_json(root / "receipt.json")["completed"])

    @patch.object(exact, "group_rss_mib", return_value=0)
    @patch.object(exact, "memory_free_percent", return_value=50)
    @patch.object(exact, "swapout_bytes", return_value=0)
    @patch.object(exact.shutil, "disk_usage", return_value=type("Usage", (), {"free": 20 * 1024**3})())
    def test_collection_verifies_loaded_identity_but_remains_diagnostic(self, *_):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary, artifact, source = root / "driver", root / "model.gguf", root / "build.json"
            artifact.write_bytes(b"model")
            row = json.dumps(self.row(loaded_identity=self.loaded_identity(
                artifact_sha256=release.sha(artifact))))
            binary.write_text("#!/bin/sh\necho '" + exact.PREFIX + row + "'\n")
            binary.chmod(0o755)
            release_build_fixture.write(root, source, binary)
            plan = {"schema_version": 1, "binary": str(binary), "artifact": str(artifact),
                    "source_build_receipt": str(source),
                    "runs": [{"id": "ok", "argv": [str(binary)],
                              "expected": {"storage": "automatic", "prompt_tokens": 7680,
                                           "reserved_output_tokens": 512}}]}
            receipt = exact.run_plan(plan, root / "receipt.json")
            self.assertTrue(receipt["completed"])
            self.assertTrue(receipt["loaded_identity_verified"])
            self.assertEqual(receipt["loaded_semantic_identity"]["artifact"], release.sha(artifact))
            self.assertTrue(receipt["diagnostic_only"])
            self.assertFalse(receipt["evidence_passed"])
            identity = self.identity()
            identity["artifact"] = release.sha(artifact)
            origin = self.origin()
            origin.update(binary_sha256=release.sha(binary), source_build_sha256=release.sha(source))
            raw = {"session": {"path": "receipt.json", "sha256": release.sha(root / "receipt.json")}}
            reconstructed = exact.raw_contract(raw, root, identity, origin)
            self.assertEqual(reconstructed["observations"][0]["observation"], receipt["observations"][0]["observation"])
            self.assertEqual(reconstructed["observations"][0]["peak_group_rss_mib"], 0)
            # Rehashing a summary cannot change measurements captured in the
            # parent journal or the executable's original output.
            receipt["observations"][0]["min_free_percent"] = 99
            (root / "receipt.json").write_text(json.dumps(receipt))
            raw["session"]["sha256"] = release.sha(root / "receipt.json")
            with self.assertRaisesRegex(ValueError, "summary does not match journal"):
                exact.raw_contract(raw, root, identity, origin)
            receipt["observations"][0]["min_free_percent"] = 50
            events_path = root / receipt["raw_files"]["events"]["path"]
            events = list(release.journal_records(events_path))
            next(event for event in events if event["event"] == "resource")["swapout_bytes"] = 1
            events_path.write_text("".join(json.dumps(event) + "\n" for event in events))
            receipt["raw_files"]["events"]["sha256"] = release.sha(events_path)
            (root / "receipt.json").write_text(json.dumps(receipt))
            raw["session"]["sha256"] = release.sha(root / "receipt.json")
            with self.assertRaisesRegex(ValueError, "resource guard failed"):
                exact.raw_contract(raw, root, identity, origin)

    @patch.object(exact, "group_rss_mib", return_value=0)
    @patch.object(exact, "memory_free_percent", return_value=50)
    @patch.object(exact, "swapout_bytes", return_value=0)
    @patch.object(exact.shutil, "disk_usage", return_value=type("Usage", (), {"free": 20 * 1024**3})())
    def test_stderr_observation_env_and_partial_failure_receipt(self, *_):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary, artifact, source = root / "driver", root / "model.gguf", root / "build.json"
            artifact.write_bytes(b"model")
            row = json.dumps(self.row(loaded_identity=self.loaded_identity(
                artifact_sha256=release.sha(artifact))))
            binary.write_text("#!/bin/sh\n" +
                              "if [ \"$ANTFLY_EXACT_CASE\" = fail ]; then echo broken >&2; exit 3; fi\n" +
                              "echo noise-before >&2\n" +
                              "echo '" + exact.PREFIX + row + "' >&2\n")
            binary.chmod(0o755)
            release_build_fixture.write(root, source, binary)
            expected = {"storage": "automatic", "prompt_tokens": 7680, "reserved_output_tokens": 512}
            plan = {"schema_version": 1, "binary": str(binary), "artifact": str(artifact),
                    "source_build_receipt": str(source),
                    "runs": [{"id": "ok", "argv": [str(binary)], "env": {"ANTFLY_EXACT_CASE": "ok"},
                              "expected": expected},
                             {"id": "fails", "argv": [str(binary)], "env": {"ANTFLY_EXACT_CASE": "fail"},
                              "expected": expected}]}
            output = root / "receipt.json"
            with self.assertRaisesRegex(ValueError, "driver failed"):
                exact.run_plan(plan, output)
            terminal = release.read_json(output)
            self.assertFalse(terminal["completed"])
            self.assertEqual([row["id"] for row in terminal["observations"]], ["ok"])
            self.assertTrue((root / "receipt.json.ok.stderr.log").exists())

    def test_duplicate_or_nonfinite_observations_reject(self):
        output = exact.PREFIX + json.dumps(self.row()) + "\n" + exact.PREFIX + json.dumps(self.row())
        self.assertEqual(len(exact.parse_observations(output)), 2)
        with self.assertRaisesRegex(ValueError, "nonfinite"):
            exact.parse_observations(exact.PREFIX + '{"x":NaN}')


if __name__ == "__main__":
    unittest.main()
