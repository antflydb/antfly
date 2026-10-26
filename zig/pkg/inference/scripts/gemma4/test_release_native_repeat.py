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

import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

import native_repeat_profile
import release_attention_matching as release


class NativeCalibrationReleaseTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        names = ("profile", "campaign_spec", "results", "target_bootstrap",
                 "target_bootstrap_resource")
        self.raw = {"schema": release.NATIVE_REPEAT_CALIBRATION_SCHEMA,
                    "evidence_scope": "native_repeat_same_replay_calibration"}
        for name in names:
            path = self.root / (name + ".json")
            path.write_text(json.dumps({"fixture": name}))
            self.raw[name] = {"path": path.name, "sha256": release.sha(path)}
        self.identity = {name: format(index + 1, "064x") for index, name in enumerate(
            ("artifact", "tokenizer", "template", "layout", "implementation"))}
        self.identity["profile"] = self.raw["profile"]["sha256"]
        self.origin = {name: format(index + 20, "064x") for index, name in enumerate(
            ("binary_sha256", "source_build_sha256", "device_sha256", "os_sha256"))}
        self.documents = {"d" * 64}
        self.summary = {
            "evidence_scope": "native_repeat_same_replay_calibration",
            "calibration_only": True, "fresh_unseen_numerical_qualified": False,
            "serving_qualified": False, "release_authority": False,
            "semantic_identity": {name + "_sha256": self.identity[name] for name in (
                "artifact", "tokenizer", "template", "layout", "implementation")},
            "provenance": {name: self.origin[name] for name in (
                "binary_sha256", "source_build_sha256")},
            "calibration_documents": list(self.documents),
            "owner_heads": [{"owner_layer": owner, "kv_head": head,
                             "retained_rows_per_block": 128}
                            for owner in (5, 11, 17, 23) for head in (0, 1)],
            "selected_head_count": 8,
            "consumer_strata": [dict(zip(("owner_layer", "kv_head", "consumer_layer",
                                            "query_head"), row))
                                for row in sorted(release.expected_strata("gemma4-e4b"))],
            "calibration_context_tokens": [8192],
            "profile": {"profile_semantic_sha256": "b" * 64},
        }
        self.summary["per_head_numerics"] = copy.deepcopy(self.summary["owner_heads"])

    def audit(self, model="gemma4-e4b", lengths=None):
        with mock.patch.object(native_repeat_profile, "validate_freeze",
                               return_value=self.summary) as validator:
            result = release.raw_numerical_contract(self.raw, self.root,
                self.identity, self.origin, model, self.documents,
                [8192] if lengths is None else lengths)
        return result, validator

    def test_native_calibration_is_audited_and_cannot_complete_release(self):
        result, validator = self.audit()
        validator.assert_called_once()
        self.assertEqual(result["calibration_context_tokens"], [8192])
        self.assertNotIn("qualified_context_tokens", result)
        self.assertEqual(result["evaluation_documents"], [])
        gates = release.numerical_incomplete_gates(result)
        self.assertIn("fresh_unseen_native_numerical_observations", gates)
        release_result = release._finish_v2_audit([{"incomplete_gates": gates}])
        self.assertFalse(release_result["evidence_passed"])
        self.assertFalse(release_result["serving_qualified"])

    def test_files_are_checked_before_graph_validation(self):
        (self.root / "results.json").write_text("substituted results")
        with mock.patch.object(native_repeat_profile, "validate_freeze") as validator:
            with self.assertRaisesRegex(ValueError, "digest mismatch"):
                release.raw_numerical_contract(self.raw, self.root, self.identity,
                    self.origin, "gemma4-e4b", self.documents, [8192])
            validator.assert_not_called()

    def test_fresh_or_serving_authority_cannot_be_inferred(self):
        for key in ("fresh_unseen_numerical_qualified", "serving_qualified", "release_authority"):
            with self.subTest(key=key):
                self.summary[key] = True
                with self.assertRaisesRegex(ValueError, "cannot grant release"):
                    self.audit()
                self.summary[key] = False

    def test_identity_build_and_corpus_substitution_fail(self):
        original = copy.deepcopy(self.summary)
        mutations = (
            lambda value: value["semantic_identity"].update(implementation_sha256="c" * 64),
            lambda value: value["provenance"].update(binary_sha256="c" * 64),
            lambda value: value.update(calibration_documents=["c" * 64]),
        )
        for mutation in mutations:
            with self.subTest(mutation=mutation):
                self.summary = copy.deepcopy(original)
                mutation(self.summary)
                with self.assertRaisesRegex(ValueError, "mismatch"):
                    self.audit()

    def test_complete_physical_heads_and_shared_consumers_are_required(self):
        for key in ("owner_heads", "consumer_strata", "per_head_numerics"):
            original = copy.deepcopy(self.summary[key])
            with self.subTest(key=key):
                self.summary[key].pop()
                with self.assertRaisesRegex(ValueError, "heads|coverage|per-head"):
                    self.audit()
                self.summary[key] = original

    def test_model_context_or_profile_claim_cannot_be_broadened(self):
        with self.assertRaisesRegex(ValueError, "artifact mismatch"):
            self.audit(model="gemma4-e2b")
        with self.assertRaisesRegex(ValueError, "context envelope"):
            self.audit(lengths=[8192, 32768])
        self.identity["profile"] = "0" * 64
        with self.assertRaisesRegex(ValueError, "artifact mismatch"):
            self.audit()

    def test_native_format_is_disjoint_from_legacy_raw_evidence(self):
        self.raw["schema"] = "foreign-schema"
        with self.assertRaisesRegex(ValueError, "invalid raw numerical evidence bundle"):
            self.audit()


if __name__ == "__main__":
    unittest.main()
