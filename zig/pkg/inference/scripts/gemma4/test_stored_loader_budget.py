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
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

import stored_loader_budget as budget


MANIFEST_PATH = "/qualification/manifest.json"
MANIFEST_SHA = "ab" * 32


def fixture():
    artifacts = []
    parts = []
    for owner, retained in ((4, 352), (9, 288), (14, 288)):
        parts.append([{"logical_count": 512, "retained": [512]}]
                     + [{"logical_count": 512, "retained": [retained]} for _ in range(12)]
                     + [{"logical_count": 1504, "retained": [1504]}])
        for block in range(12):
            coordinate = {"owner_layer": owner, "head": 0, "span": block,
                          "logical_start": (block + 1) * 512, "logical_count": 512,
                          "retained_rows": retained, "head_dim": 512}
            artifacts.append({"coordinate": coordinate,
                              "keys": {"size_bytes": retained * 512 * 2},
                              "values": {"size_bytes": retained * 512 * 2},
                              "bias": {"size_bytes": retained * 4}})
    coordinates = [copy.deepcopy(row["coordinate"]) for row in artifacts]
    manifest = {"artifacts": artifacts, "parts": parts, "prompt_tokens": 8160,
                "reservation": {"coordinates": coordinates,
                                "loader_budget_bytes": 23412736,
                                "materialization_host_bytes": 23396352}}
    receipt = {"schema_name": budget.SCHEMA, "completed": True, "diagnostic_only": True,
               "serving_authority": False, "release_authority": False,
               "input_manifest": {"path": MANIFEST_PATH, "sha256": list(bytes.fromhex(MANIFEST_SHA))},
               "geometry_sha256": list(budget.geometry_digest(coordinates)),
               "artifact_count": 36, "coordinates": copy.deepcopy(coordinates),
               "destination_spans": list(range(1, 13)) * 3, "layout_validated": True,
               "host_page_size_bytes": 16384,
               # Synthetic native entry-size evidence, not a claimed ABI measurement.
               "entries_logical_bytes": 36 * 256, "entries_physical_upper_bytes": 16384,
               "buffers_shared_bytes": 23396352, "physical_upper_bytes": 23412736,
               "declared_loader_budget_bytes": 23412736,
               "declared_materialization_host_bytes": 23396352,
               "buffers_match_declared_materialization": True,
               "total_matches_declared_loader_budget": True}
    return manifest, receipt


class BudgetReceiptTests(unittest.TestCase):
    def validate(self, manifest, receipt, **kwargs):
        return budget.validate_budget_receipt(receipt, manifest, manifest_path=MANIFEST_PATH,
                                             manifest_sha256=MANIFEST_SHA, **kwargs)

    def test_accepts_separate_entry_allocation(self):
        manifest, receipt = fixture()
        result = self.validate(manifest, receipt)
        self.assertEqual(result["physical_upper_bytes"] - result["buffers_shared_bytes"], 16384)
        self.assertTrue(result["metadata_admission_validated"])
        self.assertEqual(result["validation_mode"], "admission")

    def test_exact_prefix_and_tail_preserve_lossy_ordinals(self):
        manifest, receipt = fixture()
        self.assertEqual(budget.validate_manifest_layout(manifest), list(range(1, 13)) * 3)
        self.validate(manifest, receipt)

    def test_physical_span_substitution_does_not_relabel_frozen_artifacts(self):
        manifest, _ = fixture()
        for row in manifest["artifacts"]:
            row["coordinate"]["span"] += 1
        for row in manifest["reservation"]["coordinates"]:
            row["span"] += 1
        with self.assertRaisesRegex(ValueError, "missing stored artifact"):
            budget.validate_manifest_layout(manifest)

    def test_lossy_ordinal_must_still_match_logical_source(self):
        manifest, _ = fixture()
        manifest["parts"][0][0]["logical_count"] = 513
        manifest["parts"][0][0]["retained"] = [513]
        with self.assertRaisesRegex(ValueError, "differs from destination span"):
            budget.validate_manifest_layout(manifest)

    def test_historical_buffer_only_budget_is_diagnostic_not_admission(self):
        manifest, receipt = fixture()
        manifest["reservation"]["loader_budget_bytes"] = 23396352
        receipt["declared_loader_budget_bytes"] = 23396352
        receipt["total_matches_declared_loader_budget"] = False
        with self.assertRaisesRegex(ValueError, "loader budget mismatch"):
            self.validate(manifest, receipt)
        result = self.validate(manifest, receipt, require_declared_match=False)
        self.assertEqual(result["physical_upper_bytes"], 23412736)
        self.assertFalse(result["declared_loader_budget_matches"])
        self.assertFalse(result["metadata_admission_validated"])
        self.assertEqual(result["validation_mode"], "historical_calculation_only")

    def test_reservation_coordinates_reject_boolean_integer_alias(self):
        manifest, receipt = fixture()
        manifest["reservation"]["coordinates"][0]["span"] = False
        with self.assertRaisesRegex(ValueError, "span"):
            self.validate(manifest, receipt)

    def test_parts_must_cover_exact_prompt(self):
        for prompt in (9999, True, 8160.0):
            with self.subTest(prompt=prompt):
                manifest, receipt = fixture()
                manifest["prompt_tokens"] = prompt
                with self.assertRaises(ValueError):
                    self.validate(manifest, receipt)

    def test_prospective_admission_checks_declared_budgets(self):
        manifest, receipt = fixture()
        prospective = copy.deepcopy(manifest)
        prospective["reservation"]["loader_budget_bytes"] = 23396352
        with self.assertRaisesRegex(ValueError, "prospective declared budget"):
            self.validate(manifest, receipt, prospective_manifest=prospective)

    def test_historical_mode_rejects_wrong_prospective_budget(self):
        manifest, receipt = fixture()
        manifest["reservation"]["loader_budget_bytes"] = 23396352
        receipt["declared_loader_budget_bytes"] = 23396352
        receipt["total_matches_declared_loader_budget"] = False
        prospective = copy.deepcopy(manifest)
        prospective["reservation"]["loader_budget_bytes"] = 1
        with self.assertRaisesRegex(ValueError, "prospective declared budget"):
            self.validate(manifest, receipt, require_declared_match=False, prospective_manifest=prospective)

    def test_historical_mode_checks_corrected_prospective_without_granting_admission(self):
        manifest, receipt = fixture()
        manifest["reservation"]["loader_budget_bytes"] = 23396352
        receipt["declared_loader_budget_bytes"] = 23396352
        receipt["total_matches_declared_loader_budget"] = False
        prospective = copy.deepcopy(manifest)
        prospective["reservation"]["loader_budget_bytes"] = 23412736
        result = self.validate(manifest, receipt, require_declared_match=False, prospective_manifest=prospective)
        self.assertFalse(result["metadata_admission_validated"])
        self.assertFalse(result["declared_loader_budget_matches"])
        self.assertEqual(result["validation_mode"], "historical_calculation_only")

    def test_total_cannot_omit_entry_allocation(self):
        manifest, receipt = fixture()
        receipt["physical_upper_bytes"] = 23396352
        with self.assertRaisesRegex(ValueError, "omitted entry array"):
            self.validate(manifest, receipt)

    def test_does_not_guess_private_native_entry_size(self):
        manifest, receipt = fixture()
        receipt["entries_logical_bytes"] = 36 * 1024
        receipt["entries_physical_upper_bytes"] = 49152
        receipt["physical_upper_bytes"] = 23396352 + 49152
        receipt["declared_loader_budget_bytes"] = receipt["physical_upper_bytes"]
        manifest["reservation"]["loader_budget_bytes"] = receipt["physical_upper_bytes"]
        self.assertEqual(self.validate(manifest, receipt)["entries_physical_upper_bytes"], 49152)

    def test_rejects_changed_geometry_even_at_same_count(self):
        manifest, receipt = fixture()
        prospective = copy.deepcopy(manifest)
        prospective["artifacts"][0]["coordinate"]["logical_start"] += 1
        prospective["reservation"]["coordinates"][0]["logical_start"] += 1
        with self.assertRaisesRegex(ValueError, "different geometry"):
            self.validate(manifest, receipt, prospective_manifest=prospective)

    def test_rejects_metadata_tampering(self):
        manifest, original = fixture()
        mutations = {
            "authority": lambda r: r.update(serving_authority=True),
            "digest": lambda r: r["geometry_sha256"].__setitem__(0, r["geometry_sha256"][0] ^ 1),
            "coordinate": lambda r: r["coordinates"][0].update(head=True),
            "page": lambda r: r.update(host_page_size_bytes=12000),
            "entry rounding": lambda r: r.update(entries_physical_upper_bytes=8192),
            "entry zero": lambda r: r.update(entries_logical_bytes=0),
            "boolean number": lambda r: r.update(entries_logical_bytes=True),
            "match flag": lambda r: r.update(total_matches_declared_loader_budget=False),
            "input path": lambda r: r["input_manifest"].update(path="/other/manifest.json"),
            "input hash": lambda r: r["input_manifest"]["sha256"].__setitem__(0, 0),
            "layout not checked": lambda r: r.update(layout_validated=False),
            "physical destination": lambda r: r["destination_spans"].__setitem__(0, 0),
            "destination bool": lambda r: r["destination_spans"].__setitem__(0, True),
        }
        for label, mutation in mutations.items():
            with self.subTest(label=label):
                receipt = copy.deepcopy(original)
                mutation(receipt)
                with self.assertRaises(ValueError):
                    self.validate(manifest, receipt)

    def test_rejects_wrong_file_geometry(self):
        manifest, receipt = fixture()
        manifest["artifacts"][0]["keys"]["size_bytes"] -= 2
        with self.assertRaisesRegex(ValueError, "file size differs"):
            self.validate(manifest, receipt)

    def test_rejects_duplicate_coordinate(self):
        manifest, receipt = fixture()
        manifest["artifacts"][1]["coordinate"] = copy.deepcopy(manifest["artifacts"][0]["coordinate"])
        with self.assertRaisesRegex(ValueError, "not strictly ordered"):
            self.validate(manifest, receipt)

    def test_exact_json_rejects_nonfinite_duplicate_and_changed_bytes(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "receipt.json"
            for data in (b'{"x":1,"x":2}', b'{"x":NaN}', b'{"x":Infinity}'):
                path.write_bytes(data)
                with self.assertRaises(ValueError):
                    budget.read_exact_json(path, hashlib.sha256(data).hexdigest())
            path.write_text(json.dumps({"ok": True}))
            self.assertEqual(budget.read_exact_json(path, hashlib.sha256(path.read_bytes()).hexdigest()),
                             {"ok": True})
            with self.assertRaisesRegex(ValueError, "digest mismatch"):
                budget.read_exact_json(path, "00" * 32)


if __name__ == "__main__":
    unittest.main()
