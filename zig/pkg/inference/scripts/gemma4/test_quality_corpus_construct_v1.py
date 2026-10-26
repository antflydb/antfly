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
import tempfile
import unittest
from pathlib import Path

import quality_corpus_construct_v1 as construct


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


class QualityCorpusConstructTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        sources = []
        for index in range(4):
            raw = (f"const ITEM_{index} = {index + 10}\n"
                   f"setting_{index}: value_{index}\n"
                   f"first independently sourced statement number {index}\n"
                   f"replacement independently sourced statement number {index}\n").encode()
            path = f"sources/source-{index}.zig"
            target = self.root / path
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(raw)
            sources.append({"family_id": f"family-{index}", "family_sha256": sha(f"family-{index}".encode()),
                            "repository": "local://antfly", "revision": "%040x" % 1,
                            "path": path, "sha256": sha(raw), "license": "Apache-2.0",
                            "content_type": "code", "authorship": "generated" if index == 0 else "human",
                            "normalized_sha256": sha(construct.prepare.normalized(raw).encode()),
                            "clone_cluster_id": f"cluster-{index}", "partition": "quality_evaluation"})
        self.manifest = {"schema_version": 1, "stage": "quality_source_manifest",
                         "model": construct.prepare.MODELS[0], "selection_seed": sha(b"manifest seed"),
                         "snapshot_sha256": sha(b"local source snapshot"),
                         "selection_source_families": [], "fitting_source_families": [],
                         "calibration_source_families": [], "sources": sources}
        raw = json.dumps(self.manifest, sort_keys=True, separators=(",", ":")).encode()
        self.manifest_path = self.root / "sources.json"
        self.manifest_path.write_bytes(raw)
        self.ref = {"path": "sources.json", "sha256": sha(raw)}
        self.seed = sha(b"fixed construction seed")

    def test_constructs_balanced_deterministic_source_backed_tasks_without_threshold(self):
        first = construct.construct(self.ref, self.root, construct.prepare.MODELS[0], self.seed, 4)
        second = construct.construct(self.ref, self.root, construct.prepare.MODELS[0], self.seed, 4)
        self.assertEqual(first, second)
        self.assertFalse(first["model_outputs_observed"])
        self.assertIsNone(first["acceptance_threshold"])
        self.assertTrue(first["diagnostic_only"])
        self.assertFalse(first["production_quality_coverage"])
        self.assertEqual(len(first["cases"]), 4 * len(construct.BUCKETS))
        for bucket in construct.BUCKETS:
            rows = [row for row in first["cases"] if row["bucket"] == bucket]
            self.assertEqual(len({row["primary_source_family"] for row in rows}), 4)
            self.assertEqual([row["intended_compactable_quartile"] for row in rows], [0, 1, 2, 3])
            for row in rows:
                raw = (self.root / row["source"]["path"]).read_bytes()
                evidence = raw[row["source"]["byte_start"]:row["source"]["byte_end"]]
                self.assertEqual(sha(evidence), row["evidence_sha256"])
        by_bucket = {bucket: next(row for row in first["cases"] if row["bucket"] == bucket)
                     for bucket in construct.BUCKETS}
        self.assertEqual(by_bucket["code"]["oracle"], "bounded_integer_expression_v1")
        self.assertTrue(by_bucket["code"]["reference_utf8"].isdigit())
        self.assertEqual(by_bucket["document_qa"]["oracle"], "source_scalar_lookup_v1")
        self.assertTrue(by_bucket["structured_tool"]["reference_utf8"].startswith("{"))
        self.assertEqual(len(by_bucket["multiturn"]["messages"]), 3)
        self.assertIn("bytes [", by_bucket["exact_retrieval"]["question_utf8"])
        generated = [row for row in first["cases"] if row["primary_source_family"] == "family-0"]
        self.assertTrue(generated and all(row["authorship"] == "generated" for row in generated))

    def test_rejects_evaluation_family_declared_in_fitting(self):
        value = json.loads(self.manifest_path.read_text())
        value["fitting_source_families"] = ["family-0"]
        raw = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
        self.manifest_path.write_bytes(raw)
        with self.assertRaisesRegex(ValueError, "overlapping source family"):
            construct.construct({"path": "sources.json", "sha256": sha(raw)}, self.root,
                                construct.prepare.MODELS[0], self.seed, 4)

    def test_cli_writes_new_bounded_blueprint_artifact(self):
        output = self.root / "blueprints.json"
        self.assertEqual(0, construct.main(["--source-manifest", str(self.manifest_path),
                                            "--root", str(self.root), "--model", construct.prepare.MODELS[0],
                                            "--selection-seed", self.seed, "--output", str(output),
                                            "--cases-per-bucket", "4"]))
        value = json.loads(output.read_text())
        self.assertTrue(value["completed"])
        with self.assertRaisesRegex(ValueError, "new file"):
            construct.main(["--source-manifest", str(self.manifest_path), "--root", str(self.root),
                            "--model", construct.prepare.MODELS[0], "--selection-seed", self.seed,
                            "--output", str(output), "--cases-per-bucket", "4"])


if __name__ == "__main__":
    unittest.main()
