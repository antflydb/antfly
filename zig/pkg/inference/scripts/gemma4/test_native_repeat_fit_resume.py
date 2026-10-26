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
from unittest import mock

import native_repeat_fit_resume as resume


class NativeRepeatFitResumeTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.output = self.root / "campaign"
        self.coverage = self.output / "coverage"
        self.coverage.mkdir(parents=True)
        self.capture = self.output / "capture-phase.json"
        self.capture.write_text("capture\n")
        self.authority = self.coverage / "training-authority.json"
        self.authority.write_text("authority\n")
        self.binary = self.root / "context-tests"
        self.binary.write_bytes(b"binary")
        self.spec_sha = "a" * 64
        self.coordinates = [(layer, head) for layer in (5, 11, 17, 23)
                            for head in range(2)]
        self.spec = {"expected_owners": [
            {"owner_layer": layer, "source_heads": 2, "head_dim": 512}
            for layer in (5, 11, 17, 23)]}
        self.context = {"output": self.output, "coverage": self.coverage,
            "capture_path": self.capture,
            "auth_path": self.authority,
            "auth_ref": self.ref(self.authority),
            "binding": {}, "build": {"binary_path": self.binary},
            "coordinates": self.coordinates}

    def tearDown(self):
        self.temporary.cleanup()

    @staticmethod
    def sha(path):
        return hashlib.sha256(Path(path).read_bytes()).hexdigest()

    def ref(self, path, with_bytes=False):
        value = {"path": str(path), "sha256": self.sha(path)}
        if with_bytes:
            value["bytes"] = Path(path).stat().st_size
        return value

    def manifest(self, path):
        value = {"source_rows": 3073, "head_dim": 512,
            "training_rows": 24, "validation_rows": 8, "consumer_groups": 4,
            "prefix_rows": 1, "recent_rows": 2048, "continuation_rows": 32,
            "retained_row_cap": 6144}
        Path(str(path) + ".json").write_text(json.dumps(value))
        return value

    def accepted_report(self, input_path, candidate_path):
        rows = 2305
        candidate_path.write_bytes(bytes(rows * (4 * 512 + 4)))
        candidate_sha = self.sha(candidate_path)
        metrics = {"version": 2, "queries": 8, "consumer_groups": 4,
            "max_mass_error": .01, "output_rmse": .02,
            "max_consumer_output_rmse": .03}
        return {"schema": "antfly.context.native.head-qualification.v2",
            "accepted_head": True, "input_sha256": self.sha(input_path),
            "attempts": [{"divisor": 4, "retained_rows": rows,
                          "outcome": "accepted_head", "metrics": metrics,
                          "reference": dict(metrics)}],
            "candidate_sha256": candidate_sha,
            "candidate_bytes": candidate_path.stat().st_size}

    def fake_run(self, command, resource, limits, numerical=False):
        resource = Path(resource)
        resource.write_text(json.dumps({"command": command, "returncode": 0}))
        resource.with_suffix(".stdout").write_text("qualified\n")
        input_path = Path(command[2].split("=", 1)[1])
        output = Path(command[4].split("=", 1)[1])
        report_path = output.with_suffix(".json")
        candidate_path = output.with_suffix(".candidate")
        report = self.accepted_report(input_path, candidate_path)
        report_path.write_text(json.dumps(report))
        marker = ("context_native_head_artifacts: report_sha256=" + self.sha(report_path) +
                  " candidate_sha256=" + report["candidate_sha256"] +
                  " candidate_bytes=" + str(report["candidate_bytes"]))
        resource.with_suffix(".stderr").write_text(marker + "\n")
        return {"returncode": 0}

    def fake_prepare(self, **kwargs):
        output = Path(kwargs["output"])
        output.write_bytes(b"AMHEAD02 fixture")
        return self.manifest(output)

    def fake_report(self, report_path, input_path, returncode, owner, head, manifest):
        self.assertEqual(returncode, 0)
        return json.loads(Path(report_path).read_text())

    def fake_artifacts(self, report, report_path, candidate_path, stderr_path):
        self.assertIn("context_native_head_artifacts:", Path(stderr_path).read_text())
        self.assertEqual(self.sha(candidate_path), report["candidate_sha256"])
        return {"report_sha256": self.sha(report_path),
                "candidate_sha256": report["candidate_sha256"],
                "candidate_bytes": report["candidate_bytes"]}

    def patches(self):
        return (mock.patch.object(resume, "_context", return_value=self.context),
                mock.patch.object(resume.preparer, "prepare", side_effect=self.fake_prepare),
                mock.patch.object(resume.campaign, "_run", side_effect=self.fake_run),
                mock.patch.object(resume.campaign, "_qualification_report",
                                  side_effect=self.fake_report),
                mock.patch.object(resume.campaign, "validate_head_artifacts",
                                  side_effect=self.fake_artifacts))

    def run_one(self, owner, head, attempt):
        patches = self.patches()
        with patches[0], patches[1], patches[2], patches[3], patches[4]:
            return resume.run_head(self.spec, self.spec_sha, self.output,
                                   owner, head, attempt)

    def test_runs_only_explicit_representative_head_and_preserves_retry_directory(self):
        value = self.run_one(23, 0, "000001")
        self.assertTrue(value["accepted_head"])
        selected = self.coverage / "fit-attempts/owner23-head0/attempt-000001"
        self.assertTrue((selected / "seal.json").is_file())
        self.assertFalse((self.coverage / "fit-attempts/owner5-head0").exists())
        with self.assertRaisesRegex(ValueError, "already exists"):
            self.run_one(23, 0, "000001")
        retry = self.run_one(23, 0, "000002")
        self.assertTrue(retry["accepted_head"])
        self.assertTrue(selected.is_dir())

    def test_operational_partial_is_preserved_but_not_reusable(self):
        def fail(command, resource, limits, numerical=False):
            Path(resource).write_text("partial resource")
            raise RuntimeError("timeout")
        with mock.patch.object(resume, "_context", return_value=self.context), \
             mock.patch.object(resume.preparer, "prepare", side_effect=self.fake_prepare), \
             mock.patch.object(resume.campaign, "_run", side_effect=fail):
            with self.assertRaisesRegex(RuntimeError, "timeout"):
                resume.run_head(self.spec, self.spec_sha, self.output, 23, 0, "000003")
        partial = self.coverage / "fit-attempts/owner23-head0/attempt-000003"
        self.assertTrue((partial / "input.bin").is_file())
        self.assertTrue((partial / "resource.json").is_file())
        self.assertFalse((partial / "seal.json").exists())
        with mock.patch.object(resume, "_context", return_value=self.context):
            with self.assertRaisesRegex((ValueError, FileNotFoundError), "seal|file"):
                resume.validate_head(self.spec, self.spec_sha, self.output,
                                     23, 0, "000003", "0" * 64)

    def test_numerical_rejection_is_sealed_diagnostic_but_never_reusable(self):
        def reject_run(command, resource, limits, numerical=False):
            resource = Path(resource)
            resource.write_text("resource")
            resource.with_suffix(".stdout").write_text("")
            resource.with_suffix(".stderr").write_text("rejected\n")
            output = Path(command[4].split("=", 1)[1])
            output.with_suffix(".json").write_text("{}")
            return {"returncode": 1}
        rejected = {"accepted_head": False, "attempts": [],
                    "candidate_sha256": None, "candidate_bytes": 0}
        with mock.patch.object(resume, "_context", return_value=self.context), \
             mock.patch.object(resume.preparer, "prepare", side_effect=self.fake_prepare), \
             mock.patch.object(resume.campaign, "_run", side_effect=reject_run), \
             mock.patch.object(resume.campaign, "_qualification_report",
                               return_value=rejected), \
             mock.patch.object(resume.campaign, "validate_head_artifacts",
                               return_value={"report_sha256": "1" * 64,
                                   "candidate_sha256": None, "candidate_bytes": 0}):
            value = resume.run_head(self.spec, self.spec_sha, self.output,
                                    23, 1, "000004")
        self.assertFalse(value["accepted_head"])
        seal_path = Path(value["seal"]["path"])
        self.assertTrue(seal_path.is_file())
        self.assertFalse(json.loads(seal_path.read_text())["accepted_head"])
        with mock.patch.object(resume, "_context", return_value=self.context):
            with self.assertRaisesRegex(ValueError, "non-accepted"):
                resume.validate_head(self.spec, self.spec_sha, self.output,
                    23, 1, "000004", value["seal"]["sha256"])

    def test_validate_rejects_tamper_even_with_attempt_still_present(self):
        value = self.run_one(5, 0, "000001")
        attempt = self.coverage / "fit-attempts/owner5-head0/attempt-000001"
        candidate = attempt / "result.candidate"
        candidate.write_bytes(candidate.read_bytes()[:-1] + b"x")
        with mock.patch.object(resume, "_context", return_value=self.context), \
             mock.patch.object(resume.profile, "_manifest",
                               return_value=self.manifest(attempt / "input.bin")), \
             mock.patch.object(resume.campaign, "_resource_contract",
                               return_value={"returncode": 0}), \
             mock.patch.object(resume.campaign, "_qualification_report",
                               side_effect=self.fake_report), \
             mock.patch.object(resume.campaign, "validate_head_artifacts",
                               side_effect=self.fake_artifacts):
            with self.assertRaisesRegex(ValueError, "SHA-256"):
                resume.validate_head(self.spec, self.spec_sha, self.output,
                    5, 0, "000001", value["seal"]["sha256"])

    def test_terminal_assembly_requires_all_eight_in_canonical_order(self):
        selections = []
        for index, (owner, head) in enumerate(self.coordinates, 1):
            value = self.run_one(owner, head, f"{index:06d}")
            selections.append({"owner_layer": owner, "kv_head": head,
                "attempt_id": value["attempt_id"],
                "seal_sha256": value["seal"]["sha256"]})
        with mock.patch.object(resume, "_context", return_value=self.context), \
             mock.patch.object(resume.profile, "_manifest",
                               side_effect=lambda path, *args: json.loads(Path(path).read_text())), \
             mock.patch.object(resume.campaign, "_resource_contract",
                               return_value={"returncode": 0}), \
             mock.patch.object(resume.campaign, "_qualification_report",
                               side_effect=self.fake_report), \
             mock.patch.object(resume.campaign, "validate_head_artifacts",
                               side_effect=self.fake_artifacts):
            with self.assertRaisesRegex(ValueError, "one explicit selection"):
                resume.assemble(self.spec, self.spec_sha, self.output, selections[:-1])
            with self.assertRaisesRegex(ValueError, "reordered"):
                resume.assemble(self.spec, self.spec_sha, self.output,
                                list(reversed(selections)))
            result = resume.assemble(self.spec, self.spec_sha, self.output, selections)
            repeated = resume.assemble(self.spec, self.spec_sha, self.output, selections)
        self.assertEqual(result["schema"], resume.RESULT_SCHEMA)
        self.assertEqual(repeated, result)
        self.assertEqual(result["fit_layout"], resume.FIT_LAYOUT)
        self.assertEqual(result["accepted_head_count"], 8)
        self.assertTrue((self.coverage / "results.json").is_file())

        changed = json.loads((self.coverage / "results.json").read_text())
        changed["accepted_head_count"] = 7
        (self.coverage / "results.json").write_text(json.dumps(changed))
        with mock.patch.object(resume, "_context", return_value=self.context), \
             mock.patch.object(resume.profile, "_manifest",
                               side_effect=lambda path, *args: json.loads(Path(path).read_text())), \
             mock.patch.object(resume.campaign, "_resource_contract",
                               return_value={"returncode": 0}), \
             mock.patch.object(resume.campaign, "_qualification_report",
                               side_effect=self.fake_report), \
             mock.patch.object(resume.campaign, "validate_head_artifacts",
                               side_effect=self.fake_artifacts):
            with self.assertRaisesRegex(ValueError, "differ"):
                resume.assemble(self.spec, self.spec_sha, self.output, selections)

    def test_attempt_id_is_fixed_width_and_not_path_syntax(self):
        for value in ("1", "0000001", "../001", "abc001", "000/01"):
            with self.subTest(value=value), self.assertRaisesRegex(ValueError, "six"):
                resume._attempt_id(value)


if __name__ == "__main__":
    unittest.main()
