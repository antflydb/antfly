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
import os
from pathlib import Path
import selectors
import struct
import subprocess
import sys
import tempfile
import time
import unittest

import quality_tokenizer_materialize as materialize


def sha(raw): return hashlib.sha256(raw).hexdigest()


class MaterializationContractTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.expected = {"session_id": "1" * 64, "request_index": 0, "case_id": "case-1",
                         "messages_sha256": "2" * 64, "request_content_sha256": "3" * 64,
                         "source_sha256": "4" * 64, "selected_part_sha256s": ["5" * 64],
                         "reference_sha256": "6" * 64, "qualified_context_tokens": 2048,
                         "reserved_output_tokens": 32, "settings_sha256": "7" * 64,
                         "variant_settings_sha256": "d" * 64,
                         "loaded_identity": {"load_generation": 1, "artifact_sha256": "8" * 64,
                            "tokenizer_sha256": "9" * 64, "template_sha256": "a" * 64,
                            "layout_sha256": "b" * 64, "implementation_sha256": "c" * 64}}
        self.case = {"case_id": self.expected["case_id"], "messages_file": "messages.json",
                     "messages_sha256": self.expected["messages_sha256"],
                     "request_content_sha256": self.expected["request_content_sha256"],
                     "source_sha256": self.expected["source_sha256"],
                     "selected_part_sha256s": self.expected["selected_part_sha256s"],
                     "reference_file": "reference.txt", "reference_sha256": self.expected["reference_sha256"],
                     "qualified_context_tokens": self.expected["qualified_context_tokens"],
                     "reserved_output_tokens": self.expected["reserved_output_tokens"],
                     "settings_sha256": self.expected["settings_sha256"],
                     "variant_settings_sha256": self.expected["variant_settings_sha256"]}
        prompt = [1, 2, 3]
        reference = [4, 5]
        self.refs = {}
        for name, raw, count in (
            ("rendered", b"abc", 3),
            ("rendered_labels", struct.pack("<III", 1, 1, 0), 3),
            ("prompt", struct.pack("<qqq", *prompt), 3),
            ("offsets", struct.pack("<IIIIII", 0, 1, 1, 2, 2, 3), 3),
            ("prompt_labels", struct.pack("<III", 1, 1, 0), 3),
            ("combined", struct.pack("<qqqqq", *(prompt + reference)), 5),
            ("reference", json.dumps(reference, separators=(",", ":")).encode(), 2)):
            path = self.root / name
            path.write_bytes(raw)
            self.refs[name] = {"path": name, "sha256": sha(raw), "count": count}
        self.report = {"schema_version": 2, "stage": "quality_tokenizer_materialization", "completed": True,
            "model_outputs_observed": False, "model_forward_steps": 0, **self.expected,
            "loaded_identity": dict(self.expected["loaded_identity"]),
            "rendered_prompt": self.refs["rendered"], "prompt_tokens": self.refs["prompt"],
            "prompt_token_offsets": self.refs["offsets"], "combined_tokens": self.refs["combined"],
            "rendered_labels": self.refs["rendered_labels"],
            "prompt_token_labels": self.refs["prompt_labels"],
            "reference_tokens": self.refs["reference"],
            "reference_sequence_sha256": materialize.reference_digest(reference),
            "stable_prefix_verified": True, "reference_roundtrip_verified": True}

    def test_accepts_raw_reconstructable_loaded_tokenizer_receipt(self):
        command = materialize.command(self.case, self.expected["session_id"], 0)
        self.assertEqual(command["command"], "materialize_quality_v1")
        self.assertNotIn("qualified_context_tokens", command)
        self.assertNotIn("reference_tokens_file", command)
        self.assertIs(materialize.report_contract(self.report, self.root, self.expected), self.report)

    def test_rejects_boundary_merge_and_unverifiable_claims(self):
        raw = struct.pack("<qqqqq", 1, 2, 9, 4, 5)
        (self.root / "combined").write_bytes(raw)
        self.report["combined_tokens"]["sha256"] = sha(raw)
        with self.assertRaisesRegex(ValueError, "preserve the prompt prefix"):
            materialize.report_contract(self.report, self.root, self.expected)
        self.report["stable_prefix_verified"] = False
        with self.assertRaises(ValueError):
            materialize.report_contract(self.report, self.root, self.expected)

    def test_rejects_one_token_reference_and_identity_tampering(self):
        reference = [4]
        raw = json.dumps(reference).encode()
        (self.root / "reference").write_bytes(raw)
        self.report["reference_tokens"] = {"path": "reference", "sha256": sha(raw), "count": 1}
        with self.assertRaisesRegex(ValueError, "reference continuation"):
            materialize.report_contract(self.report, self.root, self.expected)
        self.report["loaded_identity"]["artifact_sha256"] = "e" * 64
        with self.assertRaisesRegex(ValueError, "identity mismatch"):
            materialize.report_contract(self.report, self.root, self.expected)

    def test_rejects_offset_count_context_overflow_and_boolean_integers(self):
        self.report["prompt_token_offsets"]["count"] = 2
        with self.assertRaisesRegex(ValueError, "offset count mismatch"):
            materialize.report_contract(self.report, self.root, self.expected)
        self.report["prompt_token_offsets"]["count"] = 3
        self.report["qualified_context_tokens"] = 34
        expected = dict(self.expected)
        expected["qualified_context_tokens"] = 34
        with self.assertRaisesRegex(ValueError, "exceeds qualified context"):
            materialize.report_contract(self.report, self.root, expected)
        self.report["qualified_context_tokens"] = self.expected["qualified_context_tokens"]
        self.report["schema_version"] = True
        with self.assertRaisesRegex(ValueError, "schema"):
            materialize.report_contract(self.report, self.root, self.expected)
        self.report["schema_version"] = 2
        self.report["request_index"] = False
        with self.assertRaisesRegex(ValueError, "request binding"):
            materialize.report_contract(self.report, self.root, self.expected)

    def test_rejects_nonregular_and_oversized_artifacts(self):
        fifo = self.root / "fifo"
        try:
            os.mkfifo(fifo)
        except PermissionError:
            self.skipTest("FIFO unavailable")
        self.report["rendered_prompt"] = {"path": "fifo", "sha256": "0" * 64, "count": 1}
        with self.assertRaisesRegex(ValueError, "must be regular"):
            materialize.report_contract(self.report, self.root, self.expected)
        self.report["rendered_prompt"] = self.refs["rendered"]
        huge = b"[" + b"1," * (materialize.MAX_REFERENCE_TOKENS * 6 + 10) + b"1]"
        (self.root / "reference").write_bytes(huge)
        self.report["reference_tokens"] = {"path": "reference", "sha256": sha(huge), "count": 2}
        with self.assertRaisesRegex(ValueError, "exceeds byte limit"):
            materialize.report_contract(self.report, self.root, self.expected)

    def test_persistent_collector_rejects_generation_change(self):
        collector = materialize.PersistentCollector(self.root, self.expected["session_id"],
                                                     self.expected["loaded_identity"])
        self.assertIs(collector.accept(self.report, self.case), self.report)
        second = dict(self.report)
        second["request_index"] = 1
        second["loaded_identity"] = dict(self.report["loaded_identity"])
        second["loaded_identity"]["load_generation"] = 2
        with self.assertRaisesRegex(ValueError, "identity mismatch"):
            collector.accept(second, self.case)

    def test_schema1_legacy_is_diagnostic_and_exactly_label_free(self):
        legacy = dict(self.report)
        legacy["schema_version"] = 1
        legacy.pop("rendered_labels")
        legacy.pop("prompt_token_labels")
        self.assertIs(materialize.report_contract(legacy, self.root, self.expected), legacy)
        legacy["rendered_labels"] = self.refs["rendered_labels"]
        with self.assertRaisesRegex(ValueError, "report fields"):
            materialize.report_contract(legacy, self.root, self.expected)

    def test_schema2_requires_both_authoritative_label_artifacts(self):
        for field in ("rendered_labels", "prompt_token_labels"):
            report = dict(self.report)
            report.pop(field)
            with self.subTest(field=field):
                with self.assertRaisesRegex(ValueError, "report fields"):
                    materialize.report_contract(report, self.root, self.expected)

    def test_rejects_label_count_mismatch(self):
        self.report["rendered_labels"]["count"] = 2
        with self.assertRaisesRegex(ValueError, "rendered labels"):
            materialize.report_contract(self.report, self.root, self.expected)
        self.report["rendered_labels"]["count"] = 3
        self.report["prompt_token_labels"]["count"] = 2
        with self.assertRaisesRegex(ValueError, "prompt token labels"):
            materialize.report_contract(self.report, self.root, self.expected)

    def test_rehashed_unknown_selected_label_is_rejected(self):
        for field, name in (("rendered_labels", "rendered_labels"),
                            ("prompt_token_labels", "prompt_labels")):
            original = (self.root / name).read_bytes()
            raw = struct.pack("<III", 1, 0xffffffff, 0)
            (self.root / name).write_bytes(raw)
            self.report[field]["sha256"] = sha(raw)
            with self.subTest(field=field):
                with self.assertRaisesRegex(ValueError, "label exceeds selected part count"):
                    materialize.report_contract(self.report, self.root, self.expected)
            (self.root / name).write_bytes(original)
            self.report[field]["sha256"] = sha(original)

    def test_persistent_collector_runs_one_loaded_process_and_writes_journal(self):
        driver = self.root / "driver.py"
        driver.write_text(
            "import json,sys\n"
            "report=" + repr(self.report) + "\n"
            "for line in sys.stdin:\n"
            " command=json.loads(line)\n"
            " if command['command']=='stop': break\n"
            " report['request_index']=command['request_index']\n"
            " print(json.dumps(report,separators=(',',':')),flush=True)\n",
            encoding="utf-8")
        journal = self.root / "journal.jsonl"
        reports = materialize.collect(self.root, [self.case], [sys.executable, str(driver)],
                                      journal, self.expected["session_id"],
                                      self.expected["loaded_identity"])
        self.assertEqual(len(reports), 1)
        self.assertEqual(json.loads(journal.read_text()), self.report)
        self.assertEqual(Path(str(journal) + ".stderr").read_bytes(), b"")

    def test_silent_child_times_out_and_writes_terminal_failure(self):
        driver = self.root / "silent.py"
        driver.write_text("import time\ntime.sleep(30)\n", encoding="utf-8")
        journal = self.root / "silent.jsonl"
        started = time.monotonic()
        with self.assertRaisesRegex(ValueError, "timed out"):
            materialize.collect(self.root, [self.case], [sys.executable, str(driver)],
                                journal, self.expected["session_id"], self.expected["loaded_identity"],
                                request_timeout_seconds=0.05, total_timeout_seconds=1)
        self.assertLess(time.monotonic() - started, 3)
        rows = [json.loads(line) for line in journal.read_text().splitlines()]
        self.assertEqual(rows, [{"accepted_reports": 0, "completed": False, "error": "ValueError",
                                 "message": "materialization driver request timed out",
                                 "schema_version": 1,
                                 "stage": "quality_tokenizer_materialization_failure"}])

    def test_timeout_limits_reject_nonfinite_and_unbounded_before_launch(self):
        journal = self.root / "invalid-timeout.jsonl"
        for field, maximum in (("request_timeout_seconds", materialize.MAX_REQUEST_TIMEOUT_SECONDS),
                               ("total_timeout_seconds", materialize.MAX_TOTAL_TIMEOUT_SECONDS)):
            for value in (float("inf"), float("nan"), 0, -1, True, maximum + 1):
                with self.subTest(field=field, value=value):
                    with self.assertRaisesRegex(ValueError, "invalid materialization timeout"):
                        materialize.collect(self.root, [self.case], [sys.executable, "-c", "pass"],
                                            journal, self.expected["session_id"],
                                            self.expected["loaded_identity"], **{field: value})
                    self.assertFalse(journal.exists())

    def test_request_bounds_reject_unrepresentable_context_and_index(self):
        for index in (-1, True, materialize.MAX_MATERIALIZATION_CASES):
            with self.assertRaisesRegex(ValueError, "invalid materialization request"):
                materialize.command(self.case, self.expected["session_id"], index)
        for context in (True, 0, self.case["reserved_output_tokens"],
                        materialize.MAX_PROMPT_TOKENS + 1):
            with self.assertRaisesRegex(ValueError, "invalid materialization context limits"):
                materialize.command(dict(self.case, qualified_context_tokens=context),
                                    self.expected["session_id"], 0)

    def test_nonreading_child_cannot_block_large_nonblocking_write(self):
        child = subprocess.Popen(
            [sys.executable, "-c", "import time; time.sleep(30)"],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            start_new_session=True, bufsize=0)
        selector = selectors.DefaultSelector()
        selector.register(child.stdout.fileno(), selectors.EVENT_READ, "stdout")
        selector.register(child.stderr.fileno(), selectors.EVENT_READ, "stderr")
        stderr_path = self.root / "write.stderr"
        with stderr_path.open("xb") as stderr:
            with self.assertRaisesRegex(ValueError, "timed out"):
                materialize._write_command(child, selector, b"x" * (1024 * 1024), stderr, 0,
                                           time.monotonic() + 0.05)
        materialize._terminate_group(child, force=True)
        child.stdin.close(); child.stdout.close(); child.stderr.close(); selector.close()

    def test_preexisting_journal_does_not_create_or_leak_stderr(self):
        journal = self.root / "exists.jsonl"
        journal.write_text("sealed\n", encoding="utf-8")
        with self.assertRaises(FileExistsError):
            materialize.collect(self.root, [self.case], [sys.executable, "-c", "pass"],
                                journal, self.expected["session_id"], self.expected["loaded_identity"])
        self.assertFalse(Path(str(journal) + ".stderr").exists())

    def test_partial_success_is_fsynced_before_duplicate_json_failure(self):
        driver = self.root / "partial.py"
        driver.write_text(
            "import json,sys\n"
            "report=" + repr(self.report) + "\n"
            "for line in sys.stdin:\n"
            " command=json.loads(line)\n"
            " if command['command']=='stop': break\n"
            " if command['request_index']==0:\n"
            "  report['request_index']=0; print(json.dumps(report,separators=(',',':')),flush=True)\n"
            " else:\n"
            "  print('{\"schema_version\":1,\"schema_version\":1}',flush=True)\n",
            encoding="utf-8")
        journal = self.root / "partial.jsonl"
        with self.assertRaisesRegex(ValueError, "duplicate materialization driver JSON field"):
            materialize.collect(self.root, [self.case, self.case], [sys.executable, str(driver)],
                                journal, self.expected["session_id"], self.expected["loaded_identity"],
                                request_timeout_seconds=1, total_timeout_seconds=3)
        rows = [json.loads(line) for line in journal.read_text().splitlines()]
        self.assertEqual(rows[0], self.report)
        self.assertEqual(rows[1]["accepted_reports"], 1)
        self.assertFalse(rows[1]["completed"])


if __name__ == '__main__': unittest.main()
