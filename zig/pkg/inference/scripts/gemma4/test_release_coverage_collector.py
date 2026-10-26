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

"""Coverage-only parent collector regressions; fixtures never grant qualification."""
import copy
import errno
import hashlib
import json
import os
import resource
import signal
import subprocess
import sys
from pathlib import Path
import stat
import tempfile
import unittest
from unittest import mock

import release_attention_matching as release
import release_build_fixture


def digest(value):
    return hashlib.sha256(value.encode()).hexdigest()


DRIVER = r'''#!/usr/bin/env python3
import hashlib, json, os, pathlib, struct, sys
root = pathlib.Path(os.environ["ANTFLY_CONTEXT_CAMPAIGN_DIR"])
if "ANTFLY_CONTEXT_PROFILE" in os.environ or "ANTFLY_CONTEXT_PROFILE_SHA256" in os.environ:
    raise SystemExit("coverage bootstrap inherited a retention profile")
manifest = json.loads((root / os.environ["ANTFLY_CONTEXT_COVERAGE_MANIFEST"]).read_text())
for line in sys.stdin:
    command = json.loads(line)
    if command["command"] == "stop": break
    index, count = command["request_index"], command["prompt_tokens"] or 1024
    token_bytes = b"".join(struct.pack("<q", 7) for _ in range(count))
    token_sha = hashlib.sha256(token_bytes).hexdigest()
    question_start, question_end = count - 5, count - 1
    question_bytes = token_bytes[question_start * 8:question_end * 8]
    question_token_sha = hashlib.sha256(question_bytes).hexdigest()
    rendered_bytes = b"renderer-owned prompt bytes"
    rendered_sha = hashlib.sha256(rendered_bytes).hexdigest()
    offsets_bytes = struct.pack("<II", 0, len(rendered_bytes)) + b"\0" * ((count - 1) * 8)
    offsets_sha = hashlib.sha256(offsets_bytes).hexdigest()
    loaded = {name + "_sha256": os.environ["TEST_" + name.upper()] for name in
              ("artifact", "tokenizer", "template", "layout", "implementation")}
    loaded["load_generation"] = 1
    receipt = {"schema_version": 2, "stage": "context_payload_block_coverage",
      "request": {"session_id": command["session_id"], "request_index": index,
                  "question_sha256": command["question_sha256"],
                  "request_content_sha256": command["request_content_sha256"]},
      "document": {"id": command["case_id"], "sha256": command["document_sha256"], "bucket": command["bucket"]},
      "question": {"message_index": 0, "part_index": 1, "source_start": question_start,
                   "source_end": question_end, "token_count": question_end - question_start,
                   "tokens_sha256": question_token_sha},
      "qualified_context_tokens": count + command["reserved_output_tokens"], "prompt_tokens": count,
      "reserved_output_tokens": command["reserved_output_tokens"], "admitted_prefill_chunk_rows": 64,
      "prompt_tokens_sha256": token_sha, "prompt_token_offsets_sha256": offsets_sha,
      "rendered_prompt_sha256": rendered_sha,
      "policy": {"keep_prefix_tokens": 0, "keep_recent_tokens": 1, "block_tokens": 512,
                 "compaction_targets": [{"message_index": 0, "part_index": 0}]},
      "payload_parts": [{"message_index": 0, "part_index": 0, "label": 1, "token_start": 0, "token_end": count}],
      "eligible_token_labels": [1] * question_start + [0] * (count - question_start),
      "eligible_ranges": [{"message_index": 0, "part_index": 0, "label": 1, "start": 0, "end": question_start}],
      "eligible_blocks": [{"message_index": 0, "part_index": 0, "label": 1, "start": start, "end": start + 512}
                          for start in range(0, question_start - 511, 512)],
      "loaded_identity": loaded,
      "provenance": {"binary_sha256": command["binary_receipt_id"], "source_build_sha256": command["source_receipt_id"]},
      "completed": True, "cancelled": False, "elapsed_ns": 100, "status_readback_bytes": 4, "kv_readback_bytes": 0}
    receipt_bytes = json.dumps(receipt, sort_keys=True).encode()
    receipt_name, token_name = f"coverage-receipt-{index:06d}.json", f"coverage-prompt-{index:06d}.i64"
    question_name = f"coverage-question-{index:06d}.i64"
    offsets_name = f"coverage-prompt-offsets-{index:06d}.u32le"
    rendered_name = f"coverage-rendered-prompt-{index:06d}.txt"
    (root / receipt_name).write_bytes(receipt_bytes); (root / token_name).write_bytes(token_bytes)
    (root / question_name).write_bytes(question_bytes)
    (root / offsets_name).write_bytes(offsets_bytes)
    (root / rendered_name).write_bytes(rendered_bytes)
    fit_name, gate_name = f"coverage-fit-{index:06d}.i64", f"coverage-gate-{index:06d}.i64"
    fit_template_name, gate_template_name = "compiled-fit.txt", "compiled-gate.txt"
    contract_name = "compiled-generation.json"
    fit_bytes, gate_bytes = token_bytes[:64], token_bytes[64:128]
    for name, value in ((fit_name, fit_bytes), (gate_name, gate_bytes),
                        (fit_template_name, b"compiled fit"), (gate_template_name, b"compiled gate"),
                        (contract_name, b'{"mode":"greedy_fixed_length_ignore_eos"}')):
        path = root / name
        if not path.exists(): path.write_bytes(value)
    def ref(name):
        value = (root / name).read_bytes()
        return {"path": name, "sha256": hashlib.sha256(value).hexdigest()}
    def study(token_name, template_name):
        return {"tokens": {**ref(token_name), "count": 8}, "prompt_tokens_sha256": token_sha,
                "rendered_suffix_sha256": hashlib.sha256(template_name.encode()).hexdigest(),
                "template": ref(template_name), "generation_contract": ref(contract_name),
                "response_tokens": 8, "generation_mode": "greedy_fixed_length_ignore_eos"}
    capture = {"schema_version": 1, "stage": "context_numerical_capture_inputs",
      "request": receipt["request"], "coverage_receipt": ref(receipt_name),
      "rendered_prompt": ref(rendered_name),
      "prompt": {**ref(token_name), "count": count},
      "prompt_token_offsets": {**ref(offsets_name), "count": count},
      "question": {"tokens": {**ref(question_name), "count": question_end-question_start},
                   "source_start": question_start, "source_end": question_end},
      "fit": study(fit_name, fit_template_name), "gate": study(gate_name, gate_template_name),
      "loaded_identity": loaded, "provenance": receipt["provenance"], "completed": True,
      "diagnostic_only": command["prompt_tokens"] is None, "admitted_prefill_chunk_rows": 64}
    capture_bytes = json.dumps(capture, sort_keys=True).encode()
    capture_name = f"coverage-capture-inputs-{index:06d}.json"
    (root / capture_name).write_bytes(capture_bytes)
    print(json.dumps({"schema_version": 2, "completed": True, "operation": "coverage",
      "session_id": command["session_id"], "request_index": index,
      "diagnostic_only": command["prompt_tokens"] is None, "observed_prompt_tokens": count,
      "admitted_prefill_chunk_rows": 64,
      "receipt": {"path": receipt_name, "sha256": hashlib.sha256(receipt_bytes).hexdigest()},
      "prompt_tokens": {"path": token_name, "sha256": token_sha},
      "prompt_token_offsets": {"path": offsets_name, "sha256": offsets_sha},
      "question_tokens": {"path": question_name, "sha256": question_token_sha},
      "rendered_prompt": {"path": rendered_name, "sha256": rendered_sha},
      "capture_inputs": {"path": capture_name, "sha256": hashlib.sha256(capture_bytes).hexdigest()}}, sort_keys=True), flush=True)
'''


class CoverageCollectorTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name); self.inputs = self.root / "inputs"; self.inputs.mkdir()
        document, question = b"sealed document", b"question"
        (self.inputs / "document.txt").write_bytes(document); (self.inputs / "question.txt").write_bytes(question)
        self.identity = {key: digest(key) for key in release.SEMANTIC_BINDINGS}
        self.identity.update(dtype="f16_kv_f32_bias", kernel_route="metal_context_optimized")
        self.origin = {key: digest(key) for key in release.PROVENANCE_BINDINGS}
        self.driver = self.root / "driver.py"; self.driver.write_text(DRIVER)
        self.driver.chmod(self.driver.stat().st_mode | stat.S_IXUSR)
        self.origin["binary_sha256"] = release.sha(self.driver)
        self.receipt = self.root / "build.json"
        release_build_fixture.write(self.root, self.receipt, self.driver)
        self.origin["source_build_sha256"] = release.sha(self.receipt)
        self.plan = {"schema_version": 2,
          "policy": {"max_context_tokens": 1040, "max_kv_tokens": 1024, "keep_prefix_tokens": 0, "keep_recent_tokens": 1},
          "cases": [{"case_id": "doc-1", "document_file": "document.txt",
            "document_sha256": hashlib.sha256(document).hexdigest(), "bucket": "document_qa",
            "question_file": "question.txt", "question_sha256": hashlib.sha256(question).hexdigest(),
            "request_content_sha256": release._coverage_structured_request_identity(document, question),
            "prompt_tokens": 1024, "reserved_output_tokens": 16, "route": "native"}]}
        self.env = {"TEST_" + key.upper(): self.identity[key] for key in
                    ("artifact", "tokenizer", "template", "layout", "implementation")}
        self.env.update(ANTFLY_CONTEXT_PROFILE="stale.json", ANTFLY_CONTEXT_PROFILE_SHA256="0" * 64)

    def collect(self, name="campaign", origin=None, build_receipt=None):
        origin = self.origin if origin is None else origin
        build_receipt = self.receipt if build_receipt is None else build_receipt
        disk = type("D", (), {"free": 50 * 1048576 * 1024})()
        with mock.patch.dict(os.environ, self.env), mock.patch.object(release, "memory_free_percent", return_value=50), \
             mock.patch.object(release, "swapout_bytes", return_value=0), mock.patch.object(release, "group_rss_mib", return_value=0), \
             mock.patch.object(release.shutil, "disk_usage", return_value=disk):
            return release.collect_coverage_v2([str(self.driver)], self.identity, origin, self.plan,
                self.inputs, build_receipt, self.root / name, timeout=5, max_rss_mib=32)

    def test_child_file_limit_preserves_cap_and_converts_oversize_to_efbig(self):
        limit = 1024 * 1024
        child = r"""
import errno, json, os, resource, signal, sys
root, limit = sys.argv[1], int(sys.argv[2])
assert resource.getrlimit(resource.RLIMIT_FSIZE) == (limit, limit)
assert signal.getsignal(signal.SIGXFSZ) == signal.SIG_IGN
path = os.path.join(root, "bounded.bin")
fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_EXCL, 0o600)
try:
    try: os.ftruncate(fd, limit + 1); raise AssertionError("ftruncate exceeded cap")
    except OSError as error: assert error.errno == errno.EFBIG
    os.lseek(fd, limit, os.SEEK_SET)
    try: os.write(fd, b"x"); raise AssertionError("write exceeded cap")
    except OSError as error: assert error.errno == errno.EFBIG
    assert os.fstat(fd).st_size <= limit
    os.lseek(fd, 0, os.SEEK_SET); assert os.write(fd, b"under-limit") == 11
    os.fsync(fd)
finally: os.close(fd)
report = {"completed": True, "errno": errno.EFBIG, "size": os.stat(path).st_size}
with open(os.path.join(root, "report.json"), "x") as out:
    json.dump(report, out); out.write("\n"); out.flush(); os.fsync(out.fileno())
print(json.dumps(report))
"""
        with mock.patch.object(release, "MAX_JOURNAL_BYTES", limit):
            completed = subprocess.run([sys.executable, "-c", child, str(self.root), str(limit)],
                preexec_fn=release._limit_qualification_child_files,
                capture_output=True, text=True, timeout=10, check=True)
        report = json.loads(completed.stdout)
        self.assertEqual(report, json.loads((self.root / "report.json").read_text()))
        self.assertEqual(report, {"completed": True, "errno": errno.EFBIG, "size": 11})

        with mock.patch.object(release, "MAX_JOURNAL_BYTES", limit):
            bounded = subprocess.run(["/bin/dd", "if=/dev/zero", "of=" + str(self.root / "bounded-dd.bin"),
                                      "bs=" + str(limit + 1), "count=1"],
                                     preexec_fn=release._limit_qualification_child_files,
                                     capture_output=True, timeout=10)
        self.assertGreaterEqual(bounded.returncode, 1)
        self.assertNotEqual(bounded.returncode, -signal.SIGXFSZ)
        self.assertIn(b"File too large", bounded.stderr)
        self.assertLessEqual((self.root / "bounded-dd.bin").stat().st_size, limit)

        def default_limit():
            resource.setrlimit(resource.RLIMIT_FSIZE, (limit, limit))
        killed = subprocess.run(["/bin/dd", "if=/dev/zero", "of=" + str(self.root / "default.bin"),
                                 "bs=" + str(limit + 1), "count=1"],
                                preexec_fn=default_limit, capture_output=True, timeout=10)
        self.assertEqual(killed.returncode, -signal.SIGXFSZ)
        self.assertLessEqual((self.root / "default.bin").stat().st_size, limit)

    def test_coverage_receipt_records_child_file_limit_policy(self):
        result = self.collect("file-limit-policy")
        self.assertEqual(result["limits"]["child_file_limit_policy"], {
            "bytes": release.MAX_JOURNAL_BYTES,
            "sigxfsz_disposition": "ignored",
            "oversize_result": "EFBIG",
        })

    def test_structured_request_cross_language_golden_and_discovery_is_diagnostic(self):
        self.assertEqual(release._coverage_structured_request_identity(b"DOC", b"Q"),
                         "581cbb0a43f6f1b0ce63bf8c4fba8b33393fd4a1e15ddc55ce4bfee984fc3f9f")
        self.plan["cases"][0]["prompt_tokens"] = None
        result = self.collect("discovery")
        self.assertTrue(result["completed"]); self.assertTrue(result["diagnostic_only"])
        self.assertEqual(result["missing_gates"], ["sealed_expected_prompt_tokens"])
        self.assertEqual(result["coverage_receipts"], [])
        self.assertEqual(len(result["diagnostic_coverage_receipts"]), 1)
        self.assertEqual(result["observed_prompt_tokens"][0]["prompt_tokens"], 1024)

    def test_variant_settings_v2_cross_language_golden_binds_policy_and_implementation(self):
        policy = {"max_context_tokens": 1040, "max_kv_tokens": 1024,
                  "keep_prefix_tokens": 0, "keep_recent_tokens": 1}
        expected = "4ae5c9c7661822e0323f8800ee2758a66c27515dec29906b980f6d566150a27e"
        self.assertEqual(release._variant_settings_v2("packed_exact", policy, "11" * 32,
                                                      "00" * 32, "00" * 32, True, True), expected)
        for changed in (
            dict(policy, max_context_tokens=1041),
            dict(policy, max_kv_tokens=1023),
            dict(policy, keep_prefix_tokens=1),
            dict(policy, keep_recent_tokens=2),
        ):
            self.assertNotEqual(release._variant_settings_v2("packed_exact", changed, "11" * 32,
                                                              "00" * 32, "00" * 32, True, True), expected)
        self.assertNotEqual(release._variant_settings_v2("packed_exact", policy, "22" * 32,
                                                         "00" * 32, "00" * 32, True, True), expected)

    def test_collects_and_reconstructs_parent_bound_coverage(self):
        result = self.collect(); campaign = (self.root / "campaign").resolve()
        self.assertTrue(result["completed"]); self.assertFalse(result["serving_qualified"])
        self.assertEqual(result["trial_count"], 1); self.assertEqual(len(result["coverage_receipts"]), 1)
        events = list(release.journal_records(campaign / "events.jsonl"))
        self.assertEqual(events[0]["session_id"], result["session_id"])
        command = next(row for row in events if row["event"] == "command")
        self.assertEqual(command["request_index"], 0)
        self.assertEqual(command["request_content_sha256"], self.plan["cases"][0]["request_content_sha256"])
        coverage = result["coverage_receipts"][0]
        self.assertEqual(release.sha(campaign / coverage["receipt"]["path"]), coverage["receipt"]["sha256"])
        self.assertEqual(release.sha(campaign / coverage["prompt_tokens"]["path"]), coverage["prompt_tokens"]["sha256"])
        self.assertEqual(release.sha(campaign / coverage["question_tokens"]["path"]), coverage["question_tokens"]["sha256"])
        self.assertEqual(release.sha(campaign / coverage["rendered_prompt"]["path"]), coverage["rendered_prompt"]["sha256"])
        self.assertEqual(release.sha(campaign / coverage["capture_inputs"]["path"]), coverage["capture_inputs"]["sha256"])

    def test_report_rejects_inline_forgery_and_request_drift(self):
        result = self.collect(); campaign = (self.root / "campaign").resolve()
        report = release.read_json(campaign / "trial-000000.json")
        request = next(row for row in release.journal_records(campaign / "events.jsonl") if row["event"] == "command")
        with self.assertRaisesRegex(ValueError, "invalid coverage driver report"):
            release._coverage_report_contract(dict(report, receipt_json="{}"), campaign, request,
                                              self.identity, self.origin, result["session_id"], 0)
        receipt_path = campaign / report["receipt"]["path"]
        receipt = json.loads(receipt_path.read_text()); receipt["request"]["request_index"] = 1
        receipt_path.write_text(json.dumps(receipt)); report["receipt"]["sha256"] = release.sha(receipt_path)
        with self.assertRaisesRegex(ValueError, "request binding mismatch"):
            release._coverage_report_contract(report, campaign, request, self.identity, self.origin, result["session_id"], 0)
        receipt["request"]["request_index"] = 0
        receipt["loaded_identity"]["template_sha256"] = digest("forged-template")
        receipt_path.write_text(json.dumps(receipt)); report["receipt"]["sha256"] = release.sha(receipt_path)
        with self.assertRaisesRegex(ValueError, "coverage loaded identity mismatch"):
            release._coverage_report_contract(report, campaign, request, self.identity, self.origin, result["session_id"], 0)

    def test_question_tokens_must_be_the_attributed_prompt_slice(self):
        result = self.collect(); campaign = (self.root / "campaign").resolve()
        report = release.read_json(campaign / "trial-000000.json")
        request = next(row for row in release.journal_records(campaign / "events.jsonl") if row["event"] == "command")
        question_path = campaign / report["question_tokens"]["path"]
        question_path.write_bytes(b"\x08" + question_path.read_bytes()[1:])
        report["question_tokens"]["sha256"] = release.sha(question_path)
        with self.assertRaisesRegex(ValueError, "coverage receipt/request mismatch"):
            release._coverage_report_contract(report, campaign, request, self.identity, self.origin,
                                              result["session_id"], 0)

    def test_source_build_receipt_is_rehashed_and_binds_executed_binary(self):
        self.receipt.write_text(self.receipt.read_text() + " ")
        with self.assertRaisesRegex(ValueError, "coverage source build receipt mismatch"):
            self.collect("source-hash")
        forged = self.root / "forged-build.json"
        forged.write_text(json.dumps({"binary_sha256": digest("other-binary")}))
        origin = dict(self.origin, source_build_sha256=release.sha(forged))
        with self.assertRaisesRegex(ValueError, "historical, scoped"):
            self.collect("source-binary", origin=origin, build_receipt=forged)

    def test_plan_and_parent_resource_fail_closed(self):
        changed = copy.deepcopy(self.plan); changed["cases"][0]["question_sha256"] = digest("wrong")
        with self.assertRaisesRegex(ValueError, "coverage input identity mismatch"):
            release._coverage_plan_contract(changed, self.inputs)
        disk = type("D", (), {"free": 50 * 1048576 * 1024})()
        with mock.patch.dict(os.environ, self.env), mock.patch.object(release, "memory_free_percent", return_value=14), \
             mock.patch.object(release, "swapout_bytes", return_value=0), mock.patch.object(release, "group_rss_mib", return_value=0), \
             mock.patch.object(release.shutil, "disk_usage", return_value=disk):
            with self.assertRaisesRegex(ValueError, "coverage resource guard failed"):
                release.collect_coverage_v2([str(self.driver)], self.identity, self.origin, self.plan,
                    self.inputs, self.receipt, self.root / "resource-fail", timeout=5, max_rss_mib=32)
        partial = release.read_json(self.root / "resource-fail" / "session.json")
        self.assertFalse(partial["completed"]); self.assertEqual(partial["trial_count"], 0)

    def test_low_work_volume_rejects_abundant_coverage_volume_before_launch(self):
        work_dir = Path.cwd().resolve()

        def disk_usage(path):
            free = 4095 if Path(path).resolve() == work_dir else 50 * 1024
            return type("Disk", (), {"free": free * 1048576})()

        with mock.patch.dict(os.environ, self.env), \
             mock.patch.object(release, "memory_free_percent", return_value=80), \
             mock.patch.object(release, "swapout_bytes", return_value=0), \
             mock.patch.object(release.shutil, "disk_usage", side_effect=disk_usage), \
             mock.patch.object(release.subprocess, "Popen") as launch:
            with self.assertRaisesRegex(ValueError, "coverage resource guard failed"):
                release.collect_coverage_v2([str(self.driver)], self.identity, self.origin, self.plan,
                    self.inputs, self.receipt, self.root / "work-volume-low", timeout=5, max_rss_mib=32)
        launch.assert_not_called()
        partial = release.read_json(self.root / "work-volume-low" / "session.json")
        self.assertFalse(partial["completed"])
        self.assertEqual(partial["trial_count"], 0)
        resource = next(row for row in release.journal_records(self.root / "work-volume-low" / "events.jsonl")
                        if row["event"] == "resource")
        self.assertEqual(resource["disk_free_mib"], 4095)


if __name__ == "__main__": unittest.main()
