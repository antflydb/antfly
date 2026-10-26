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

"""Synthetic persistent-collector tests; these runs are not quality proof."""
import copy
import hashlib
import json
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import quality_context_qualify as quality
import release_attention_matching as release


def digest(value):
    return hashlib.sha256(value).hexdigest()


class ReleaseQualityCollectorTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.inputs = self.root / "inputs"
        self.inputs.mkdir()
        self.identity = {key: digest(key.encode()) for key in release.SEMANTIC_BINDINGS}
        self.identity.update(dtype="f16_kv_f32_bias", kernel_route="metal_context_optimized")
        self.origin = {key: digest(("origin-" + key).encode()) for key in release.PROVENANCE_BINDINGS}
        self.origin["binary_sha256"] = release.sha(Path(sys.executable).resolve())
        self.loaded = {key + "_sha256": self.identity[key] for key in release.SEMANTIC_BINDINGS}
        self.loaded["load_generation"] = 9
        cases = []
        for index, bucket in enumerate(sorted(release.BUCKETS)):
            directory = self.inputs / f"case-{index}"
            directory.mkdir()
            document = f"independent document {index}".encode()
            question = f"sealed evaluation question {index}".encode()
            contents = {"document": document, "question": question,
                        "prompt": document + b"\n" + question,
                        "reference": "café\n".encode(),
                        "reference_tokens": json.dumps([1, 200 + index]).encode()}
            paths = {}
            for name, raw in contents.items():
                path = directory / (name + (".json" if name == "reference_tokens" else ".txt"))
                path.write_bytes(raw)
                paths[name] = path
            cases.append({"id": f"case-{index}", "bucket": bucket,
                          "document_file": str(paths["document"].relative_to(self.inputs)),
                          "document_sha256": digest(contents["document"]),
                          "question_file": str(paths["question"].relative_to(self.inputs)),
                          "question_sha256": digest(contents["question"]),
                          "request_content_sha256": release._coverage_structured_request_identity(
                              contents["document"], contents["question"]),
                          "prompt_file": str(paths["prompt"].relative_to(self.inputs)),
                          "prompt_sha256": digest(contents["prompt"]),
                          "prompt_tokens": 4092, "reserved_output_tokens": 4,
                          "reference_file": str(paths["reference"].relative_to(self.inputs)),
                          "reference_sha256": digest(contents["reference"]),
                          "reference_tokens_file": str(paths["reference_tokens"].relative_to(self.inputs)),
                          "reference_tokens_sha256": digest(contents["reference_tokens"]),
                          "scorer": quality.SCORER, "route": "native",
                          "settings_sha256": digest(b"generation-settings"),
                          "variant_settings_sha256": {
                              variant: digest(variant.encode()) for variant in quality.VARIANTS}})
        self.plan = {"schema_version": 3, "qualified_context_tokens": [4096], "cases": cases}
        self.run = self.root / "run"
        for target, value in (("group_rss_mib", 0), ("memory_free_percent", 50), ("swapout_bytes", 0)):
            mock = patch.object(release, target, return_value=value)
            mock.start()
            self.addCleanup(mock.stop)
        disk = patch.object(release.shutil, "disk_usage", return_value=SimpleNamespace(free=20 * 1024**3))
        disk.start()
        self.addCleanup(disk.stop)
        minimum = patch.object(release, "MIN_QUALITY_CASES", 1)
        minimum.start()
        self.addCleanup(minimum.stop)

    def command(self, elapsed_ns=1000):
        base = {"schema_version": 2, "semantic_identity": self.identity, "provenance": self.origin,
                "loaded_identity": self.loaded, "completed": True, "exact_fallback": False,
                "elapsed_ns": elapsed_ns, "committed_before_bytes": 1000, "context_peak_bytes": 1200,
                "live_bytes_before": 10, "live_bytes_after_release": 10,
                "context_allocation_scope": release.CONTEXT_ALLOCATION_SCOPE,
                "context_allocation_scope_version": release.CONTEXT_ALLOCATION_SCOPE_VERSION,
                "context_allocation_scope_complete": True, "full_owner_physical_available": True,
                "model_execution_pool_admission_version": release.CONTEXT_EXECUTION_POOL_ADMISSION_VERSION,
                "model_execution_pool_admitted_backend_bytes": release.CONTEXT_EXECUTION_POOL_ADMITTED_BACKEND_BYTES,
                "model_execution_pool_shared_residency": True, "owner_released": True,
                "owner_post_release_live_bytes": 0, "backend_kv_readback_bytes": 0}
        code = (
            "import sys,json,hashlib,struct,pathlib\n"
            "base=json.loads(" + repr(json.dumps(base)) + ")\n"
            "for line in sys.stdin:\n"
            " c=json.loads(line)\n"
            " if c['command']=='stop': break\n"
            " root=pathlib.Path(__import__('os').environ['ANTFLY_CONTEXT_CAMPAIGN_DIR'])\n"
            " assert c.get('prompt_file') and (root/c['prompt_file']).is_file()\n"
            " assert hashlib.sha256((root/c['document_file']).read_bytes()).hexdigest()==c['document_sha256']\n"
            " assert hashlib.sha256((root/c['question_file']).read_bytes()).hexdigest()==c['question_sha256']\n"
            " tids=json.loads((root/c['reference_tokens_file']).read_text())\n"
            " out=[1,2]; decoded='café\\n'\n"
            " r={**base,**c,'published':c['variant']=='attention_matching',"
            "'committed_after_bytes':700 if c['variant']=='attention_matching' else 1000,"
            "'committed_context_bytes':700 if c['variant']=='attention_matching' else 1000,"
            "'output_token_ids':out,'output_sha256':hashlib.sha256(b''.join(struct.pack('<I',x) for x in out)).hexdigest(),"
            "'output_utf8':decoded,'output_utf8_sha256':hashlib.sha256(decoded.encode()).hexdigest(),"
            "'reference_sequence_sha256':hashlib.sha256(b''.join(struct.pack('<I',x) for x in tids)).hexdigest(),"
            "'continuation_forward_steps':len(tids)-1,"
            "'published_compaction_forward_steps':len(tids)-1 if c['variant']=='attention_matching' else 0,"
            "'teacher_forced':[{'sequence_position':i,'token_id':x,'log_probability':p,"
            "'conditioned_on_token_id':tids[i-1] if i else None,"
            "'source':'prefill_final_row' if i==0 else 'published_compacted_context' if c['variant']=='attention_matching' else 'exact_context_continuation'}"
            " for i,(x,p) in enumerate(zip(tids,[-.1,-.2]))]}\n"
            " print(json.dumps(r),flush=True)\n")
        return [sys.executable, "-c", code]

    def collect(self, elapsed_ns=1000):
        return release.collect_quality_v2(self.command(elapsed_ns), self.identity, self.origin,
                                          self.plan, self.inputs, self.run, timeout=5, max_rss_mib=256)

    def raw(self):
        session = self.run / "session.json"
        return {"session": {"path": str(session.relative_to(self.root)), "sha256": release.sha(session)}}

    def audit(self):
        return release.raw_quality_contract(self.raw(), self.root, self.identity, self.origin, [4096])

    def resign(self, changed):
        session_path = self.run / "session.json"
        session = release.read_json(session_path)
        changed(session)
        session_path.write_text(json.dumps(session, indent=2, sort_keys=True) + "\n")

    def test_collects_raw_pairs_and_reconstructs_quality(self):
        receipt = self.collect()
        self.assertTrue(receipt["completed"])
        result = self.audit()
        self.assertTrue(result["diagnostic_only"])
        self.assertEqual(len(result["cases"]), len(release.BUCKETS))
        for case in result["cases"]:
            self.assertTrue(case["exact_correct"] and case["candidate_correct"])
            self.assertAlmostEqual(case["exact_nll_sum"], .3)
            self.assertEqual(case["swapout_growth_bytes"], 0)

    def test_rehashed_plan_and_report_order_tampering_are_rejected(self):
        self.collect()
        plan_path = self.run / "request-plan.json"
        plan = release.read_json(plan_path)
        plan["cases"].reverse()
        plan_path.write_text(json.dumps(plan, indent=2, sort_keys=True) + "\n")
        self.resign(lambda session: session["files"]["plan"].update(sha256=release.sha(plan_path)))
        with self.assertRaisesRegex(ValueError, "command/plan mismatch"):
            self.audit()

        other = self.root / "order-run"
        self.run = other
        self.collect()
        journal_path = self.run / "events.jsonl"
        rows = list(release.journal_records(journal_path))
        reports = [row for row in rows if row["event"] == "report"]
        reports[0]["file"], reports[1]["file"] = reports[1]["file"], reports[0]["file"]
        journal_path.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in rows))
        self.resign(lambda session: session["files"]["journal"].update(sha256=release.sha(journal_path)))
        with self.assertRaisesRegex(ValueError, "request mismatch|stdout/report mismatch"):
            self.audit()

    def test_extra_planned_command_is_deliberately_rejected(self):
        self.collect()
        journal_path = self.run / "events.jsonl"
        rows = list(release.journal_records(journal_path))
        commands = [row for row in rows if row["event"] == "command"]
        extra = dict(commands[0])
        extra["request_index"] = len(commands)
        extra["elapsed_ns"] = next(row["elapsed_ns"] for row in rows if row["event"] == "stop") - 1
        stop = next(index for index, row in enumerate(rows) if row["event"] == "stop")
        rows.insert(stop, extra)
        journal_path.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in rows))
        self.resign(lambda session: session["files"]["journal"].update(sha256=release.sha(journal_path)))
        with self.assertRaisesRegex(ValueError, "extra command"):
            self.audit()

    def test_campaign_reference_mutation_is_rejected(self):
        self.collect()
        plan = release.read_json(self.run / "request-plan.json")
        reference = self.run / plan["cases"][0]["reference_file"]
        reference.write_text("changed answer")
        with self.assertRaisesRegex(ValueError, "reference_file digest mismatch"):
            self.audit()

    def test_child_file_limit_policy_tampering_is_rejected(self):
        self.collect()
        self.resign(lambda session: session["limits"]["child_file_limit_policy"].update(bytes=1))
        with self.assertRaisesRegex(ValueError, "invalid child file limit policy"):
            self.audit()

    def test_missing_child_file_limit_policy_is_rejected(self):
        self.collect()
        self.resign(lambda session: session["limits"].pop("child_file_limit_policy"))
        with self.assertRaisesRegex(ValueError, "invalid raw soak resource limits"):
            self.audit()

    def test_parent_resource_failure_is_not_overridden_by_child(self):
        self.collect()
        journal_path = self.run / "events.jsonl"
        rows = list(release.journal_records(journal_path))
        next(row for row in rows if row["event"] == "resource")["free_percent"] = 14
        journal_path.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in rows))
        self.resign(lambda session: session["files"]["journal"].update(sha256=release.sha(journal_path)))
        with self.assertRaisesRegex(ValueError, "resource guard failed"):
            self.audit()

    def test_child_elapsed_time_must_fit_parent_clock(self):
        self.collect(elapsed_ns=10**18)
        with self.assertRaisesRegex(ValueError, "child timing exceeds parent duration"):
            self.audit()


if __name__ == "__main__":
    unittest.main()
