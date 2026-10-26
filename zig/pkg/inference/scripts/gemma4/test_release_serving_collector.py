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

"""Raw serving protocol regressions. Mock drivers never qualify production."""
import hashlib
import json
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import release_attention_matching as release


class ServingCollectorTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.identity = {key: hashlib.sha256(key.encode()).hexdigest() for key in release.SEMANTIC_BINDINGS}
        self.identity.update(dtype="f16_kv_f32_bias", kernel_route="metal_context_optimized")
        self.origin = {key: hashlib.sha256(key.encode()).hexdigest() for key in release.PROVENANCE_BINDINGS}
        self.origin["binary_sha256"] = release.sha(Path(sys.executable).resolve())
        loaded = {key + "_sha256": value for key, value in self.identity.items() if key in release.SEMANTIC_BINDINGS}
        loaded["load_generation"] = 7
        self.report = {"schema_version": 2, "semantic_identity": self.identity, "provenance": self.origin,
                       "published": True, "exact_fallback": False, "committed_before_bytes": 10000,
                       "committed_after_bytes": 7000, "backend_kv_readback_bytes": 0, "swapout_growth_bytes": 0,
                       "live_bytes_before": 100, "live_bytes_after_release": 100,
                       "context_allocation_scope": release.CONTEXT_ALLOCATION_SCOPE,
                       "context_allocation_scope_version": release.CONTEXT_ALLOCATION_SCOPE_VERSION,
                       "context_allocation_scope_complete": True, "full_owner_physical_available": True,
                       "model_execution_pool_admission_version": release.CONTEXT_EXECUTION_POOL_ADMISSION_VERSION,
                       "model_execution_pool_admitted_backend_bytes": release.CONTEXT_EXECUTION_POOL_ADMITTED_BACKEND_BYTES,
                       "model_execution_pool_shared_residency": True, "committed_context_bytes": 7000,
                       "context_peak_bytes": 10000, "owner_released": True,
                       "owner_post_release_live_bytes": 0,
                       "owner_measurement": {"available": True, "released_baseline_bytes": 0},
                       "prompt_tokens": 8160, "reserved_output_tokens": 32, "loaded_identity": loaded}
        for target, value in (("group_rss_mib", 0), ("memory_free_percent", 50), ("swapout_bytes", 0)):
            mock = patch.object(release, target, return_value=value)
            mock.start()
            self.addCleanup(mock.stop)
        disk = patch.object(release.shutil, "disk_usage", return_value=SimpleNamespace(free=20 * 1024**3))
        disk.start()
        self.addCleanup(disk.stop)

    def collect(self, directory="run", mutation="", duration=1.05, timeout=5):
        code = ("import sys,json,time\nr=json.loads(" + repr(json.dumps(self.report)) + ")\n"
                "for line in sys.stdin:\n c=json.loads(line)\n if c['command']=='stop': break\n"
                " r.update(session_id=c['session_id'],request_index=c['request_index'],route=c['route'])\n"
                + mutation + " time.sleep(.01)\n print(json.dumps(r),flush=True)\n")
        return release.collect_soak_v2([sys.executable, "-c", code], self.identity, self.origin,
                                       self.root / directory, duration, timeout)

    def audit(self, directory="run", minimum_duration=0):
        path = self.root / directory / "session.json"
        return release.raw_soak_contract({"session": {"path": str(path.relative_to(self.root)),
                                                       "sha256": release.sha(path)}},
                                         self.root, self.identity, self.origin, [8192], minimum_duration)

    def rewrite_journal(self, mutate):
        directory = self.root / "run"
        path = directory / "session.json"
        session = release.read_json(path)
        journal = directory / session["files"]["journal"]["path"]
        rows = list(release.journal_records(journal))
        mutate(rows)
        journal.write_text("".join(json.dumps(row) + "\n" for row in rows))
        session["files"]["journal"]["sha256"] = release.sha(journal)
        path.write_text(json.dumps(session))

    def test_persistent_raw_smoke_recalculates_and_never_qualifies_four_hours(self):
        receipt = self.collect()
        self.assertTrue(receipt["completed"])
        self.assertFalse(receipt["serving_qualified"])
        result = self.audit()
        self.assertEqual(result["load_generation"], 7)
        self.assertEqual(len(result["trials"]), receipt["trial_count"])
        self.assertEqual({trial["report"]["route"] for trial in result["trials"]}, release.ROUTES)
        with self.assertRaisesRegex(ValueError, "duration below"):
            self.audit(minimum_duration=14400)

    def test_raw_resource_tampering_rejects_even_with_rehashed_journal(self):
        self.collect()
        self.rewrite_journal(lambda rows: next(row for row in rows if row["event"] == "resource").update(swapout_bytes=1))
        with self.assertRaisesRegex(ValueError, "resource guard"):
            self.audit()

    def test_shutdown_rejects_live_descendants_at_collection_and_reaudit(self):
        with patch.object(release, "group_rss_mib", return_value=64), \
                self.assertRaisesRegex(ValueError, "live descendants"):
            self.collect("descendants")
        self.assertFalse(release.read_json(self.root / "descendants/session.json")["completed"])
        self.collect()
        self.rewrite_journal(lambda rows: next(row for row in rows if row["event"] == "shutdown").update(remaining_group_rss_mib=64))
        with self.assertRaisesRegex(ValueError, "live descendants"):
            self.audit()

    def test_raw_command_order_rejects(self):
        self.collect()
        self.rewrite_journal(lambda rows: next(row for row in rows if row["event"] == "command").update(request_index=3))
        with self.assertRaisesRegex(ValueError, "command sequence"):
            self.audit()

    def test_raw_sampling_gap_cannot_be_hidden_in_trial_summary(self):
        self.collect()
        path = self.root / "run/session.json"
        session = release.read_json(path)
        # Insert an idle hole after launch while preserving every event's order.
        self.rewrite_journal(lambda rows: [row.update(elapsed_ns=row["elapsed_ns"] + 3_000_000_000)
                                          for row in rows if row["event"] != "start"])
        session = release.read_json(path)
        session["elapsed_ns"] += 3_000_000_000
        path.write_text(json.dumps(session))
        with self.assertRaisesRegex(ValueError, "sampling gap"):
            self.audit()

    def test_stdout_and_trial_files_are_cross_checked(self):
        self.collect()
        path = self.root / "run/session.json"
        session = release.read_json(path)
        stdout = path.parent / session["files"]["stdout"]["path"]
        rows = list(release.journal_records(stdout))
        rows[0]["committed_after_bytes"] -= 1
        stdout.write_text("".join(json.dumps(row) + "\n" for row in rows))
        session["files"]["stdout"]["sha256"] = release.sha(stdout)
        path.write_text(json.dumps(session))
        with self.assertRaisesRegex(ValueError, "stdout/report mismatch"):
            self.audit()

    def test_reply_loaded_identity_sequence_and_baseline_are_checked_at_collection(self):
        for label, change, error in (
                ("identity", " r['loaded_identity']['profile_sha256']='0'*64\n", "identity mismatch"),
                ("sequence", " r['request_index']+=1\n", "reply/command mismatch"),
                ("baseline", " r['live_bytes_before']+=c['request_index']; r['live_bytes_after_release']=r['live_bytes_before']\n", "baseline changed")):
            with self.subTest(label=label):
                with self.assertRaisesRegex(ValueError, error):
                    self.collect(label, change)
                receipt = release.read_json(self.root / label / "session.json")
                self.assertFalse(receipt["completed"])
                self.assertIn("failure", receipt)

    def test_binary_mismatch_resource_refusal_and_timeout_do_not_launch_unbounded_work(self):
        bad = dict(self.origin, binary_sha256="0" * 64)
        with self.assertRaisesRegex(ValueError, "binary provenance"):
            release.collect_soak_v2([sys.executable], self.identity, bad, self.root / "binary")
        with patch.object(release, "memory_free_percent", return_value=5), patch.object(release.subprocess, "Popen") as launch:
            with self.assertRaisesRegex(ValueError, "resource guard"):
                self.collect("memory")
            launch.assert_not_called()
        with self.assertRaisesRegex(ValueError, "timed out"):
            self.collect("timeout", " time.sleep(10)\n", duration=.01, timeout=.05)
        self.assertFalse(release.read_json(self.root / "timeout/session.json")["completed"])

    def test_truncated_oversized_duplicate_and_nonfinite_journal_records_reject(self):
        path = self.root / "bad.jsonl"
        for raw in (b'{"a":1}', b'{"a":1,"a":2}\n', b'{"a":NaN}\n', b" " * 65536 + b"\n"):
            with self.subTest(raw=raw[:40]):
                path.write_bytes(raw)
                with self.assertRaises(ValueError):
                    list(release.journal_records(path))


if __name__ == "__main__":
    unittest.main()
