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

from pathlib import Path
from types import SimpleNamespace
import sys
import tempfile
import unittest
from unittest import mock

BASE = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE))

import qualify_metal_native_checkpoint_performance as performance


class FakeResponse:
    status = 200

    def getheader(self, name, default=None):
        if name == "Content-Type":
            return "text/event-stream"
        return default


class FakeConnection:
    def __init__(self, *args, **kwargs):
        self.response = FakeResponse()
        self.closed = False

    def request(self, *args, **kwargs):
        pass

    def getresponse(self):
        return self.response

    def close(self):
        self.closed = True


class PerformanceObservationEvidence(unittest.TestCase):
    def fixture(self, root):
        output = Path(root) / "result.json"
        output.with_suffix(".worker.log").write_bytes(b"existing log\n")
        args = SimpleNamespace(
            output=output,
            port=1234,
            timeout=1,
            encoding="f16",
        )
        raw = {
            "ttft_ms": 34_170.0,
            "decode_ms": 2_712.0,
            "e2e_ms": 36_882.0,
            "completion_tokens": 64,
            "prompt_tokens": 8_128,
            "content_sha256": "a" * 64,
        }
        return args, raw

    def test_context_failed_ledger_retains_attributed_raw_timing(self):
        with tempfile.TemporaryDirectory() as temporary:
            args, raw = self.fixture(temporary)
            record = {}
            rows = [{"phase": "generation_committed", "logical_tokens": 8192,
                     "binding_logical_tokens": 8191}]
            with mock.patch.object(performance.http.client, "HTTPConnection",
                                   FakeConnection), \
                    mock.patch.object(performance, "send_resume",
                                      return_value=FakeResponse()), \
                    mock.patch.object(performance, "response_headers"), \
                    mock.patch.object(performance, "timed_stream",
                                      return_value=raw), \
                    mock.patch.object(performance, "wait_ledger",
                                      return_value=rows), \
                    mock.patch.object(performance, "ledger_contract",
                                      side_effect=ValueError(
                                          "checkpoint restore binding is incomplete")):
                with self.assertRaisesRegex(ValueError, "binding is incomplete"):
                    performance.observe(args, {}, record, "exact", Path("exact.kv"))

            self.assertEqual(len(record["performance_observations"]), 1)
            attempt = record["performance_observations"][0]
            self.assertEqual(attempt["attempt_index"], 0)
            self.assertEqual(attempt["mode"], "exact")
            self.assertEqual(attempt["route"], "checkpoint_resume")
            self.assertTrue(attempt["checkpoint"].endswith("exact.kv"))
            self.assertEqual(attempt["observation"], raw)
            self.assertEqual(attempt["ledger_rows"], rows)
            self.assertFalse(attempt["validated"])
            self.assertIn("binding is incomplete", attempt["validation_error"])

    def test_context_success_marks_observation_validated_after_contract(self):
        with tempfile.TemporaryDirectory() as temporary:
            args, raw = self.fixture(temporary)
            record = {}
            rows = [{"phase": "generation_committed"}]
            contract = {"committed_context_bytes": 1024}
            with mock.patch.object(performance.http.client, "HTTPConnection",
                                   FakeConnection), \
                    mock.patch.object(performance, "response_headers"), \
                    mock.patch.object(performance, "timed_stream",
                                      return_value=raw), \
                    mock.patch.object(performance, "wait_ledger",
                                      return_value=rows), \
                    mock.patch.object(performance, "ledger_contract",
                                      return_value=contract):
                result = performance.observe(args, {}, record, "ordinary")

            self.assertEqual(result["owner_ledger"], contract)
            attempt = record["performance_observations"][0]
            self.assertTrue(attempt["validated"])
            self.assertEqual(attempt["owner_ledger"], contract)
            self.assertNotIn("validation_error", attempt)


class PendingSampledTokenLedger(unittest.TestCase):
    def rows(self):
        common = {
            "schema": "antfly.metal-checkpoint-phase.v1",
            "request": 3, "route": "checkpoint_resume", "owner_id": 3,
            "owner_available": True, "physical_available": True,
            "combined_available": True, "invalid": False, "binding": True,
            "validation_ns": 1, "upload_ns": 2, "install_ns": 3,
            "suffix_prefill_ns": 0,
            "tracked_combined_live_bytes": 180, "tracked_combined_peak_bytes": 200,
            "binding_representation": "f16", "binding_fp8_heads": 0,
            "binding_fp8_rows": 0, "binding_exact_resident_bytes": 60,
            "binding_encoded_resident_bytes": 0,
            "binding_metadata_resident_bytes": 15, "binding_allocated_bytes": 75,
            "kv_boundary_proven": True,
        }
        ready = {
            **common, "phase": "resumed_ready", "logical_tokens": 8128,
            "binding_logical_tokens": 8128, "attention_calls": 0,
            "donor_appends": 0, "kv_materialized_tokens": 8128,
            "pending_sampled_tokens": 0,
            "binding_attention_calls_per_materialized_token": 42,
            "binding_donor_appends_per_materialized_token": 24,
        }
        committed = {
            **common, "phase": "generation_committed", "logical_tokens": 8192,
            "binding_logical_tokens": 8191, "attention_calls": 63 * 42,
            "donor_appends": 63 * 24, "kv_materialized_tokens": 8191,
            "pending_sampled_tokens": 1,
            "binding_attention_calls_per_materialized_token": 42,
            "binding_donor_appends_per_materialized_token": 24,
        }
        released = {
            "schema": "antfly.metal-checkpoint-release.v1",
            "phase": "final_release", "request": 3,
            "route": "checkpoint_resume", "owner_id": 3,
            "released": True, "scope_complete": True,
            "full_owner_physical_available": True, "measured_available": True,
            "initial_live_bytes": 105, "measured_committed_bytes": 75,
            "post_release_live_bytes": 0,
        }
        return ready, committed, released

    def test_context_pending_sampled_token_requires_exact_boundary_and_activity(self):
        ready, committed, released = self.rows()
        result = performance.ledger_contract(
            [ready, committed, released], "checkpoint_resume",
            expected_representation="f16",
        )
        self.assertEqual(result["committed"]["binding_logical_tokens"], 8191)
        self.assertEqual(result["committed"]["pending_sampled_tokens"], 1)

    def test_context_pending_sampled_token_rejects_unproved_off_by_one(self):
        mutations = {
            "missing proof": {"kv_boundary_proven": False},
            "two pending": {"pending_sampled_tokens": 2},
            "wrong materialized": {"kv_materialized_tokens": 8190},
            "attention remainder": {"attention_calls": 63 * 42 - 1},
            "append remainder": {"donor_appends": 63 * 24 - 1},
            "zero attention rate": {
                "binding_attention_calls_per_materialized_token": 0},
            "zero append rate": {
                "binding_donor_appends_per_materialized_token": 0},
            "wrong model attention rate": {
                "binding_attention_calls_per_materialized_token": 41},
            "wrong model append rate": {
                "binding_donor_appends_per_materialized_token": 23},
        }
        for label, mutation in mutations.items():
            with self.subTest(label=label):
                ready, committed, released = self.rows()
                committed.update(mutation)
                with self.assertRaisesRegex(
                        ValueError, "KV boundary|KV activity|KV rates|typed"):
                    performance.ledger_contract(
                        [ready, committed, released], "checkpoint_resume",
                        expected_representation="f16",
                    )

    def test_context_pending_sampled_token_rejects_consistent_skipped_consumer(self):
        ready, committed, released = self.rows()
        ready["binding_attention_calls_per_materialized_token"] = 41
        committed["binding_attention_calls_per_materialized_token"] = 41
        committed["attention_calls"] = 63 * 41
        with self.assertRaisesRegex(ValueError, "qualified E4B geometry"):
            performance.ledger_contract(
                [ready, committed, released], "checkpoint_resume",
                expected_representation="f16",
            )

    def test_context_equal_binding_rejects_contradictory_optional_proof(self):
        ready, committed, released = self.rows()
        committed.update(
            binding_logical_tokens=8192,
            kv_materialized_tokens=8192,
            pending_sampled_tokens=0,
        )
        # The claimed boundary now has 64 materialized steps, while activity
        # proves only 63. Equality must not bypass a present proof contract.
        with self.assertRaisesRegex(ValueError, "KV activity"):
            performance.ledger_contract(
                [ready, committed, released], "checkpoint_resume",
                expected_representation="f16",
            )

    def test_context_pending_sampled_token_rejects_unproved_ready_boundary(self):
        mutations = {
            "ready pending": {"pending_sampled_tokens": 1},
            "ready materialized": {"kv_materialized_tokens": 8127},
            "ready binding": {"binding_logical_tokens": 8127},
        }
        for label, mutation in mutations.items():
            with self.subTest(label=label):
                ready, committed, released = self.rows()
                ready.update(mutation)
                with self.assertRaisesRegex(ValueError, "KV boundary"):
                    performance.ledger_contract(
                        [ready, committed, released], "checkpoint_resume",
                        expected_representation="f16",
                    )


if __name__ == "__main__":
    unittest.main()
