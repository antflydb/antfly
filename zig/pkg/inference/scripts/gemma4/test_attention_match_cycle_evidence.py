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
import unittest
from unittest import mock
import qualify_attention_match_queries as qualification


class ReadinessTests(unittest.TestCase):
    def test_both_launchers_require_a_successful_health_response(self):
        for address in ("127.0.0.1:8531", "http://127.0.0.1:8531"):
            with self.subTest(address=address):
                process = mock.Mock()
                process.poll.return_value = None
                log = mock.Mock()
                log.read_text.return_value = f"listening on {address}\n"
                with mock.patch.object(
                    qualification.urllib.request, "urlopen"
                ) as health:
                    health.return_value.__enter__.return_value.status = 200
                    qualification.wait_server_ready(process, log, 8531, timeout=0)
                    health.assert_called_once_with(
                        "http://127.0.0.1:8531/healthz", timeout=1
                    )

    def test_unrelated_listener_cannot_satisfy_readiness(self):
        process = mock.Mock()
        process.poll.return_value = None
        log = mock.Mock()
        log.read_text.return_value = "listening on http://127.0.0.1:85310\n"
        with mock.patch.object(qualification.urllib.request, "urlopen") as health:
            with self.assertRaises(TimeoutError):
                qualification.wait_server_ready(process, log, 8531, timeout=0)
            health.assert_not_called()

    def test_startup_log_alone_does_not_prove_readiness(self):
        process = mock.Mock()
        process.poll.return_value = None
        log = mock.Mock()
        log.read_text.return_value = "listening on http://127.0.0.1:8531\n"
        with mock.patch.object(
            qualification.urllib.request, "urlopen", side_effect=OSError
        ):
            with self.assertRaises(TimeoutError):
                qualification.wait_server_ready(process, log, 8531, timeout=0)
        process.poll.return_value = 1
        with self.assertRaises(RuntimeError):
            qualification.wait_server_ready(process, log, 8531, timeout=0)


class CycleEvidenceTests(unittest.TestCase):
    def test_rotated4_coverage_rejects_fallback_only_and_malformed_counts(self):
        valid = dict(
            rotated4_heads=2,
            rotated4_fallback_heads=6,
            validated_heads=8,
            rotated4_eligible_rows=80,
            rotated4_compressed_rows=15,
            rotated4_exact_rows=65,
        )
        qualification.validate_quantization_evidence([valid], "rotated4")
        self.assertEqual(qualification.checkpoint_wire_version("rotated4"), 4)
        with self.assertRaises(AssertionError):
            qualification.validate_quantization_evidence([], "rotated4")
        for key, value in (
            ("rotated4_heads", 0),
            ("rotated4_heads", True),
            ("rotated4_fallback_heads", -1),
            ("validated_heads", 9),
            ("validated_heads", True),
        ):
            with self.subTest(key=key, value=value), self.assertRaises(AssertionError):
                qualification.validate_quantization_evidence(
                    [{**valid, key: value}], "rotated4"
                )
        for key in valid:
            changed = dict(valid)
            del changed[key]
            with self.subTest(missing=key), self.assertRaises(AssertionError):
                qualification.validate_quantization_evidence([changed], "rotated4")

    def fixture(self):
        requests = [{"response": {"usage": {"prompt_tokens": 8189}}}]
        rows, fits = [], []
        length = 8189
        for index in range(2):
            length += 512 if index else 0
            fits.append(
                dict(
                    source_tokens=length,
                    snapshot_ns=1,
                    fit_ns=1,
                    export_ns=1,
                    upload_ns=1,
                    install_ns=1,
                    total_ns=6,
                    source_host_bytes=100,
                    candidate_host_bytes=80,
                    candidate_device_bytes=40,
                    export_bytes=90,
                    completed_chunks=6,
                    max_nnls_sweeps=1,
                    export_sha256="a" * 64,
                    export_version=2,
                    validated_heads=6,
                    max_head_mass_error=0.01,
                    max_head_output_rmse=0.01,
                    residual_fit=1,
                    preserved_heads=2,
                    source_global_bytes=100,
                    candidate_global_bytes=80,
                    actual_global_savings_ratio=1.25,
                    cuda_fit=1,
                    diagnostic_only=1,
                    installed=1,
                    cooperative_fit=1,
                    cooperative_launches=128,
                    cooperative_restarts=0,
                    fit_workers=4,
                    workspace_reuses=20,
                    host_workspace_limit_bytes=2 * 1024**3,
                )
            )
            length += 2
            rows.append(
                dict(
                    cycle=index + 1,
                    logical_tokens=length,
                    completed_chunks=6,
                    greedy_matches=3,
                    greedy_comparisons=3,
                    answer_appends=2,
                    max_logit_relative_l2=0.01,
                    ragged_repeat_calls=42 if index else 0,
                    installed_revision=2 + index * 10,
                    checkpoint_roundtrip=1,
                    source_unchanged=1,
                    invalid=0,
                    incremental_savings_ratio=1.25,
                )
            )
        summaries = [
            dict(
                cycles=2,
                initial_tokens=8189,
                final_tokens=length,
                greedy_matches=6,
                greedy_comparisons=6,
                source_unchanged=1,
            )
        ]
        return rows, fits, summaries, requests

    def validate(self, evidence):
        qualification.validate_cycle_evidence(
            *evidence,
            cycles=2,
            append_tokens=512,
            answer_tokens=4,
            minimum_savings=1.01,
            first_minimum_savings=1.2,
        )

    def test_middle_fact_is_outside_the_protected_prompt_prefix(self):
        prompt = next(
            qualification.requests("model", "f16", [742], fixture="fact_middle")
        )["messages"][0]["content"]
        before, after = prompt.split("The access code is violet-729.\n")
        self.assertEqual(before.count("This log entry"), 371)
        self.assertEqual(after.count("This log entry"), 371)
        self.assertTrue(after.endswith("Reply with only the access code."))

    def test_complete_successive_chain(self):
        self.validate(self.fixture())
        parsed = qualification.parse_capture(
            "incremental_savings_ratio=1.25 max_logit_relative_l2=0.01 answer_appends=2"
        )
        self.assertEqual(
            parsed,
            dict(
                incremental_savings_ratio=1.25,
                max_logit_relative_l2=0.01,
                answer_appends=2,
            ),
        )

    def test_fp8_cycles_require_actual_quantization_and_current_policy(self):
        evidence = self.fixture()
        for fit in evidence[1]:
            fit.update(
                export_version=3,
                fp8_heads=5,
                fp8_fallback_heads=1,
                min_retention_divisor=2,
            )

        def validate(rows):
            qualification.validate_cycle_evidence(
                *rows,
                cycles=2,
                append_tokens=512,
                answer_tokens=4,
                minimum_savings=1.01,
                first_minimum_savings=1.2,
                encoding="fp8",
                native_policy=True,
            )

        validate(evidence)
        for key, value in (
            ("export_version", 2),
            ("fp8_heads", 0),
            ("fp8_heads", True),
            ("fp8_fallback_heads", 0),
            ("fp8_fallback_heads", -1),
            ("min_retention_divisor", 4),
        ):
            changed = copy.deepcopy(evidence)
            changed[1][-1][key] = value
            with self.subTest(key=key, value=value), self.assertRaises(AssertionError):
                validate(changed)
        for key in ("fp8_heads", "fp8_fallback_heads", "min_retention_divisor"):
            changed = copy.deepcopy(evidence)
            del changed[1][-1][key]
            with self.subTest(missing=key), self.assertRaises(AssertionError):
                validate(changed)

    def test_rejects_missing_or_misordered_cycles_and_false_success(self):
        for key, value in (
            ("cycle", 1),
            ("source_unchanged", 0),
            ("invalid", 1),
            ("checkpoint_roundtrip", 0),
            ("ragged_repeat_calls", 0),
            ("greedy_matches", 2),
            ("answer_appends", 5),
            ("logical_tokens", 8189),
            ("installed_revision", 2),
            ("max_logit_relative_l2", float("nan")),
            ("max_logit_relative_l2", -1),
            ("incremental_savings_ratio", 1),
            ("completed_chunks", 0),
        ):
            evidence = self.fixture()
            evidence[0][1][key] = value
            with self.subTest(key=key), self.assertRaises(AssertionError):
                self.validate(evidence)
        for part in (0, 1, 2):
            evidence = self.fixture()
            evidence[part].pop()
            with self.subTest(part=part), self.assertRaises(AssertionError):
                self.validate(evidence)

    def test_rejects_fit_or_summary_disagreement(self):
        for part, key, value in (
            (1, "source_tokens", 8189),
            (1, "export_version", 1),
            (1, "host_workspace_limit_bytes", 0),
            (1, "actual_global_savings_ratio", 2),
            (1, "completed_chunks", 7),
            (2, "greedy_matches", 0),
            (2, "final_tokens", 8189),
            (2, "cycles", 20),
        ):
            evidence = self.fixture()
            evidence[part][-1][key] = value
            with self.subTest(part=part, key=key), self.assertRaises(AssertionError):
                self.validate(evidence)


class MemoryCoverageTests(unittest.TestCase):
    def test_memory_scope_tracks_cold_start_and_prefers_full_block_samples(self):
        from qualify_attention_match_queries import summarize_memory

        warm = {
            "samples": 10,
            "errors": [],
            "peak_host_rss_bytes": 3 * 1024**3,
            "peak_gpu_memory_bytes": 10 * 1024**3,
        }
        cold = warm | {"peak_host_rss_bytes": 9 * 1024**3}
        result = summarize_memory(
            [
                {
                    "memory": cold,
                    "memory_after_first_request": warm,
                    "sampling_started_before_requests": True,
                }
            ]
        )
        self.assertTrue(result["includes_cold_start"])
        self.assertFalse(result["memory_gate_met"])
        self.assertFalse(
            summarize_memory([{"memory_after_first_request": warm}])[
                "includes_cold_start"
            ]
        )
        self.assertFalse(summarize_memory([])["includes_cold_start"])


if __name__ == "__main__":
    unittest.main()
