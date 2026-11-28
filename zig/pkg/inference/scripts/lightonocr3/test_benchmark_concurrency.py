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

"""Serving qualification must distinguish a CUDA model from CUDA recurrence."""

import unittest

from benchmark_concurrency import (
    apply_token_burst_overrides,
    graph_first_replays,
    graph_funding_denials,
    graph_recording_terminals,
    graph_rebound_replays,
    graph_retirements,
    recurrent_placement_observation,
    retirement_capacity_retries,
)


class GraphFundingTests(unittest.TestCase):
    def test_records_funding_bytes_without_inventing_a_park_denial(self):
        text = (
            "LIGHTONOCR3 cuda_model_graph_precommit_decline rows=98 stage=park "
            "error=ResourceTemporarilyUnavailable\n"
            "LIGHTONOCR3 cuda_model_graph_funding rows=97 stage=storage "
            "error=ResourceTemporarilyUnavailable requested_host_scratch=100 "
            "requested_backend_scratch=200 occupied_host_scratch=300 "
            "occupied_backend_scratch=400 policy_scratch_limit=900\n"
        )
        self.assertEqual(
            graph_funding_denials(text),
            [
                dict(
                    rows=97,
                    stage="storage",
                    error="ResourceTemporarilyUnavailable",
                    requested_host_scratch=100,
                    requested_backend_scratch=200,
                    occupied_host_scratch=300,
                    occupied_backend_scratch=400,
                    policy_scratch_limit=900,
                )
            ],
        )
        self.assertEqual(graph_funding_denials(text.splitlines()[0]), [])


class GraphReplayTests(unittest.TestCase):
    def test_recording_terminal_receipts_distinguish_zero_from_absent(self):
        self.assertEqual(graph_recording_terminals("older reader"), [])
        text = (
            "LIGHTONOCR3 cuda_model_graph_recorded_eos_rows=0 rows=113 "
            "minimum_remaining_tokens=101\n"
            "LIGHTONOCR3 cuda_model_graph_recorded_eos_rows=3 rows=29 "
            "minimum_remaining_tokens=78\n"
        )
        self.assertEqual(
            graph_recording_terminals(text),
            [
                dict(eos_rows=0, rows=113, minimum_remaining_tokens=101),
                dict(eos_rows=3, rows=29, minimum_remaining_tokens=78),
            ],
        )

    def test_distinguishes_dense_fused_and_uninstrumented_scratch(self):
        prefix = "LIGHTONOCR3 cuda_model_graph_first_replay_rows="
        text = "\n".join(
            (
                prefix + "2",
                prefix + "8 capture=1 head_packet_rows=8 head_bank_bytes=128",
                prefix
                + "64 capture=1 head_packet_rows=64 head_bank_bytes=0 fused_head_scratch_bytes=0",
                prefix
                + "64 capture=1 head_packet_rows=64 head_bank_bytes=0 fused_head_scratch_bytes=496640"
                + " workspace_plan_origin=declared workspace_events=0 workspace_bytes=0",
            )
        )
        rows, banks = graph_first_replays(text)
        self.assertEqual(rows, [2, 8, 64, 64])
        self.assertNotIn("fused_head_scratch_bytes", banks[0])
        self.assertEqual(banks[1]["fused_head_scratch_bytes"], 0)
        self.assertEqual(banks[2]["fused_head_scratch_bytes"], 496640)
        self.assertNotIn("workspace_plan_origin", banks[1])
        self.assertEqual(banks[2]["workspace_plan_origin"], "declared")
        self.assertEqual(banks[2]["workspace_events"], 0)
        self.assertEqual(banks[2]["workspace_bytes"], 0)


class GraphReboundReplayTests(unittest.TestCase):
    def test_counts_only_replays_with_completed_readback(self):
        text = (
            "LIGHTONOCR3 cuda_model_graph_rebound_rows=113\n"
            "LIGHTONOCR3 cuda_model_graph_first_replay_rows=113\n"
            "LIGHTONOCR3 cuda_model_graph_rebound_replay_rows=98\n"
            "LIGHTONOCR3 cuda_model_graph_rebound_replay_rows=30\n"
        )
        self.assertEqual(graph_rebound_replays(text), [98, 30])


class GraphRetirementTests(unittest.TestCase):
    def test_capacity_reports_preserve_zero_and_distinguish_missing_data(self):
        self.assertEqual(retirement_capacity_retries("legacy benchmark"), [])
        self.assertEqual(
            retirement_capacity_retries(
                "LIGHTONOCR3 retirement_capacity_retries=0\n"
                "LIGHTONOCR3 retirement_capacity_retries=23\n"
            ),
            [0, 23],
        )

    def test_records_progress_and_ignores_reset_messages(self):
        text = (
            "LIGHTONOCR3 cuda_model_graph_reset phase=disabled rows=64\n"
            "LIGHTONOCR3 cuda_model_graph_retired reason=mixed_prefill "
            "phase=replay epoch=3 rows=64 progress=12\n"
            "LIGHTONOCR3 cuda_model_graph_retired reason=foreground_vision "
            "phase=capture epoch=3 rows=98 progress=2\n"
        )
        self.assertEqual(
            graph_retirements(text),
            [
                dict(
                    reason="mixed_prefill",
                    phase="replay",
                    epoch=3,
                    rows=64,
                    progress=12,
                ),
                dict(reason="foreground_vision", phase="capture", epoch=3, rows=98, progress=2),
            ],
        )


class RecurrentPlacementTests(unittest.TestCase):
    def test_cuda_backend_banner_is_insufficient(self):
        self.assertFalse(
            recurrent_placement_observation("LIGHTONOCR3 backend=cuda", None)["matched"]
        )

    def test_default_requires_every_loaded_session_on_device(self):
        gpu = "inference recurrent placement backend=cuda placement=cuda reason=supported_geometry compute=8.0"
        host = "inference recurrent placement backend=cuda placement=host reason=unsupported_geometry compute=8.0"
        result = recurrent_placement_observation(gpu, None)
        self.assertTrue(result["matched"])
        self.assertEqual(result["sessions"][0]["compute"], "8.0")
        self.assertFalse(
            recurrent_placement_observation(gpu + "\n" + host, None)["matched"]
        )

    def test_explicit_cpu_baseline_requires_operator_override(self):
        host = "inference recurrent placement backend=cuda placement=host reason=operator_disabled compute=12.1"
        self.assertTrue(recurrent_placement_observation(host, False)["matched"])
        self.assertFalse(recurrent_placement_observation(host, None)["matched"])
        self.assertFalse(
            recurrent_placement_observation(
                host.replace("operator_disabled", "unsupported_device"), False
            )["matched"]
        )


class AutomaticBurstTests(unittest.TestCase):
    def test_omitted_options_leave_runtime_selection_and_width_unset(self):
        env = {}
        apply_token_burst_overrides(env, None, None)
        self.assertEqual(env, {})

    def test_explicit_disable_and_width_are_independent(self):
        env = {}
        apply_token_burst_overrides(env, False, None)
        self.assertEqual(env, {"ANTFLY_LIGHTONOCR3_GPU_TOKEN_BURST": "0"})
        env = {}
        apply_token_burst_overrides(env, None, 4)
        self.assertEqual(env, {"ANTFLY_LIGHTONOCR3_GPU_TOKEN_BURST_WIDTH": "4"})

    def test_explicit_enable_is_recorded(self):
        env = {}
        apply_token_burst_overrides(env, True, 8)
        self.assertEqual(
            env,
            {
                "ANTFLY_LIGHTONOCR3_GPU_TOKEN_BURST": "1",
                "ANTFLY_LIGHTONOCR3_GPU_TOKEN_BURST_WIDTH": "8",
            },
        )


if __name__ == "__main__":
    unittest.main()
