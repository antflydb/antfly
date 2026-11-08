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

"""Reject skipped, partial and duplicate performance evidence."""

import json
import unittest
from check_lake_execution_runtime import observations, PHASES, FAMILIES


class Evidence(unittest.TestCase):
    def log(self):
        return "\n".join(
            "LAKE_RUNTIME "
            + json.dumps(
                dict(
                    parallel_jobs=5,
                    headroom_mode="auto",
                    headroom_queries=1,
                    family_names=list(FAMILIES),
                    family_ns=[1] * len(FAMILIES),
                    family_calls=[1] * len(FAMILIES),
                    family_declaration_root_bytes=[1] * len(FAMILIES),
                    api_decoded_hits=1,
                    provider_decoded_hits=1,
                    io_dispatch="linear",
                    caller_io_dispatch_calls=1,
                    provider_io_dispatch_calls=1,
                    working_quota_layers=2 if mode == "native" else 1,
                    provider_quota_peak_bytes=1,
                    source_optimize="debug",
                    provider_optimize=0,
                    source_is_test=False,
                    provider_is_test=False,
                    mode=mode,
                    phase=phase,
                    round=0,
                    declarations=5,
                    rows=40 if phase == "incremental_append" else 32,
                    api_source_open_ns=0,
                    api_admission_ns=0,
                    provider_store_open_ns=1,
                    provider_source_open_ns=1,
                    provider_physical_ns=1,
                    provider_cache_hits=1,
                    provider_cache_reads=1,
                    provider_cache_bytes=1,
                    control_ns=0,
                    execution_ns=1,
                    total_attempt_ns=1,
                    peak_working_set_bytes=1,
                )
            )
            for mode in ("source", "native")
            for phase in PHASES
        )

    def test_complete(self):
        self.assertEqual(len(observations(self.log(), 1, 8)), 6)

    def test_skip_partial_and_duplicate_fail(self):
        for log in (
            "1 skipped",
            self.log().split("\n", 1)[1],
            self.log() + "\n" + self.log().split("\n")[0],
        ):
            with self.assertRaises(ValueError):
                observations(log, 1, 8)

    def test_test_mode_evidence_is_rejected(self):
        with self.assertRaises(ValueError):
            observations(
                self.log().replace('"source_is_test": false', '"source_is_test": true'),
                1,
                8,
            )

    def test_optimized_evidence_requires_both_compilation_modes(self):
        fast = (
            self.log()
            .replace('"source_optimize": "debug"', '"source_optimize": "fast"')
            .replace('"provider_optimize": 0', '"provider_optimize": 1')
        )
        self.assertEqual(len(observations(fast, 1, 8, optimize="fast")), 6)
        for log in (
            self.log(),
            fast.replace('"provider_optimize": 1', '"provider_optimize": 0'),
        ):
            with self.assertRaisesRegex(ValueError, "optimization mode mismatch"):
                observations(log, 1, 8, optimize="fast")

    def test_incorrect_workload_fails(self):
        with self.assertRaises(ValueError):
            observations(self.log(), 1, 9)


if __name__ == "__main__":
    unittest.main()
