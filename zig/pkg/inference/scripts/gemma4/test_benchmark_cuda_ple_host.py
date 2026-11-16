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

"""CPU regressions for placement, output contracts and contention evidence."""

import unittest

import benchmark_cuda_ple_host as bench


class PlacementTests(unittest.TestCase):
    def check(self, values, placement):
        bench.validate_placement([{"host_ple": v} for v in values], placement, 64)

    def test_device_rejects_later_host_placement(self):
        self.check([0, 0, 0], "device")
        with self.assertRaises(RuntimeError):
            self.check([0, 64], "device")

    def test_host_requires_exact_stable_size_after_loading(self):
        self.check([0, 64, 64], "host")
        for values in ([], [0], [32, 64], [64, 0], [64, 128]):
            with self.subTest(values=values), self.assertRaises(RuntimeError):
                self.check(values, "host")

    def test_parity_includes_all_usage_fields(self):
        first = {
            "text": "answer",
            "reasoning": "",
            "usage": {"prompt_tokens": 1, "completion_tokens": 2, "cached_tokens": 0},
        }
        second = {**first, "usage": {**first["usage"], "cached_tokens": 1}}
        self.assertNotEqual(bench.output_contract(first), bench.output_contract(second))


class ContentionTests(unittest.TestCase):
    def metadata(self, monitor):
        return [
            {
                "cotenants_before": [],
                "cotenants_after": [],
                "contention": {
                    "valid": monitor.samples > 0 and not monitor.errors,
                    "cotenants": sorted(monitor.cotenants),
                },
            }
        ]

    def test_transient_cotenant_is_retained_between_idle_boundaries(self):
        pids = [1, 2]
        monitor = bench.ContentionMonitor(lambda: pids, lambda pid: pid in (1, 2))
        monitor.sample()
        self.assertTrue(bench.uncontended(self.metadata(monitor), False))
        pids = [1, 2, 99]
        monitor.sample()
        pids = [1]
        monitor.sample()
        self.assertEqual(monitor.cotenants, {99})
        self.assertFalse(bench.uncontended(self.metadata(monitor), False))

    def test_diagnostic_flag_always_disables_uncontended_claim(self):
        monitor = bench.ContentionMonitor(lambda: [], lambda pid: False)
        monitor.sample()
        self.assertFalse(bench.uncontended(self.metadata(monitor), True))

    def test_monitoring_failure_or_no_samples_cannot_qualify(self):
        def failure():
            raise RuntimeError("NVML unavailable")

        monitor = bench.ContentionMonitor(failure, lambda pid: False)
        self.assertFalse(bench.uncontended(self.metadata(monitor), False))
        monitor.sample()
        self.assertTrue(monitor.errors)
        self.assertFalse(bench.uncontended(self.metadata(monitor), False))
        self.assertFalse(bench.uncontended([], False))


if __name__ == "__main__":
    unittest.main()
