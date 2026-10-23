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

"""Capacity checks protect the validity of explicit-cache benchmark controls."""

import unittest

from benchmark_vllm_metal import cache_override_admission


class CacheOverrideAdmissionTests(unittest.TestCase):
    def test_rejects_capacity_above_server_probe(self):
        log = "Overriding num_gpu_blocks=221 with num_gpu_blocks_override=256"
        result = cache_override_admission(log, 256)
        self.assertFalse(result["admitted"])
        self.assertEqual(result["probes"][0]["automatic_blocks"], 221)

    def test_accepts_capacity_within_every_probe(self):
        log = "\n".join(
            [
                "Overriding num_gpu_blocks=221 with num_gpu_blocks_override=192",
                "Overriding num_gpu_blocks=194 with num_gpu_blocks_override=192",
            ]
        )
        self.assertTrue(cache_override_admission(log, 192)["admitted"])
        self.assertFalse(cache_override_admission(log, 128)["admitted"])

    def test_missing_probe_fails_closed_only_for_explicit_override(self):
        self.assertFalse(cache_override_admission("", 128)["admitted"])
        self.assertTrue(cache_override_admission("", None)["admitted"])

    def test_rejects_one_unsafe_worker_among_safe_workers(self):
        log = "\n".join(
            [
                "Overriding num_gpu_blocks=300 with num_gpu_blocks_override=256",
                "Overriding num_gpu_blocks=221 with num_gpu_blocks_override=256",
            ]
        )
        self.assertFalse(cache_override_admission(log, 256)["admitted"])


if __name__ == "__main__":
    unittest.main()
