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
from pathlib import Path
import tempfile
import unittest

from summarize_cuda_http_pair import validate_campaign


class CampaignTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.fixture = self.root / "fixtures.json"
        self.fixture.write_text(json.dumps([{"id": "image", "max_tokens": 64, "calls": [{"text": "describe"}, {"text": "read"}]}]))
        self.provenance = {"arguments": {"arms": ["antfly", "llama"], "lifecycles": 2, "repeats": 1, "fixtures": str(self.fixture)},
                           "hashes": {"fixtures": hashlib.sha256(self.fixture.read_bytes()).hexdigest()}}
        self.rows = [{"arm": arm, "lc": lc, "cell": "image", "repeat": 0, "index": i,
                      "usage": {"prompt_tokens": 284, "completion_tokens": 64},
                      "ms": 100 if arm == "antfly" else 110, "ttft_ms": 10, "decode_tok_s": 700,
                      "text": str(i)} for arm in ("antfly", "llama") for lc in range(2) for i in range(2)]
        self.memory = {"valid": True, "errors": [], "samples": 20,
                       "peak_footprint_bytes": 1000, "peak_gpu_bytes": 700, "peak_pss_bytes": 600}
        self.save()

    def save(self):
        (self.root / "provenance.json").write_text(json.dumps(self.provenance))
        (self.root / "calls.jsonl").write_text("".join(json.dumps(r) + "\n" for r in self.rows))
        for arm in ("antfly", "llama"):
            for lc in range(2):
                (self.root / f"lc{lc}-{arm}.json").write_text(json.dumps({"memory": self.memory}))

    def test_complete_pair_uses_simultaneous_memory_peak(self):
        result = validate_campaign(self.root)
        self.assertEqual(result["calls_per_arm"], 4)
        self.assertTrue(result["all_cells_faster"])
        self.assertEqual(result["memory"]["antfly"]["peak_footprint_bytes"], [1000, 1000])
        self.assertFalse(result["combined_memory_lower"])

    def test_receipt_patch_grid_mismatch_rejects_summary(self):
        self.rows[-1]["usage"]["prompt_tokens"] = 252
        self.save()
        with self.assertRaisesRegex(ValueError, "prompt token"):
            validate_campaign(self.root)

    def test_missing_or_duplicate_calls_reject_summary(self):
        removed = self.rows.pop()
        self.save()
        with self.assertRaisesRegex(ValueError, "incomplete or duplicate"):
            validate_campaign(self.root)
        self.rows.extend([removed, removed])
        self.save()
        with self.assertRaisesRegex(ValueError, "incomplete or duplicate"):
            validate_campaign(self.root)

    def test_wrong_output_count_rejects_summary(self):
        self.rows[0]["usage"]["completion_tokens"] = 63
        self.save()
        with self.assertRaisesRegex(ValueError, "completion cap"):
            validate_campaign(self.root)

    def test_invalid_sampler_rejects_summary(self):
        self.memory["errors"] = ["NVML unavailable"]
        self.save()
        with self.assertRaisesRegex(ValueError, "memory sampler"):
            validate_campaign(self.root)

    def test_unstable_native_greedy_output_rejects_summary(self):
        self.rows[2]["text"] = "changed"
        self.save()
        with self.assertRaisesRegex(ValueError, "native greedy"):
            validate_campaign(self.root)

    def test_invalid_memory_values_reject_summary(self):
        for value in (float("nan"), -1, 0, True):
            self.memory["peak_gpu_bytes"] = value
            self.save()
            with self.assertRaisesRegex(ValueError, "memory sampler"):
                validate_campaign(self.root)

    def test_unpaired_campaign_rejects_summary(self):
        self.provenance["arguments"]["arms"] = ["antfly"]
        self.save()
        with self.assertRaisesRegex(ValueError, "paired campaign"):
            validate_campaign(self.root)

    def test_changed_fixture_rejects_summary(self):
        self.fixture.write_text("[]")
        with self.assertRaisesRegex(ValueError, "fixture checksum"):
            validate_campaign(self.root)

    def test_invalid_or_inverted_timing_rejects_summary(self):
        for value in (float("nan"), 0, 100):
            self.rows[0]["ttft_ms"] = value
            self.save()
            with self.assertRaisesRegex(ValueError, "timing|first-token"):
                validate_campaign(self.root)


if __name__ == "__main__":
    unittest.main()
