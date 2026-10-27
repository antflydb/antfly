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

import unittest

from source_family import PHASES, parse_metrics


class QualificationReportTest(unittest.TestCase):
    def record(self):
        return "SOURCE_FAMILY " + " ".join(f"{name}=123" for name in PHASES) + (
            " documents=131 payload=4096 update_claims=3 physical_shared=1 verified=12 claims_after_gc=131 production_allocator=1 claims_after_drop=0 optimize_mode=2")

    def test_accepts_verified_native_record_among_test_runner_output(self):
        result = parse_metrics("1/1 test..." + self.record() + "\nOK\n", 131, 4096)
        self.assertEqual(result["documents"], 131)

    def test_requires_exactly_one_record(self):
        for text in ("OK", self.record() + "\n" + self.record()):
            with self.subTest(text=text), self.assertRaises(RuntimeError):
                parse_metrics(text, 131, 4096)

    def test_rejects_unverified_or_mismatched_workloads(self):
        for text in (
            self.record().replace("verified=12", "verified=6"),
            self.record().replace("physical_shared=1", "physical_shared=0"),
            self.record().replace("claims_after_gc=131", "claims_after_gc=134"),
            self.record().replace("documents=131", "documents=195"),
            self.record().replace("payload=4096", "payload=0"),
            self.record().replace("claims_after_drop=0", "claims_after_drop=131"),
            self.record().replace("production_allocator=1", "production_allocator=0"),
            self.record().replace("update_claims=3", "update_claims=64"),
            self.record().replace("optimize_mode=2", "optimize_mode=99"),
            self.record().replace("prepare_ns=123 ", "", 1),
        ):
            with self.subTest(text=text), self.assertRaises(RuntimeError):
                parse_metrics(text, 131, 4096)

    def test_rejects_truncated_and_duplicate_numbers(self):
        for text in (self.record().replace("prepare_ns=123", "prepare_ns=123ms", 1),
                     self.record() + " prepare_ns=456"):
            with self.subTest(text=text), self.assertRaises(RuntimeError):
                parse_metrics(text, 131, 4096)


if __name__ == "__main__":
    unittest.main()
