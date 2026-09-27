# Copyright 2026 Antfly, Inc.
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

from check_source_publication_churn import check_identity, check_restored_recall


class PublicationChurnGatesTest(unittest.TestCase):
    def test_identity_and_restored_count_are_required(self):
        payload = {
            "status": {"readiness": {"incarnation": "clone"}, "doc_count": 50000}
        }
        check_identity(payload, "clone")
        with self.assertRaises(RuntimeError):
            check_identity(payload, "another-server")
        payload["status"]["doc_count"] = 49000
        with self.assertRaises(RuntimeError):
            check_identity(payload, "clone")

    def test_missing_nonfinite_and_degraded_recall_fail_closed(self):
        before = {"count": 1000, "recall": 0.99}
        check_restored_recall(before, {"count": 1000, "recall": 0.98})
        for after in (
            {},
            {"count": 1000, "recall": float("nan")},
            {"count": 10, "recall": 0.99},
            {"count": 1000, "recall": 0.979},
        ):
            with self.assertRaises(RuntimeError):
                check_restored_recall(before, after)


if __name__ == "__main__":
    unittest.main()
