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
from qualify_native_prepare_sampling import validate_export


class PreparationSamplingTests(unittest.TestCase):
    def fixture(self):
        return {
            "metadata": {
                "logical_tokens": 8189,
                "outcome": "compacted",
                "checkpoint_bytes": 100,
            },
            "identity": {"weights": "a" * 64},
            "file_sha256": "b" * 64,
        }

    def test_exact_qualified_export_is_accepted(self):
        expected = self.fixture()
        validate_export(copy.deepcopy(expected), expected, 8189)

    def test_fallback_identity_bytes_and_metadata_changes_fail(self):
        for mutate in [
            lambda x: x["metadata"].update(outcome="unchanged_insufficient_savings"),
            lambda x: x["metadata"].update(logical_tokens=32752),
            lambda x: x["metadata"].update(checkpoint_bytes=200),
            lambda x: x.update(file_sha256="c" * 64),
            lambda x: x["identity"].update(weights="d" * 64),
        ]:
            expected = self.fixture()
            actual = copy.deepcopy(expected)
            mutate(actual)
            with self.assertRaises(AssertionError):
                validate_export(actual, expected, 8189)


if __name__ == "__main__":
    unittest.main()
