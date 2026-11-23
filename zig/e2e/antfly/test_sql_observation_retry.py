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

"""Pure predicate checks; these tests do not start an Antfly process."""

import unittest

from sql_observation_retry import definite_observation_abort


class DefiniteObservationAbortTest(unittest.TestCase):
    def setUp(self):
        # Public SQLDiagnostic encoding: nullable optional fields may be omitted.
        self.wire = {
            "code": "40001",
            "message": "The mutation conflicted with a concurrent change and was not committed.",
            "retryable": True,
            "transaction_status": "idle",
        }

    def test_known_aborted_pure_row_observation(self):
        self.assertTrue(
            definite_observation_abort(409, self.wire, pure_observation=True)
        )
        self.assertTrue(
            definite_observation_abort(
                409, dict(self.wire, transaction_id=None), pure_observation=True
            )
        )

    def test_never_opt_in_mutations_or_other_statuses(self):
        self.assertFalse(
            definite_observation_abort(409, self.wire, pure_observation=False)
        )
        for status in (200, 202, 400, 503):
            self.assertFalse(
                definite_observation_abort(status, self.wire, pure_observation=True)
            )

    def test_missing_or_uncertain_proof(self):
        for key in ("code", "retryable", "transaction_status"):
            value = dict(self.wire)
            value.pop(key)
            self.assertFalse(
                definite_observation_abort(409, value, pure_observation=True)
            )
        for key, value in (
            ("code", "40003"),
            ("retryable", False),
            ("retryable", 1),
            ("transaction_status", "failed"),
            ("transaction_status", "in_transaction"),
        ):
            self.assertFalse(
                definite_observation_abort(
                    409, dict(self.wire, **{key: value}), pure_observation=True
                )
            )

    def test_receipts_ids_and_results_never_qualify(self):
        for key, value in (
            ("transaction_id", "a" * 32),
            ("mutation_outcome", "committed"),
            ("admission_outcome", "pending"),
            ("status", "aborted"),
            ("ddl_receipt", {}),
            ("catalog_receipt", {}),
            ("rows", []),
            ("columns", []),
            ("rows_affected", 0),
            ("command_tag", "SELECT"),
        ):
            with self.subTest(key=key):
                self.assertFalse(
                    definite_observation_abort(
                        409, dict(self.wire, **{key: value}), pure_observation=True
                    )
                )


if __name__ == "__main__":
    unittest.main()
