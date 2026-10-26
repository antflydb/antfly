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

import json
import unittest

from qualify_native_cancellation import fitting_cancellation_evidence


def events(error="Cancelled", phase="fit", attempted=4, completed=1):
    return b"\n".join(
        json.dumps({"msg": message}).encode()
        for message in [
            f"attention_match_fit_error error={error}",
            f"attention_match_fit_rejected phase={phase} elapsed_ns=123456789 "
            f"attempted_chunks={attempted} completed_chunks={completed}",
        ]
    )


class FittingCancellationEvidenceTests(unittest.TestCase):
    def test_actual_fitting_cancellation_counts_started_work(self):
        result = fitting_cancellation_evidence(events())
        self.assertEqual(result["attempted_chunks"], 4)
        self.assertEqual(result["completed_chunks"], 1)

    def test_timer_or_generic_cancel_message_is_not_phase_evidence(self):
        with self.assertRaises(AssertionError):
            fitting_cancellation_evidence(
                b'{"msg":"request cancelled after 40 seconds"}'
            )

    def test_failure_in_other_phase_or_before_first_chunk_does_not_count(self):
        for data in [
            events(phase="snapshot"),
            events(attempted=0, completed=0),
            events(completed=5),
        ]:
            with self.subTest(data=data), self.assertRaises(AssertionError):
                fitting_cancellation_evidence(data)

    def test_driver_failure_cannot_be_reclassified_as_cancellation(self):
        with self.assertRaisesRegex(AssertionError, "reason other than cancellation"):
            fitting_cancellation_evidence(events(error="CudaDriverError"))

    def test_ambiguous_multiple_requests_do_not_qualify(self):
        with self.assertRaisesRegex(AssertionError, "ambiguous"):
            fitting_cancellation_evidence(events() + b"\n" + events())


if __name__ == "__main__":
    unittest.main()
