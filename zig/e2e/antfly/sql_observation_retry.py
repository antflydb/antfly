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

"""Retry proof for explicitly pure row observations in the lifetime campaign.

This is test-client policy, not SQL replay middleware. Callers must opt in only
for plain row SELECTs without functions or effects. The enclosing caller retains
its existing absolute deadline and delay. Unknown outcomes never qualify.
"""


def definite_observation_abort(status, result, *, pure_observation):
    return (
        pure_observation is True
        and status == 409
        and result.get("code") == "40001"
        and result.get("retryable") is True
        and result.get("transaction_status") == "idle"
        and all(
            result.get(key) is None
            for key in (
                "transaction_id",
                "mutation_outcome",
                "admission_outcome",
                "status",
                "ddl_receipt",
                "catalog_receipt",
                "rows",
                "columns",
                "rows_affected",
                "command_tag",
            )
        )
    )
