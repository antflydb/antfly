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

"""Commit disposition survives native decoding and DB-API error mapping."""

import json

import pytest

from antfly_embedded import _sql, dbapi


@pytest.mark.parametrize("outcome", ["applied", "not_applied", "unknown", None])
def test_commit_diagnostic_keeps_disposition_and_token(monkeypatch, outcome):
    payload = {
        "error": {"code": "40003", "message": "Reconcile this transaction.", "retryable": False},
        "transaction_id": "ab" * 16,
    }
    if outcome is not None:
        payload["commit_outcome"] = outcome
    monkeypatch.setattr(_sql._ffi, "take_buffer", lambda _: json.dumps(payload))
    with pytest.raises(_sql.SQLStateError) as raised:
        _sql.output(7, None)
    error = raised.value
    assert error.commit_outcome == outcome
    assert error.transaction_id == "ab" * 16
    mapped = dbapi._mapped(error)
    assert mapped.sqlstate == "40003"
    assert mapped.commit_outcome == outcome
    assert mapped.transaction_id == "ab" * 16
