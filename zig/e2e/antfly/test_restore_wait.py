# Copyright 2026 Antfly, Inc.
#
# Licensed under the Elastic License 2.0 (ELv2); you may not use this file
# except in compliance with the Elastic License 2.0. You may obtain a copy of
# the Elastic License 2.0 at
#
#     https://www.antfly.io/licensing/ELv2-license
#
# Unless required by applicable law or agreed to in writing, software distributed
# under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# Elastic License 2.0 for the specific language governing permissions and
# limitations.

"""Restore polling must retain diagnostics without treating retries as progress."""

from types import SimpleNamespace

import conftest
import pytest


def test_restore_retry_churn_keeps_deadline_and_reports_bounded_logs(monkeypatch):
    now = 0.0
    attempts = 0

    def sleep(seconds):
        nonlocal now
        now += seconds

    def get_job(path):
        nonlocal attempts
        assert path == "/restore/jobs/123"
        attempts += 1
        return {
            "phase": "running",
            "attempt_id": attempts,
            "error": "RestoreValidationPending",
        }

    monkeypatch.setattr(
        conftest, "time", SimpleNamespace(monotonic=lambda: now, sleep=sleep)
    )
    logs = "old log\n" * conftest.FAILURE_LOG_TAIL_LIMIT + "last owner diagnostic"
    with pytest.raises(AssertionError) as failure:
        conftest._wait_for_restore_job(
            get_job, {"job_id": "123"}, timeout_s=0.3, debug_logs=lambda: logs
        )
    message = str(failure.value)
    assert 0.3 <= now < 0.5
    assert attempts > 1
    assert "RestoreValidationPending" in message
    assert "last owner diagnostic" in message
    assert len(message) < conftest.FAILURE_LOG_TAIL_LIMIT + 1000


def test_restore_success_returns_result():
    assert conftest._wait_for_restore_job(
        lambda _: {"phase": "succeeded", "result": {"restored": True}},
        {"job_id": "123"},
    ) == {"restored": True}
