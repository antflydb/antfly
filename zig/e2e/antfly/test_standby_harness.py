# Copyright 2026 Antfly, Inc.
#
# Licensed under the Elastic License 2.0 (ELv2); you may not use this file
# except in compliance with the Elastic License 2.0. You may obtain a copy of
# the Elastic License 2.0 at
#
#     https://www.antfly.io/licensing/ELv2-license
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the Elastic License 2.0 is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the Elastic License 2.0 for the specific language governing permissions
# and limitations.

"""Fast checks for HA restart readiness and bounded failure diagnostics."""

from types import SimpleNamespace

import pytest
import test_standby as standby_tests


@pytest.fixture
def readiness_cluster(monkeypatch):
    clock = [0.0]
    monkeypatch.setattr(standby_tests.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(
        standby_tests.time,
        "sleep",
        lambda duration: clock.__setitem__(0, clock[0] + duration),
    )
    snapshots = []
    timeouts = []

    def read_status(path, *, upstream_lsn, request_timeout_s):
        assert path == "/standby/status"
        assert upstream_lsn == 10
        timeouts.append(request_timeout_s)
        snapshot = snapshots.pop(0) if len(snapshots) > 1 else snapshots[0]
        return {"snapshot": snapshot}

    cluster = SimpleNamespace(
        standby=SimpleNamespace(
            proc=SimpleNamespace(poll=lambda: None),
            admin_get_response=read_status,
            _check=lambda response: response,
        ),
        debug_logs=lambda: "primary and standby diagnostics",
    )
    return cluster, snapshots, timeouts


def snapshot(*, applied_lsn=10, last_success_ns=None, last_error=None):
    return {
        "received_lsn": 10,
        "applied_lsn": applied_lsn,
        "last_success_ns": last_success_ns,
        "last_error": last_error,
    }


@pytest.mark.parametrize(
    "require_live_replication, expected_calls", [(False, 1), (True, 3)]
)
def test_restored_progress_requires_a_live_round_before_sync_writes(
    readiness_cluster, require_live_replication, expected_calls
):
    cluster, snapshots, timeouts = readiness_cluster
    snapshots.extend(
        [
            snapshot(),
            snapshot(applied_lsn=9, last_success_ns=1),
            snapshot(last_success_ns=2),
        ]
    )
    result = standby_tests._wait_for_standby_applied(
        cluster, 10, require_live_replication=require_live_replication
    )
    assert result["applied_lsn"] == 10
    assert len(timeouts) == expected_calls


@pytest.mark.parametrize(
    "last_success_ns, last_error", [(None, None), (0, None), (1, "ConnectionRefused")]
)
def test_startup_wait_times_out_with_bounded_requests_and_diagnostics(
    readiness_cluster, last_success_ns, last_error
):
    cluster, snapshots, timeouts = readiness_cluster
    snapshots.append(snapshot(last_success_ns=last_success_ns, last_error=last_error))
    with pytest.raises(AssertionError, match="require_live_replication=True") as failed:
        standby_tests._wait_for_standby_applied(
            cluster, 10, timeout_s=0.5, require_live_replication=True
        )
    assert timeouts == [0.5, 0.25]
    assert "primary and standby diagnostics" in str(failed.value)


def test_startup_wait_fails_immediately_when_standby_exits(readiness_cluster):
    cluster, _, timeouts = readiness_cluster
    cluster.standby.proc.poll = lambda: 1
    with pytest.raises(AssertionError, match="standby exited"):
        standby_tests._wait_for_standby_applied(
            cluster, 10, require_live_replication=True
        )
    assert timeouts == []
