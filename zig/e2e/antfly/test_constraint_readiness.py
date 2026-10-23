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

"""Keep online-merge setup reads inside their original readiness deadline."""

from types import SimpleNamespace

import pytest
import requests
import test_backup_restore as backups


@pytest.fixture
def clock(monkeypatch):
    current = SimpleNamespace(now=0.0)

    def advance(seconds):
        current.now += seconds

    monkeypatch.setattr(
        backups, "time", SimpleNamespace(monotonic=lambda: current.now, sleep=advance)
    )
    return current


def response(status=200, state="enforced"):
    return SimpleNamespace(
        status_code=status, json=lambda: {"state": state}, text="unexpected status"
    )


class Session:
    def __init__(self, clock, replies):
        self.clock = clock
        self.replies = iter(replies)
        self.calls = []

    def get(self, url, *, timeout):
        self.calls.append((url, timeout))
        reply = next(self.replies)
        if isinstance(reply, requests.Timeout):
            self.clock.now += timeout
        if isinstance(reply, Exception):
            raise reply
        return reply


def wait(session, **kwargs):
    return backups._wait_for_constraints_enforced(
        session, "http://cluster/api/v1", "parent", **kwargs
    )


def test_timeout_then_readiness_requires_enforced(clock):
    session = Session(
        clock,
        [
            requests.ReadTimeout(),
            response(404),
            response(409),
            response(503),
            response(state="building"),
            response(),
        ],
    )
    assert wait(session)
    assert (
        session.calls
        == [("http://cluster/api/v1/tables/parent/constraints/status", 5.0)] * 6
    )
    assert clock.now == 10.0


def test_timeouts_exhaust_one_budget_and_bound_final_request(clock):
    session = Session(clock, [requests.ReadTimeout()] * 3)
    assert not wait(session, timeout_s=12.5)
    assert [timeout for _, timeout in session.calls] == [5.0, 5.0, 0.5]
    assert clock.now == 12.5


@pytest.mark.parametrize("state", [None, "building", "failed", "ready"])
def test_other_states_never_count_as_enforced(clock, state):
    session = Session(clock, [response(state=state)] * 2)
    assert not wait(session, timeout_s=1.5)
    assert clock.now == 1.5


def test_late_success_does_not_extend_deadline(clock):
    class SlowSession:
        def get(self, url, *, timeout):
            clock.now += 6.0
            return response()

    assert not wait(SlowSession(), timeout_s=5.0)


@pytest.mark.parametrize("status", [400, 401, 403, 500])
def test_unexpected_http_status_fails_immediately(clock, status):
    session = Session(clock, [response(status)])
    with pytest.raises(AssertionError, match="unexpected status"):
        wait(session)
    assert len(session.calls) == 1
    assert clock.now == 0


def test_non_timeout_transport_error_is_not_hidden(clock):
    session = Session(clock, [requests.ConnectionError("connection lost")])
    with pytest.raises(requests.ConnectionError, match="connection lost"):
        wait(session)
    assert len(session.calls) == 1


def test_invalid_json_is_not_hidden(clock):
    def invalid_json():
        raise ValueError("invalid JSON")

    reply = response()
    reply.json = invalid_json
    session = Session(clock, [reply])
    with pytest.raises(ValueError, match="invalid JSON"):
        wait(session)
    assert len(session.calls) == 1


def test_zero_budget_does_not_issue_a_request(clock):
    session = Session(clock, [])
    assert not wait(session, timeout_s=0)
    assert session.calls == []
