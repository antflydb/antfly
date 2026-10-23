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

"""Bounded startup retries preserve grouped replay's visibility assertions."""

from types import SimpleNamespace

import pytest
import requests
import helpers
import test_grouped_sources as grouped


def http_error(status, body):
    response = requests.Response()
    response.status_code = status
    response._content = body.encode()
    return requests.HTTPError(response=response)


@pytest.fixture
def clock(monkeypatch):
    current = SimpleNamespace(now=0.0)

    def sleep(seconds):
        current.now += seconds

    fake = SimpleNamespace(monotonic=lambda: current.now, sleep=sleep)
    monkeypatch.setattr(grouped, "time", fake)
    monkeypatch.setattr(helpers, "time", fake)
    return current


class Api:
    def __init__(self, replies):
        self.replies = iter(replies)
        self.calls = []

    def batch_write_with_timeout(self, table, **kwargs):
        self.calls.append((table, kwargs))
        reply = next(self.replies)
        if isinstance(reply, Exception):
            raise reply
        return reply

    def debug_logs(self):
        return "grouped fixture diagnostics"


def write(api, **kwargs):
    return grouped._write_grouped_fixture(
        api,
        "turns",
        inserts={"a": {"text": "same source"}},
        sync_level="enrichments",
        **kwargs,
    )


def test_restart_admission_and_pending_visibility_require_success(clock):
    api = Api(
        [
            http_error(503, "write unavailable"),
            {"status": "committed_pending"},
            {"inserted": 1},
        ]
    )
    assert write(api) == {"inserted": 1}
    assert len(api.calls) == 3
    assert api.calls == [api.calls[0]] * 3
    assert api.calls[0][1]["sync_level"] == "enrichments"


def test_startup_retry_preserves_invalid_membership_rejection(clock):
    failure = http_error(400, "invalid batch request")
    api = Api([http_error(503, "write unavailable"), failure])
    with pytest.raises(requests.HTTPError) as rejected:
        write(api)
    assert rejected.value is failure
    assert len(api.calls) == 2
    assert api.calls[0] == api.calls[1]


@pytest.mark.parametrize(
    "failure",
    [
        http_error(
            503, "write committed locally; standby durability acknowledgment pending"
        ),
        http_error(409, "write outcome unknown"),
        http_error(400, "invalid batch request"),
        http_error(500, "internal failure"),
        requests.ConnectionError("reply lost"),
        requests.ReadTimeout("reply timed out"),
    ],
)
def test_unrecognized_or_transport_failure_is_not_replayed(clock, failure):
    api = Api([failure])
    with pytest.raises(type(failure)):
        write(api)
    assert len(api.calls) == 1


def test_repair_required_is_not_success_or_retry(clock):
    api = Api([{"status": "committed_repair_required"}])
    with pytest.raises(AssertionError, match="committed_repair_required"):
        write(api)
    assert len(api.calls) == 1


def test_admission_retries_stop_with_original_deadline(clock):
    api = Api([http_error(503, "write unavailable")] * 4)
    with pytest.raises(AssertionError, match="grouped fixture diagnostics"):
        write(api, timeout_s=0.25)
    assert len(api.calls) == 3
    assert [kwargs["timeout_s"] for _, kwargs in api.calls] == pytest.approx(
        [0.25, 0.15, 0.05]
    )
    assert clock.now == 0.25


def test_late_success_does_not_extend_visibility_budget(clock):
    class SlowApi(Api):
        def batch_write_with_timeout(self, table, **kwargs):
            clock.now += 1.0
            return super().batch_write_with_timeout(table, **kwargs)

    api = SlowApi([{"inserted": 1}])
    with pytest.raises(AssertionError):
        write(api, timeout_s=0.5)
    assert len(api.calls) == 1
