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

"""Fault injection for the pagination regression's read-only retry boundary."""

from types import SimpleNamespace

import pytest
import requests

import privacy_findings_read as reader


class Clock:
    def __init__(self):
        self.now = 0.0
        self.sleeps = []

    def monotonic(self):
        return self.now

    def sleep(self, delay):
        assert delay > 0
        self.sleeps.append(delay)
        self.now += delay


class Response:
    def __init__(self, status, body, headers=None):
        self.status_code = status
        self.body = body
        self.headers = headers or {}
        self.closed = False

    def json(self):
        if isinstance(self.body, Exception):
            raise self.body
        return self.body

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}", response=self)

    def close(self):
        self.closed = True


@pytest.fixture
def transport(monkeypatch):
    clock = Clock()
    monkeypatch.setattr(reader, "time", clock)
    responses = []
    calls = []

    def request(method, url, **kwargs):
        calls.append((method, url, kwargs))
        result, elapsed = responses.pop(0)
        clock.now += elapsed
        if isinstance(result, Exception):
            raise result
        return result

    return SimpleNamespace(
        clock=clock,
        responses=responses,
        calls=calls,
        session=SimpleNamespace(request=request),
    )


@pytest.mark.parametrize("method", ["GET", "POST"])
@pytest.mark.parametrize(
    "status,body",
    [
        (503, {"code": "privacy_storage_unavailable", "retryable": True}),
        (503, {"code": "privacy_leader_unavailable", "retryable": True}),
        (504, {"code": "privacy_timeout"}),
    ],
)
def test_findings_retry_preserves_method_cursor_and_remaining_budget(
    transport, method, status, body
):
    failed = Response(status, body, {"Retry-After": "3"})
    page = {"findings": [], "next_cursor": None}
    success = Response(200, page)
    transport.responses.extend([(failed, 2), (success, 0)])
    payload = {"cursor": "same-opaque-cursor", "limit": 1}
    assert (
        reader.read_findings_page(
            transport.session,
            "http://antfly.test/privacy",
            payload,
            deadline=10,
            method=method,
        )
        is page
    )
    assert payload == {"cursor": "same-opaque-cursor", "limit": 1}
    assert transport.clock.sleeps == [3]
    assert len(transport.calls) == 2
    for call, timeout in zip(transport.calls, [10, 5], strict=True):
        assert call == (
            method,
            "http://antfly.test/privacy/findings",
            {
                "params" if method == "GET" else "json": payload,
                "timeout": timeout,
            },
        )
    assert failed.closed and success.closed


@pytest.mark.parametrize(
    "status,body,error",
    [
        (403, {"code": "privacy_admin_required"}, requests.HTTPError),
        (409, {"code": "privacy_mutation_outcome_unknown"}, requests.HTTPError),
        (422, {"code": "InvalidPrivacyCursor"}, requests.HTTPError),
        (
            503,
            {"code": "privacy_storage_unavailable", "retryable": False},
            requests.HTTPError,
        ),
        (503, {"code": "other", "retryable": True}, requests.HTTPError),
        (503, ["not an error object"], requests.HTTPError),
        (504, {"code": "other_timeout"}, requests.HTTPError),
        (200, ValueError("malformed JSON"), ValueError),
        (503, ValueError("malformed JSON"), ValueError),
        (200, [], AssertionError),
        (202, {"findings": []}, AssertionError),
    ],
)
def test_findings_permanent_or_malformed_responses_are_not_retried(
    transport, status, body, error
):
    response = Response(status, body)
    transport.responses.append((response, 0))
    with pytest.raises(error):
        reader.read_findings_page(
            transport.session, "http://antfly.test/privacy", {}, deadline=10
        )
    assert len(transport.calls) == 1
    assert not transport.clock.sleeps
    assert response.closed


def test_findings_transport_failure_is_not_reclassified(transport):
    transport.responses.append((requests.ReadTimeout("transport timeout"), 1))
    with pytest.raises(requests.ReadTimeout):
        reader.read_findings_page(
            transport.session, "http://antfly.test/privacy", {}, deadline=10
        )
    assert len(transport.calls) == 1
    assert not transport.clock.sleeps


def test_findings_retry_budget_expires_without_another_attempt(transport):
    responses = [Response(504, {"code": "privacy_timeout"}) for _ in range(3)]
    transport.responses.extend((response, 0) for response in responses)
    with pytest.raises(TimeoutError, match="deadline exhausted.*privacy_timeout"):
        reader.read_findings_page(
            transport.session, "http://antfly.test/privacy", {}, deadline=1
        )
    assert transport.clock.now == pytest.approx(1)
    assert transport.clock.sleeps == pytest.approx([0.2, 0.4, 0.4])
    assert [call[2]["timeout"] for call in transport.calls] == pytest.approx(
        [1, 0.8, 0.4]
    )
    assert all(response.closed for response in responses)


def test_findings_retry_after_cannot_extend_deadline(transport):
    response = Response(
        503,
        {"code": "privacy_storage_unavailable", "retryable": True},
        {"Retry-After": "9999"},
    )
    transport.responses.append((response, 0.2))
    with pytest.raises(TimeoutError, match="deadline exhausted"):
        reader.read_findings_page(
            transport.session, "http://antfly.test/privacy", {}, deadline=0.25
        )
    assert transport.clock.now == pytest.approx(0.25)
    assert len(transport.calls) == 1
    assert transport.calls[0][2]["timeout"] == 0.25
    assert response.closed


@pytest.mark.parametrize("header", ["nan", "inf", "-1", "bad"])
def test_findings_invalid_retry_after_uses_bounded_backoff(transport, header):
    failed = Response(504, {"code": "privacy_timeout"}, {"Retry-After": header})
    transport.responses.extend([(failed, 0), (Response(200, {}), 0)])
    reader.read_findings_page(
        transport.session, "http://antfly.test/privacy", {}, deadline=1
    )
    assert transport.clock.sleeps == [0.2]


def test_findings_deadline_is_shared_across_pages_and_rejects_late_success(transport):
    first, late = Response(200, {}), Response(200, {})
    transport.responses.extend([(first, 0.2), (late, 0.06)])
    reader.read_findings_page(
        transport.session, "http://antfly.test/privacy", {}, deadline=0.25
    )
    with pytest.raises(TimeoutError, match="deadline exhausted"):
        reader.read_findings_page(
            transport.session, "http://antfly.test/privacy", {}, deadline=0.25
        )
    assert len(transport.calls) == 2
    assert transport.calls[1][2]["timeout"] == pytest.approx(0.05)
    assert first.closed and late.closed


@pytest.mark.parametrize(
    "method,deadline,error",
    [
        ("PUT", 1, ValueError),
        ("POST", float("inf"), ValueError),
        ("GET", float("nan"), ValueError),
        ("POST", 0, TimeoutError),
    ],
)
def test_findings_invalid_or_expired_budget_never_sends_request(
    transport, method, deadline, error
):
    with pytest.raises(error):
        reader.read_findings_page(
            transport.session,
            "http://antfly.test/privacy",
            {},
            method=method,
            deadline=deadline,
        )
    assert not transport.calls
