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

"""Deterministic coverage of the runtime publication retry contract."""

from types import SimpleNamespace

import conftest
import pytest
import requests


def response(status=503, retry_after=None, body=None):
    result = requests.Response()
    result.status_code = status
    result.url = "http://test/internal/v1/tables/docs/build"
    result.request = requests.Request("POST", result.url).prepare()
    result._content = (
        body or "publication authority changed or another builder is active; retry"
    ).encode()
    if retry_after is not None:
        result.headers["Retry-After"] = retry_after
    return result


@pytest.fixture
def publication_client(monkeypatch):
    fixture = conftest.serverless_api.__wrapped__(("http://test", None))
    api = next(fixture)
    clock = SimpleNamespace(now=0.0, sleeps=[])

    def sleep(delay):
        assert delay >= 0
        clock.sleeps.append(delay)
        clock.now += delay

    monkeypatch.setattr(conftest.time, "monotonic", lambda: clock.now)
    monkeypatch.setattr(conftest.time, "sleep", sleep)
    try:
        yield api, clock
    finally:
        fixture.close()


def table_client(api, backend="serverless"):
    return conftest.table_api.__wrapped__(
        SimpleNamespace(param=backend, getfixturevalue=lambda _: api)
    )


@pytest.mark.parametrize(
    "status,header,delay", [(409, None, 0.1), (503, "1", 1.0), (503, "0", 0.1)]
)
def test_build_retries_only_explicit_contention(
    publication_client, monkeypatch, status, header, delay
):
    api, clock = publication_client
    calls = []
    replies = iter([response(status, header), response(202, body='{"published":true}')])

    def post(url, **kwargs):
        calls.append((url, kwargs))
        return next(replies)

    monkeypatch.setattr(api.s, "post", post)
    assert api.build_table("docs", timeout_s=2) == {"published": True}
    assert clock.sleeps == [delay]
    assert [kwargs["timeout"] for _, kwargs in calls] == [2, 2 - delay]
    assert all(
        url.endswith("/tables/docs/build") and kwargs["json"] == {}
        for url, kwargs in calls
    )


@pytest.mark.parametrize(
    "status,header,body",
    [
        (
            503,
            None,
            "external source resolution is required; configure an external source resolver before publishing",
        ),
        (503, "", "unavailable"),
        (503, "-1", "unavailable"),
        (503, "1.5", "unavailable"),
        (503, "NaN", "unavailable"),
        (503, "inf", "unavailable"),
        (503, "garbage", "unavailable"),
        (500, "1", "build failed"),
        (422, "1", "sidecar build exceeds resource limits"),
        (404, None, "not found"),
    ],
)
def test_permanent_errors_fail_without_retry(
    publication_client, monkeypatch, status, header, body
):
    api, clock = publication_client
    failed = response(status, header, body)
    calls = []

    def post(*args, **kwargs):
        calls.append(args)
        return failed

    monkeypatch.setattr(api.s, "post", post)
    with pytest.raises(requests.HTTPError) as caught:
        table_client(api).publish_table("docs")
    assert caught.value.response is failed
    assert len(calls) == 1
    assert clock.sleeps == []


@pytest.mark.parametrize("header", ["2", "999999999999999999999999999999", "9" * 400])
def test_retry_after_cannot_exceed_deadline(publication_client, monkeypatch, header):
    api, clock = publication_client
    failed = response(503, header)
    calls = []

    def post(*args, **kwargs):
        calls.append(args)
        return failed

    monkeypatch.setattr(api.s, "post", post)
    assert table_client(api).publish_table("docs", timeout_s=2) is None
    assert len(calls) == 1
    assert clock.sleeps == []


def test_nested_publication_retries_share_one_budget(publication_client, monkeypatch):
    api, clock = publication_client
    failed = response(503, "1")
    timeouts = []

    def post(*args, **kwargs):
        timeouts.append(kwargs["timeout"])
        return failed

    monkeypatch.setattr(api.s, "post", post)
    assert table_client(api).publish_table("docs", timeout_s=2.5) is None
    assert timeouts == [2.5, 1.5, 0.5]
    assert clock.sleeps == [1.0, 1.0]


def test_sleep_overshoot_preserves_last_error(publication_client, monkeypatch):
    api, clock = publication_client
    failed = response(503, "1")
    calls = []

    def post(*args, **kwargs):
        calls.append(args)
        return failed

    monkeypatch.setattr(api.s, "post", post)
    monkeypatch.setattr(conftest.time, "sleep", lambda _: setattr(clock, "now", 3.0))
    with pytest.raises(requests.HTTPError) as caught:
        api.build_table("docs", timeout_s=2)
    assert caught.value.response is failed
    assert len(calls) == 1


def test_publication_polls_readiness_with_remaining_budget(
    publication_client, monkeypatch
):
    api, clock = publication_client
    calls = []
    ready = {"ready": True}
    replies = iter([response(503, "1"), response(202, body='{"published":true}')])
    monkeypatch.setattr(api.s, "post", lambda *args, **kwargs: next(replies))

    def get(url, **kwargs):
        calls.append((url, kwargs["timeout"]))
        clock.now += 0.25
        return response(200, body='{"ready":true}')

    monkeypatch.setattr(api.s, "get", get)
    monkeypatch.setattr(conftest, "ready_serverless_build_status", lambda value: value)
    assert table_client(api).publish_table("docs", timeout_s=2) == ready
    assert calls == [("http://test/internal/v1/tables/docs/build-status", 1.0)]
    assert clock.sleeps == [1.0]


def test_readiness_poll_does_not_restart_publication_deadline(
    publication_client, monkeypatch
):
    api, clock = publication_client
    calls = []

    def post(*args, **kwargs):
        calls.append(kwargs["timeout"])
        return response(202, body='{"published":true}')

    monkeypatch.setattr(api.s, "post", post)
    monkeypatch.setattr(api.s, "get", lambda *args, **kwargs: response(200, body="{}"))
    monkeypatch.setattr(conftest, "ready_serverless_build_status", lambda _: None)
    assert (
        table_client(api).publish_table("docs", timeout_s=1.25, interval_s=0.5) is None
    )
    assert calls == [1.25, 0.75, 0.25]
    assert clock.sleeps == [0.5, 0.5, 0.25]


def test_batch_mutations_are_never_replayed(publication_client, monkeypatch):
    api, clock = publication_client
    calls = []

    def post(*args, **kwargs):
        calls.append(args)
        return response(503, "1")

    monkeypatch.setattr(api.s, "post", post)
    with pytest.raises(requests.HTTPError):
        api.batch_table(
            "docs", inserts={"id": {"body": "data"}}, sync_level="full_index"
        )
    assert len(calls) == 1
    assert clock.sleeps == []


def test_stateful_publication_remains_noop():
    assert table_client(None, "stateful").publish_table("docs") is None


@pytest.mark.parametrize(
    "field,value",
    [
        ("timeout_s", 0),
        ("timeout_s", float("inf")),
        ("interval_s", 0),
        ("interval_s", float("nan")),
    ],
)
def test_invalid_retry_budget_is_rejected(publication_client, field, value):
    api, _ = publication_client
    with pytest.raises(ValueError):
        api.build_table("docs", **{field: value})
