# Copyright 2026 Antfly, Inc.
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
from unittest.mock import patch

import pytest

from antfly import AntflyClient, AntflyException, SQLNotificationPollRequest


def test_notification_pages_preserve_acknowledgements_without_replaying_sql():
    client = AntflyClient(base_url="http://localhost:8080")
    connection = "a" * 32
    cursor = "00000000000000010001"
    response = {
        "notifications": [{"channel": "events", "payload": "updated", "sender_pid": 42}],
        "cursor": cursor,
        "pending": False,
    }
    with patch.object(client, "_request", return_value=response) as request:
        page = client.poll_sql_notifications(connection, SQLNotificationPollRequest(acknowledge=cursor, limit=1))
    assert page.notifications[0].payload == "updated"
    assert page.cursor == cursor
    assert request.call_count == 1
    assert request.call_args.args == ("POST", f"/db/v1/sql/connections/{connection}/notifications")
    assert json.loads(request.call_args.kwargs["content"]) == {"acknowledge": cursor, "limit": 1}
    assert request.call_args.kwargs["follow_redirects"] is False
    assert request.call_args.kwargs["_max_response_bytes"] == 1 << 20


@pytest.mark.parametrize(
    "control",
    [
        SQLNotificationPollRequest(limit=0),
        SQLNotificationPollRequest(limit=129),
        SQLNotificationPollRequest(limit=True),
        SQLNotificationPollRequest(acknowledge="bad"),
    ],
)
def test_invalid_notification_requests_never_dispatch(control):
    client = AntflyClient(base_url="http://localhost:8080")
    with patch.object(client, "_request") as transport:
        with pytest.raises(AntflyException):
            client.poll_sql_notifications("a" * 32, control)
    transport.assert_not_called()


@pytest.mark.parametrize(
    "response",
    [
        {
            "notifications": [{"channel": "events", "payload": "x", "sender_pid": True}],
            "cursor": "0" * 20,
            "pending": False,
        },
        {"notifications": [{"channel": "events", "payload": "x", "sender_pid": 42}], "pending": False},
        {
            "notifications": [{"channel": "events", "payload": "x" * 8000, "sender_pid": 42}],
            "cursor": "0" * 20,
            "pending": False,
        },
    ],
)
def test_invalid_notification_responses_are_not_exposed(response):
    client = AntflyClient(base_url="http://localhost:8080")
    with patch.object(client, "_request", return_value=response):
        with pytest.raises(AntflyException):
            client.poll_sql_notifications("a" * 32)


def test_generated_notification_poll_bounds_stream_and_refuses_redirects():
    import httpx

    from antfly.client_generated.api.data_operations import poll_sql_notifications
    from antfly.client_generated.client import AuthenticatedClient

    class Stream(httpx.SyncByteStream):
        read = 0
        closed = False

        def __iter__(self):
            for _ in range(300):
                self.read += 64 << 10
                yield b" " * (64 << 10)

        def close(self):
            self.closed = True

    stream = Stream()
    calls = []

    def respond(request):
        calls.append(request)
        if len(calls) == 1:
            return httpx.Response(
                307,
                headers={"Location": "/replayed"},
                json={"code": "40003", "message": "observe only", "retryable": False},
            )
        return httpx.Response(200, stream=stream)

    generated = AuthenticatedClient(base_url="http://sql.test", token="token")
    with httpx.Client(
        base_url="http://sql.test", transport=httpx.MockTransport(respond), follow_redirects=True
    ) as http:
        generated.set_httpx_client(http)
        page = poll_sql_notifications.sync_detailed("a" * 32, client=generated, body=SQLNotificationPollRequest())
        assert page.status_code == 307
        assert len(calls) == 1
        with pytest.raises(ValueError, match="response exceeds 1 MiB"):
            poll_sql_notifications.sync_detailed("a" * 32, client=generated, body=SQLNotificationPollRequest())
        assert http.follow_redirects is True
    assert stream.closed
    assert stream.read == (1 << 20) + (64 << 10)
    assert len(calls) == 2


@pytest.mark.asyncio
async def test_generated_notification_poll_async_bounds_stream_and_refuses_redirects():
    import httpx

    from antfly.client_generated.api.data_operations import poll_sql_notifications
    from antfly.client_generated.client import AuthenticatedClient

    class Stream(httpx.AsyncByteStream):
        read = 0
        closed = False

        async def __aiter__(self):
            for _ in range(300):
                self.read += 64 << 10
                yield b" " * (64 << 10)

        async def aclose(self):
            self.closed = True

    stream = Stream()
    calls = []

    def respond(request):
        calls.append(request)
        if len(calls) == 1:
            return httpx.Response(
                307,
                headers={"Location": "/replayed"},
                json={"code": "40003", "message": "observe only", "retryable": False},
            )
        return httpx.Response(200, stream=stream)

    generated = AuthenticatedClient(base_url="http://sql.test", token="token")
    async with httpx.AsyncClient(
        base_url="http://sql.test", transport=httpx.MockTransport(respond), follow_redirects=True
    ) as http:
        generated.set_async_httpx_client(http)
        page = await poll_sql_notifications.asyncio_detailed(
            "a" * 32, client=generated, body=SQLNotificationPollRequest()
        )
        assert page.status_code == 307
        assert len(calls) == 1
        with pytest.raises(ValueError, match="response exceeds 1 MiB"):
            await poll_sql_notifications.asyncio_detailed("a" * 32, client=generated, body=SQLNotificationPollRequest())
        assert http.follow_redirects is True
    assert stream.closed
    assert stream.read == (1 << 20) + (64 << 10)
    assert len(calls) == 2
