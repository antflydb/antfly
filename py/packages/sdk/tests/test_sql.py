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

import gzip
import json
from unittest.mock import patch

import httpx
import pytest

from antfly import AntflyClient, AntflyException, SQLExecutionError, SQLRequest


def test_sql_bound_parameters_and_exact_integer_results():
    client = AntflyClient(base_url="http://localhost:8080")
    result = {
        "columns": [{"name": "id", "type": "integer"}, {"name": "id", "type": "json"}],
        "rows": [["9223372036854775807", {"id": 9223372036854775807}]],
        "rows_affected": 0,
        "command_tag": "SELECT 1",
    }
    with patch.object(client, "_request", return_value=result) as request:
        response = client.execute_sql(SQLRequest(statement="SELECT $1", parameters=[9223372036854775807]))
    assert response.rows == result["rows"]
    assert request.call_args.args == ("POST", "/db/v1/sql")
    assert json.loads(request.call_args.kwargs["content"])["parameters"] == [9223372036854775807]
    assert request.call_args.kwargs["follow_redirects"] is False
    assert request.call_args.kwargs["_max_response_bytes"] == 16 << 20


def test_sql_rejects_malformed_row_width():
    client = AntflyClient(base_url="http://localhost:8080")
    with patch.object(client, "_request", return_value={"columns": [], "rows": [[1]]}):
        with pytest.raises(AntflyException, match="row width"):
            client.execute_sql(SQLRequest(statement="SELECT 1"))


def test_sql_keeps_native_reconciliation_receipt():
    diagnostic = {
        "code": "40003",
        "message": "do not replay",
        "retryable": False,
        "transaction_id": "0123456789abcdef0123456789abcdef",
    }
    client = AntflyClient(base_url="http://localhost:8080")
    transport = httpx.MockTransport(lambda _: httpx.Response(409, json=diagnostic))
    with httpx.Client(base_url=client.base_url, transport=transport) as http:
        client._client.set_httpx_client(http)
        with pytest.raises(SQLExecutionError) as caught:
            client.execute_sql(SQLRequest(statement="DELETE FROM docs"))
    assert caught.value.diagnostic.transaction_id == diagnostic["transaction_id"]
    assert caught.value.diagnostic.retryable is False


def test_sql_keeps_committed_repair_receipt():
    client = AntflyClient(base_url="http://localhost:8080")
    result = {
        "columns": [],
        "rows": [],
        "rows_affected": 1,
        "command_tag": "DELETE 1",
        "mutation_outcome": "committed_repair_required",
        "transaction_id": "0123456789abcdef0123456789abcdef",
    }
    with patch.object(client, "_request", return_value=result):
        response = client.execute_sql(SQLRequest(statement="DELETE FROM docs WHERE _id = 'a'"))
    assert response.transaction_id == result["transaction_id"]
    assert response.mutation_outcome == result["mutation_outcome"]


@pytest.mark.parametrize("value", [float("nan"), float("inf"), -float("inf"), "x" * (4 << 20)])
def test_sql_rejects_invalid_or_oversized_requests_before_dispatch(value):
    client = AntflyClient(base_url="http://localhost:8080")
    with patch.object(client, "_request") as request:
        with pytest.raises(AntflyException, match="Invalid SQL request"):
            client.execute_sql(SQLRequest(statement="SELECT $1", parameters=[value]))
    request.assert_not_called()


def test_generated_sql_disables_redirects_on_borrowed_client():
    from antfly.client_generated.api.data_operations import execute_sql
    from antfly.client_generated.client import AuthenticatedClient

    calls = []

    def respond(request):
        calls.append(request)
        return httpx.Response(307, headers={"Location": "/replayed"})

    generated = AuthenticatedClient(base_url="http://sql.test", token="token")
    with httpx.Client(
        base_url="http://sql.test", transport=httpx.MockTransport(respond), follow_redirects=True
    ) as http:
        generated.set_httpx_client(http)
        result = execute_sql.sync_detailed(client=generated, body=SQLRequest(statement="DELETE FROM docs"))
        assert result.status_code == 307
        assert http.follow_redirects is True
    assert len(calls) == 1


def test_generated_sql_parses_compressed_response_once():
    from antfly.client_generated.api.data_operations import execute_sql
    from antfly.client_generated.client import AuthenticatedClient

    content = json.dumps({"columns": [], "rows": [], "rows_affected": 0, "command_tag": "SELECT 0"}).encode()
    generated = AuthenticatedClient(base_url="http://sql.test", token="token")
    transport = httpx.MockTransport(
        lambda _: httpx.Response(
            200,
            content=gzip.compress(content),
            headers={"Content-Encoding": "gzip", "Content-Type": "application/json"},
        )
    )
    with httpx.Client(base_url="http://sql.test", transport=transport) as http:
        generated.set_httpx_client(http)
        response = execute_sql.sync_detailed(client=generated, body=SQLRequest(statement="SELECT * FROM docs"))
    assert response.parsed.command_tag == "SELECT 0"


def test_generated_sql_bounds_requests_and_streamed_responses():
    from antfly.client_generated.api.data_operations import execute_sql
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
        return httpx.Response(200, stream=stream)

    generated = AuthenticatedClient(base_url="http://sql.test", token="token")
    with httpx.Client(base_url="http://sql.test", transport=httpx.MockTransport(respond)) as http:
        generated.set_httpx_client(http)
        with pytest.raises(ValueError, match="request exceeds 4 MiB"):
            execute_sql.sync_detailed(client=generated, body=SQLRequest(statement="x" * (4 << 20)))
        assert not calls
        with pytest.raises(ValueError, match="response exceeds 16 MiB"):
            execute_sql.sync_detailed(client=generated, body=SQLRequest(statement="SELECT * FROM docs"))
    assert stream.closed
    assert stream.read == (16 << 20) + (64 << 10)
    assert len(calls) == 1


@pytest.mark.asyncio
async def test_generated_sql_async_bounds_and_no_redirects():
    from antfly.client_generated.api.data_operations import execute_sql
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
        return (
            httpx.Response(307, headers={"Location": "/replayed"})
            if len(calls) == 1
            else httpx.Response(200, stream=stream)
        )

    generated = AuthenticatedClient(base_url="http://sql.test", token="token")
    async with httpx.AsyncClient(
        base_url="http://sql.test", transport=httpx.MockTransport(respond), follow_redirects=True
    ) as http:
        generated.set_async_httpx_client(http)
        result = await execute_sql.asyncio_detailed(client=generated, body=SQLRequest(statement="DELETE FROM docs"))
        assert result.status_code == 307
        assert len(calls) == 1
        with pytest.raises(ValueError, match="request exceeds 4 MiB"):
            await execute_sql.asyncio_detailed(client=generated, body=SQLRequest(statement="x" * (4 << 20)))
        assert len(calls) == 1
        with pytest.raises(ValueError, match="response exceeds 16 MiB"):
            await execute_sql.asyncio_detailed(client=generated, body=SQLRequest(statement="SELECT * FROM docs"))
    assert stream.closed
    assert stream.read == (16 << 20) + (64 << 10)
    assert len(calls) == 2
