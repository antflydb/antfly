"""Successful but unusable mutation replies must remain distinguishable."""

import httpx
import pytest

from antfly import AntflyClient, AntflyResponseError


@pytest.mark.parametrize(
    "body", [b"{", b"[]", b"null", b"{}", b'{"status":"future_unknown"}', b'{"status":"committed","failure":null}']
)
def test_batch_unusable_reply_is_typed_and_never_replayed(body):
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(200, content=body)

    client = AntflyClient("http://test")
    client._client.set_httpx_client(httpx.Client(base_url="http://test", transport=httpx.MockTransport(handler)))
    try:
        with pytest.raises(AntflyResponseError):
            client.batch("t", inserts={"a": {"text": "a"}})
        assert len(calls) == 1
    finally:
        client.close()


def test_successful_oversized_reply_is_typed():
    client = AntflyClient("http://test", max_json_response_bytes=8)
    client._client.set_httpx_client(
        httpx.Client(
            base_url="http://test",
            transport=httpx.MockTransport(lambda _: httpx.Response(200, content=b'{"status":"committed"}')),
        )
    )
    try:
        with pytest.raises(AntflyResponseError) as caught:
            client.batch("t", inserts={"a": {}})
        assert caught.value.status_code == 200
    finally:
        client.close()


@pytest.mark.parametrize(
    "status,code,retryable", [(403, "row_policy_denied", False), (503, "transaction_precommit_aborted", True)]
)
def test_generated_commit_errors_preserve_content_type(status, code, retryable):
    from antfly.client_generated.api.data_operations import commit_transaction
    from antfly.client_generated.client import Client
    from antfly.client_generated.models import Error, TransactionWriteError

    client = Client(base_url="http://test")
    parsed = commit_transaction._parse_response(
        client=client,
        response=httpx.Response(
            status, json={"status": "aborted", "code": code, "message": "rejected", "retryable": retryable}
        ),
    )
    assert isinstance(parsed, TransactionWriteError)
    assert (parsed.status, parsed.code, parsed.retryable) == ("aborted", code, retryable)
    legacy = commit_transaction._parse_response(
        client=client, response=httpx.Response(status, json={"error": "unavailable"})
    )
    assert isinstance(legacy, Error)
    assert (
        commit_transaction._parse_response(client=client, response=httpx.Response(status, text="unavailable"))
        == "unavailable"
    )


def test_generated_session_commit_conflict_preserves_retry_diagnostic():
    from antfly.client_generated.api.data_operations import commit_transaction_session
    from antfly.client_generated.client import Client

    client = Client(base_url="http://test")
    message = "transaction commit retry body does not match the sealed request"
    assert (
        commit_transaction_session._parse_response(client=client, response=httpx.Response(409, text=message)) == message
    )
    outcome = commit_transaction_session._parse_response(
        client=client, response=httpx.Response(409, json={"transaction_id": "123", "status": "aborted"})
    )
    assert outcome.transaction_id == "123"


@pytest.mark.parametrize("operation", ["commit_transaction", "multi_batch_write"])
def test_generated_commit_preparation_conflict_preserves_diagnostic(operation):
    from importlib import import_module

    from antfly.client_generated.client import Client

    module = import_module(f"antfly.client_generated.api.data_operations.{operation}")
    response = httpx.Response(409, text="PreparedGenerationChanged")
    assert module._parse_response(client=Client(base_url="http://test"), response=response) == response.text
