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
