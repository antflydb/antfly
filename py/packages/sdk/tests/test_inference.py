import json
from collections.abc import Iterator

import httpx
import pytest

import antfly.client as client_module
from antfly import AntflyClient, AntflyException, InferenceAPIError, InferenceCapacityError
from antfly.client_generated.models import (
    InferenceChatMessage,
    InferenceGenerateChatTemplateKwargs,
    InferenceGenerateRequest,
    InferenceRole,
)


class ChunkStream(httpx.SyncByteStream):
    def __init__(self, chunks: list[bytes]) -> None:
        self.chunks = chunks
        self.closed = False

    def __iter__(self) -> Iterator[bytes]:
        yield from self.chunks

    def close(self) -> None:
        self.closed = True


def generation_request() -> InferenceGenerateRequest:
    return InferenceGenerateRequest(
        model="gemma",
        messages=[InferenceChatMessage(role=InferenceRole.USER, content="hello")],
    )


def test_generate_request_preserves_explicit_thinking_false() -> None:
    omitted = generation_request().to_dict()
    assert "chat_template_kwargs" not in omitted

    request = generation_request()
    request.chat_template_kwargs = InferenceGenerateChatTemplateKwargs(enable_thinking=False)
    assert request.to_dict()["chat_template_kwargs"] == {"enable_thinking": False}


def install_transport(client: AntflyClient, handler: httpx.MockTransport) -> None:
    generated = client._client
    headers = generated.get_httpx_client().headers
    generated.set_httpx_client(httpx.Client(base_url="http://test", headers=headers, transport=handler))


def test_generate_and_stream_reuse_auth_and_parse_framed_sse() -> None:
    requests: list[dict[str, object]] = []
    stream = ChunkStream(
        [
            bytes([byte])
            for byte in (
                ': heartbeat\r\n\r\nevent: message\r\ndata: {"id":"chatcmpl-stream","object":"chat.completion.chunk",\r\ndata: "created":1,"model":"gemma","choices":[{"index":0,"delta":{"content":"hé🐜"}}]}\r\n\r\ndata: [DONE]\r\n\r\n'
            ).encode()
        ]
    )

    def handle(request: httpx.Request) -> httpx.Response:
        assert request.headers["authorization"] == "Bearer secret"
        assert request.headers["accept"] in {"application/json", "text/event-stream"}
        requests.append(json.loads(request.read()))
        if requests[-1]["stream"] is True:
            return httpx.Response(
                200,
                headers={"Content-Type": "text/event-stream; charset=utf-8"},
                stream=stream,
            )
        return httpx.Response(
            200,
            json={
                "id": "chatcmpl-json",
                "object": "chat.completion",
                "created": 1,
                "model": "gemma",
                "choices": [],
                "usage": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0},
            },
        )

    client = AntflyClient("http://test", token="secret")
    install_transport(client, httpx.MockTransport(handle))
    assert client.generate(generation_request()).id == "chatcmpl-json"
    with client.generate_stream(generation_request()) as chunks:
        chunk = next(chunks)
        assert chunk.choices[0].delta.content == "hé🐜"
    assert stream.closed
    assert requests == [
        {**generation_request().to_dict(), "stream": False},
        {**generation_request().to_dict(), "stream": True},
    ]


def test_generate_stream_returns_typed_507_and_requires_done() -> None:
    responses = iter(
        [
            httpx.Response(
                507,
                json={
                    "error": "MEMORY_BUDGET_EXCEEDED",
                    "message": "model needs 8 GiB",
                    "retryable": True,
                },
            ),
            httpx.Response(
                200,
                headers={"Content-Type": "text/event-stream"},
                content=b'data: {"id":"x","object":"chat.completion.chunk","created":1,"model":"gemma","choices":[]}\n\n',
            ),
        ]
    )
    client = AntflyClient("http://test")
    install_transport(client, httpx.MockTransport(lambda _: next(responses)))

    with pytest.raises(InferenceAPIError) as exc_info:
        with client.generate_stream(generation_request()):
            pass
    assert exc_info.value.status_code == 507
    assert exc_info.value.code == "MEMORY_BUDGET_EXCEEDED"
    assert exc_info.value.retryable is True
    assert exc_info.value.detail == "model needs 8 GiB (MEMORY_BUDGET_EXCEEDED)"

    with pytest.raises(AntflyException, match=r"ended before \[DONE\]"):
        with client.generate_stream(generation_request()) as chunks:
            list(chunks)


@pytest.mark.parametrize("reason", ["inference_capacity", "inference_admission", "request_queue"])
def test_generate_returns_typed_capacity_error(reason: str) -> None:
    client = AntflyClient("http://test")
    install_transport(
        client,
        httpx.MockTransport(
            lambda _: httpx.Response(
                503,
                json={
                    "error": "MODEL_RESOURCE_BUSY",
                    "message": "model resources are temporarily busy",
                    "reason": reason,
                    "retryable": True,
                    "retry_after_ms": 1000,
                },
            )
        ),
    )

    with pytest.raises(InferenceCapacityError) as exc_info:
        client.generate(generation_request())
    assert exc_info.value.status_code == 503
    assert exc_info.value.code == "MODEL_RESOURCE_BUSY"
    assert exc_info.value.reason == reason
    assert exc_info.value.retry_after_ms == 1000
    assert exc_info.value.retryable is True


def test_generate_stream_bounds_sse_lines(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(client_module, "MAX_GENERATION_SSE_LINE_BYTES", 32)
    stream = ChunkStream([b":" + (b"x" * 20), b"x" * 13, b"\n\n"])
    client = AntflyClient("http://test")
    install_transport(
        client,
        httpx.MockTransport(
            lambda _: httpx.Response(
                200,
                headers={"Content-Type": "text/event-stream"},
                stream=stream,
            )
        ),
    )

    with pytest.raises(AntflyException, match="generation SSE line exceeded 32 bytes"):
        with client.generate_stream(generation_request()) as chunks:
            list(chunks)
    assert stream.closed


def test_generate_bounds_success_response(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(client_module, "MAX_GENERATION_RESPONSE_BYTES", 32)
    stream = ChunkStream([b"{" + (b"x" * 20), b"x" * 20 + b"}"])
    client = AntflyClient("http://test")
    install_transport(
        client,
        httpx.MockTransport(
            lambda _: httpx.Response(
                200,
                headers={"Content-Type": "application/json"},
                stream=stream,
            )
        ),
    )

    with pytest.raises(AntflyException, match="generation response exceeded 32 bytes"):
        client.generate(generation_request())
    assert stream.closed
