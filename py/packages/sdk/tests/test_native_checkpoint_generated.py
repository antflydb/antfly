"""Check the generated binary transport contract; model qualification is separate."""

from io import BytesIO

import httpx

from antfly.client_generated.api.default import prepare_native_kv_checkpoint, resume_native_kv_checkpoint
from antfly.client_generated.client import Client
from antfly.client_generated.models.inference_generate_request import InferenceGenerateRequest
from antfly.client_generated.models.inference_generate_response import InferenceGenerateResponse
from antfly.client_generated.types import File

CONTENT_TYPE = "application/vnd.antfly.attachments.v1"


def test_generated_prepare_preserves_binary_response_bytes() -> None:
    payload = b"AFATT001\x00\xff\x80\x00\n"

    def handle(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/ai/v1/chat/context/checkpoint"
        assert request.headers["Content-Type"] == "application/json"
        return httpx.Response(200, content=payload, headers={"Content-Type": CONTENT_TYPE})

    with Client(base_url="http://antfly.invalid", httpx_args={"transport": httpx.MockTransport(handle)}) as client:
        result = prepare_native_kv_checkpoint.sync_detailed(
            client=client, body=InferenceGenerateRequest(model="gemma", messages=[])
        )
    assert isinstance(result.parsed, File)
    assert result.parsed.payload.read() == payload
    assert result.headers["Content-Type"] == CONTENT_TYPE


def test_generated_resume_sends_original_mime_and_unmodified_binary_body() -> None:
    payload = b"AFATT001\x00\xff\x80\x00\n"

    def handle(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/ai/v1/chat/context/resume"
        assert request.headers["Content-Type"] == CONTENT_TYPE
        assert request.read() == payload
        return httpx.Response(
            200,
            json={
                "id": "checkpoint-test",
                "object": "chat.completion",
                "created": 1,
                "model": "gemma",
                "choices": [],
                "usage": {"prompt_tokens": 12, "completion_tokens": 1, "total_tokens": 13, "cached_prompt_tokens": 12},
            },
        )

    with Client(base_url="http://antfly.invalid", httpx_args={"transport": httpx.MockTransport(handle)}) as client:
        result = resume_native_kv_checkpoint.sync_detailed(client=client, body=File(payload=BytesIO(payload)))
    assert isinstance(result.parsed, InferenceGenerateResponse)
    assert result.parsed.usage.cached_prompt_tokens == 12
