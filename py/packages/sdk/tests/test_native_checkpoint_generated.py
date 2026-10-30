"""Check the generated binary transport contract; model qualification is separate."""

from io import BytesIO
from threading import get_ident

import httpx
import pytest

from antfly.client_generated.api.default import (
    prepare_native_kv_checkpoint,
    resume_native_kv_checkpoint,
    resume_native_kv_checkpoint_batch,
)
from antfly.client_generated.client import Client
from antfly.client_generated.models.inference_generate_batch_error import InferenceGenerateBatchError
from antfly.client_generated.models.inference_generate_batch_response import InferenceGenerateBatchResponse
from antfly.client_generated.models.inference_generate_request import InferenceGenerateRequest
from antfly.client_generated.models.inference_generate_response import InferenceGenerateResponse
from antfly.client_generated.models.prepare_native_kv_checkpoint_x_antfly_kv_encoding import (
    PrepareNativeKvCheckpointXAntflyKVEncoding,
)
from antfly.client_generated.types import File

CONTENT_TYPE = "application/vnd.antfly.attachments.v1"


@pytest.mark.parametrize("encoding", ["f32", "f16", "fp8", "rotated4"])
def test_generated_prepare_preserves_binary_response_bytes(encoding) -> None:
    payload = b"AFATT001\x00\xff\x80\x00\n"

    def handle(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/ai/v1/generate/context/checkpoint"
        assert request.headers["X-Antfly-KV-Encoding"] == encoding
        assert request.headers["Content-Type"] == "application/json"
        return httpx.Response(200, content=payload, headers={"Content-Type": CONTENT_TYPE})

    with Client(base_url="http://antfly.invalid", httpx_args={"transport": httpx.MockTransport(handle)}) as client:
        result = prepare_native_kv_checkpoint.sync_detailed(
            client=client,
            body=InferenceGenerateRequest(model="gemma", messages=[]),
            x_antfly_kv_encoding=PrepareNativeKvCheckpointXAntflyKVEncoding(encoding),
        )
    assert isinstance(result.parsed, File)
    assert result.parsed.payload.read() == payload
    assert result.headers["Content-Type"] == CONTENT_TYPE


def test_generated_resume_sends_original_mime_and_unmodified_binary_body() -> None:
    payload = b"AFATT001\x00\xff\x80\x00\n"

    def handle(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/ai/v1/generate/context/resume"
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


def _completion(cached: int) -> dict:
    return {
        "id": "checkpoint-test",
        "object": "chat.completion",
        "created": 1,
        "model": "gemma",
        "choices": [],
        "usage": {"prompt_tokens": 12, "completion_tokens": 1, "total_tokens": 13, "cached_prompt_tokens": cached},
    }


def test_generated_batch_resume_keeps_binary_body_and_per_item_results() -> None:
    payload = b"AFATT001\x00\xff\x80\x00\n"

    def handle(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/ai/v1/generate/context/resume/batch"
        assert request.headers["Content-Type"] == CONTENT_TYPE
        assert request.read() == payload
        return httpx.Response(
            200,
            json={
                "object": "generate.batch",
                "data": [
                    {"custom_id": "native", "index": 0, "response": _completion(12)},
                    {"custom_id": "ordinary", "index": 1, "response": _completion(0)},
                    {
                        "custom_id": "busy",
                        "index": 2,
                        "error": {
                            "code": "MODEL_RESOURCE_BUSY",
                            "message": "busy",
                            "retryable": True,
                            "retry_after_ms": 1000,
                        },
                    },
                ],
                "summary": {"total": 3, "succeeded": 2, "failed": 1},
            },
        )

    with Client(base_url="http://antfly.invalid", httpx_args={"transport": httpx.MockTransport(handle)}) as client:
        result = resume_native_kv_checkpoint_batch.sync_detailed(client=client, body=File(payload=BytesIO(payload)))
    assert isinstance(result.parsed, InferenceGenerateBatchResponse)
    native, ordinary, failed = result.parsed.data
    assert (native.custom_id, ordinary.custom_id, failed.custom_id) == ("native", "ordinary", "busy")
    assert isinstance(native.response, InferenceGenerateResponse)
    assert isinstance(ordinary.response, InferenceGenerateResponse)
    assert native.response.usage.cached_prompt_tokens == 12
    assert ordinary.response.usage.cached_prompt_tokens == 0
    assert isinstance(failed.error, InferenceGenerateBatchError)
    assert failed.error.retryable and failed.error.retry_after_ms == 1000


@pytest.mark.asyncio
@pytest.mark.parametrize("batch", [False, True])
async def test_generated_async_resume_streams_bounded_binary_chunks_without_blocking(batch: bool) -> None:
    payload = b"AFATT001\x00\xff\x80\n" * 17000
    event_loop_thread = get_ident()
    reads: list[tuple[int, int]] = []

    class BoundedPayload(BytesIO):
        def read(self, size: int = -1) -> bytes:
            assert 0 < size <= 65536
            reads.append((size, get_ident()))
            return super().read(size)

    async def handle(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/ai/v1/generate/context/resume" + ("/batch" if batch else "")
        assert request.headers["Content-Type"] == CONTENT_TYPE
        assert await request.aread() == payload
        response = _completion(12)
        if batch:
            response = {
                "object": "generate.batch",
                "data": [
                    {"custom_id": "native", "index": 0, "response": response},
                    {"custom_id": "ordinary", "index": 1, "response": _completion(0)},
                ],
                "summary": {"total": 2, "succeeded": 2, "failed": 0},
            }
        return httpx.Response(200, json=response)

    operation = resume_native_kv_checkpoint_batch if batch else resume_native_kv_checkpoint
    upload = BoundedPayload(payload)
    async with Client(
        base_url="http://antfly.invalid", httpx_args={"transport": httpx.MockTransport(handle)}
    ) as client:
        result = await operation.asyncio_detailed(client=client, body=File(payload=upload))
    assert isinstance(result.parsed, InferenceGenerateBatchResponse if batch else InferenceGenerateResponse)
    assert len(reads) >= 3 and all(thread != event_loop_thread for _, thread in reads)
    assert not upload.closed
