"""Public JSON inference calls preserve the inference error contract."""

import httpx
import pytest

from antfly import AntflyClient, InferenceAPIError, InferenceCapacityError


@pytest.fixture(params=["embed", "rerank", "models", "request"])
def inference_call(request):
    with AntflyClient("http://test") as client:

        def call(payload, status=503):
            calls = []

            def handler(http_request):
                calls.append(http_request)
                return httpx.Response(status, json=payload)

            client._client.set_httpx_client(
                httpx.Client(base_url="http://test", transport=httpx.MockTransport(handler))
            )
            try:
                if request.param == "request":
                    return client.request("POST", "/ai/v1/embeddings", json={"model": "m", "input": ["x"]})
                if request.param == "models":
                    return client.models()
                if request.param == "embed":
                    return client.embed({"model": "m", "input": ["x"]})
                return client.rerank({"model": "m", "query": "q", "documents": ["x"]})
            finally:
                assert len(calls) == 1

        yield call


@pytest.mark.parametrize("reason", ["inference_capacity", "inference_admission", "request_queue"])
def test_public_inference_preserves_capacity_retry_metadata(inference_call, reason):
    with pytest.raises(InferenceCapacityError) as caught:
        inference_call(
            {
                "error": "MODEL_RESOURCE_BUSY",
                "message": "model resources are temporarily busy",
                "reason": reason,
                "retryable": True,
                "retry_after_ms": 1000,
            }
        )
    assert caught.value.status_code == 503
    assert caught.value.code == "MODEL_RESOURCE_BUSY"
    assert caught.value.reason == reason
    assert caught.value.retryable is True
    assert caught.value.retry_after_ms == 1000
    assert "model resources are temporarily busy" in caught.value.detail


def test_public_inference_preserves_structured_failure(inference_call):
    with pytest.raises(InferenceAPIError) as caught:
        inference_call(
            {
                "error": "INVALID_INPUT",
                "message": "input could not be decoded",
                "retryable": False,
                "input_index": 2,
                "stage": "decode",
            },
            status=400,
        )
    assert caught.value.status_code == 400
    assert caught.value.code == "INVALID_INPUT"
    assert caught.value.retryable is False
    assert caught.value.input_index == 2
    assert caught.value.stage == "decode"
    assert "input could not be decoded" in caught.value.detail
