"""Regression tests for generated inference-capacity responses."""

import httpx
import pytest

from antfly.client_generated import Client
from antfly.client_generated.api.default import create_embedding
from antfly.client_generated.models.inference_transient_capacity_error import (
    InferenceTransientCapacityError,
)
from antfly.client_generated.models.inference_transient_capacity_error_reason import (
    InferenceTransientCapacityErrorReason,
)


@pytest.mark.parametrize(
    ("reason", "expected_reason"),
    [
        ("inference_capacity", InferenceTransientCapacityErrorReason.INFERENCE_CAPACITY),
        ("inference_admission", InferenceTransientCapacityErrorReason.INFERENCE_ADMISSION),
    ],
)
def test_transient_capacity_response_preserves_retry_metadata(
    reason: str,
    expected_reason: InferenceTransientCapacityErrorReason,
) -> None:
    response = httpx.Response(
        status_code=503,
        headers={"Retry-After": "1"},
        json={
            "error": "MODEL_RESOURCE_BUSY",
            "message": "model resources are temporarily busy",
            "reason": reason,
            "retryable": True,
            "retry_after_ms": 1000,
        },
    )

    parsed = create_embedding._parse_response(
        client=Client(base_url="http://localhost:8080"),
        response=response,
    )

    assert isinstance(parsed, InferenceTransientCapacityError)
    assert parsed.error == "MODEL_RESOURCE_BUSY"
    assert parsed.reason is expected_reason
    assert parsed.retryable is True
    assert parsed.retry_after_ms == 1000


@pytest.mark.parametrize("operation", ["query_builder_agent", "retrieval_agent"])
@pytest.mark.parametrize(
    "payload",
    [
        {"code": "doc_identity_unavailable", "message": "doc identity unavailable", "retryable": True},
        {
            "code": "query_embedding_temporarily_unavailable",
            "message": "query embedding temporarily unavailable",
            "retryable": True,
        },
        {
            "error": "GenerationCapacityUnavailable",
            "message": "inference capacity temporarily unavailable",
            "reason": "inference_capacity",
            "retryable": True,
            "retry_after_ms": 1000,
        },
    ],
)
def test_generated_agent_503_variants_preserve_retry_metadata(operation: str, payload: dict) -> None:
    from antfly.client_generated.api.query_operations import query_builder_agent, retrieval_agent
    from antfly.client_generated.models.inference_capacity_error import InferenceCapacityError
    from antfly.client_generated.models.query_temporarily_unavailable_error import QueryTemporarilyUnavailableError

    module = {"query_builder_agent": query_builder_agent, "retrieval_agent": retrieval_agent}[operation]
    response = httpx.Response(503, headers={"Retry-After": "1"}, json=payload)
    result = module._build_response(client=Client(base_url="http://antfly.invalid"), response=response)
    assert result.headers["Retry-After"] == "1"
    assert result.parsed.retryable is True
    assert result.parsed.message == payload["message"]
    if "code" in payload:
        assert isinstance(result.parsed, QueryTemporarilyUnavailableError)
        assert result.parsed.code == payload["code"]
    else:
        assert isinstance(result.parsed, InferenceCapacityError)
        assert result.parsed.error == payload["error"]
        assert result.parsed.retry_after_ms == 1000
