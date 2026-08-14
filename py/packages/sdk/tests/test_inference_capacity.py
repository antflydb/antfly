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
