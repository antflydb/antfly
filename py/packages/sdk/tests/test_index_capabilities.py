"""Regression tests for deployment-specific index capability responses."""

import httpx

from antfly.client_generated import Client
from antfly.client_generated.api.index_management import create_index
from antfly.client_generated.models.unsupported_index_capability_error import (
    UnsupportedIndexCapabilityError,
)
from antfly.client_generated.models.unsupported_index_capability_error_error import (
    UnsupportedIndexCapabilityErrorError,
)


def test_create_index_preserves_typed_capability_error() -> None:
    response = httpx.Response(
        status_code=400,
        json={
            "error": "unsupported_index_capability",
            "message": "artifact-backed index sources are not supported by this deployment",
            "retryable": False,
        },
    )

    parsed = create_index._parse_response(
        client=Client(base_url="http://localhost:8080"),
        response=response,
    )

    assert isinstance(parsed, UnsupportedIndexCapabilityError)
    assert parsed.error is UnsupportedIndexCapabilityErrorError.UNSUPPORTED_INDEX_CAPABILITY
    assert parsed.retryable is False
