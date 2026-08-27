"""Regression tests for deployment-specific index capability responses."""

import httpx

from antfly.client_generated import Client
from antfly.client_generated.api.data_operations import restore_table
from antfly.client_generated.api.index_management import create_index
from antfly.client_generated.api.table_management import create_table
from antfly.client_generated.models.error import Error
from antfly.client_generated.models.index_mutation_service_unavailable_error import (
    IndexMutationServiceUnavailableError,
)
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


def test_index_mutation_503_preserves_typed_retry_contract() -> None:
    parsed = create_index._parse_response(
        client=Client(base_url="http://localhost:8080"),
        response=httpx.Response(
            status_code=503,
            json={
                "error": "index_probe_unavailable",
                "message": "model probe is temporarily unavailable",
                "retryable": True,
            },
        ),
    )

    assert isinstance(parsed, IndexMutationServiceUnavailableError)
    assert parsed.retryable is True


def test_shared_index_mutation_503_falls_back_to_public_error_envelope() -> None:
    cases = (
        (
            create_index._parse_response,
            {
                "code": "metadata_leader_unavailable",
                "error": "metadata leader unavailable",
                "message": "metadata leader unavailable",
                "retryable": True,
                "retry_after_ms": 1000,
            },
        ),
        (
            create_table._parse_response,
            {
                "code": "ha_write_admission_rejected",
                "error": "high availability write admission rejected",
                "surface": "create_table",
            },
        ),
        (
            restore_table._parse_response,
            {"error": "asynchronous restore worker unavailable"},
        ),
    )

    client = Client(base_url="http://localhost:8080", raise_on_unexpected_status=True)
    for parse_response, body in cases:
        parsed = parse_response(client=client, response=httpx.Response(status_code=503, json=body))
        assert isinstance(parsed, Error)
        assert parsed.error == body["error"]
