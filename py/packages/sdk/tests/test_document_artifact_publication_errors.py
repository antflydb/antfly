"""Failed publication remains a typed, terminal error on every query route."""

import httpx
import pytest

from antfly.client_generated.api.query_operations import global_query, query_namespace_table, query_table
from antfly.client_generated.client import Client
from antfly.client_generated.models.document_artifact_publication_failed_error import (
    DocumentArtifactPublicationFailedError,
)


@pytest.mark.parametrize("operation", [global_query, query_table, query_namespace_table])
def test_failed_publication_response_is_typed_and_not_retryable(operation):
    code = "document_artifact_publication_failed"
    result = operation._parse_response(
        client=Client(base_url="http://unused", raise_on_unexpected_status=True),
        response=httpx.Response(
            409,
            json={"code": code, "error": code, "message": "Inspect the artifact manifest", "retryable": False},
        ),
    )
    assert isinstance(result, DocumentArtifactPublicationFailedError)
    assert result.code == code
    assert result.error == code
    assert result.retryable is False
