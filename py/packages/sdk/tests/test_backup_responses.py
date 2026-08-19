import httpx
import pytest

from antfly.client_generated.api.cluster_management.backup import _parse_response as parse_cluster_backup_response
from antfly.client_generated.api.data_operations.backup_table import _parse_response as parse_table_backup_response
from antfly.client_generated.client import Client
from antfly.client_generated.models.metadata_capability_unavailable_error import (
    MetadataCapabilityUnavailableError,
)
from antfly.client_generated.models.metadata_capability_unavailable_error_required_capability import (
    MetadataCapabilityUnavailableErrorRequiredCapability,
)
from antfly.client_generated.models.metadata_leader_unavailable_error import MetadataLeaderUnavailableError


@pytest.mark.parametrize(
    ("body", "expected_type"),
    [
        (
            {
                "code": "metadata_capability_unavailable",
                "error": "metadata_capability_unavailable",
                "message": "upgrade metadata nodes",
                "required_capability": "linearizable_snapshot",
                "retryable": True,
                "retry_after_ms": 5000,
            },
            MetadataCapabilityUnavailableError,
        ),
        (
            {
                "code": "metadata_leader_unavailable",
                "error": "metadata_leader_unavailable",
                "message": "metadata leader unavailable",
                "retryable": True,
                "retry_after_ms": 1000,
            },
            MetadataLeaderUnavailableError,
        ),
    ],
)
def test_generated_backup_503_is_typed(body: dict[str, object], expected_type: type[object]) -> None:
    client = Client(base_url="http://antfly.invalid", raise_on_unexpected_status=True)
    for parse_response in (parse_cluster_backup_response, parse_table_backup_response):
        parsed = parse_response(client=client, response=httpx.Response(503, json=body))

        assert isinstance(parsed, expected_type)
        if isinstance(parsed, MetadataCapabilityUnavailableError):
            assert (
                parsed.required_capability is MetadataCapabilityUnavailableErrorRequiredCapability.LINEARIZABLE_SNAPSHOT
            )
