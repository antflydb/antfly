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
from antfly.client_generated.models.table_backup_conflict_error import TableBackupConflictError
from antfly.client_generated.models.table_backup_conflict_error_code import TableBackupConflictErrorCode


@pytest.mark.parametrize(
    ("body", "expected_type"),
    [
        (
            {
                "code": "metadata_capability_unavailable",
                "error": "metadata capability unavailable",
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
                "error": "metadata leader unavailable",
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


def test_generated_table_backup_409_exposes_ambiguous_outcome() -> None:
    client = Client(base_url="http://antfly.invalid", raise_on_unexpected_status=True)
    parsed = parse_table_backup_response(
        client=client,
        response=httpx.Response(
            409,
            json={
                "code": "backup_outcome_ambiguous",
                "error": "backup outcome is ambiguous; inspect the backup id before retrying",
                "message": "backup outcome is ambiguous; inspect the backup id and artifact id before retrying",
                "retryable": False,
                "backup_id": "snap",
                "artifact_backup_id": "generation-7",
            },
        ),
    )

    assert isinstance(parsed, TableBackupConflictError)
    assert parsed.code is TableBackupConflictErrorCode.BACKUP_OUTCOME_AMBIGUOUS
    assert parsed.retryable is False
    assert parsed.backup_id == "snap"
    assert parsed.artifact_backup_id == "generation-7"
