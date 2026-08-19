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
from antfly.client_generated.models.backup_outcome_ambiguous_conflict import BackupOutcomeAmbiguousConflict
from antfly.client_generated.models.backup_outcome_ambiguous_conflict_code import BackupOutcomeAmbiguousConflictCode
from antfly.client_generated.models.cluster_backup_response import ClusterBackupResponse
from antfly.client_generated.models.cluster_backup_response_status import ClusterBackupResponseStatus
from antfly.client_generated.models.table_backup_status import TableBackupStatus
from antfly.client_generated.models.table_backup_status_code import TableBackupStatusCode
from antfly.client_generated.models.table_backup_status_status import TableBackupStatusStatus


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

    assert isinstance(parsed, BackupOutcomeAmbiguousConflict)
    assert parsed.code is BackupOutcomeAmbiguousConflictCode.BACKUP_OUTCOME_AMBIGUOUS
    assert parsed.retryable is False
    assert parsed.backup_id == "snap"
    assert parsed.artifact_backup_id == "generation-7"


def test_generated_cluster_backup_200_exposes_ambiguous_table_identity() -> None:
    client = Client(base_url="http://antfly.invalid", raise_on_unexpected_status=True)
    parsed = parse_cluster_backup_response(
        client=client,
        response=httpx.Response(
            200,
            json={
                "backup_id": "nightly",
                "status": "ambiguous",
                "tables": [
                    {
                        "name": "docs",
                        "status": "ambiguous",
                        "error": "backup outcome is ambiguous; inspect the backup id before retrying",
                        "code": "backup_outcome_ambiguous",
                        "retryable": False,
                        "backup_id": "attempt-t-0",
                        "artifact_backup_id": "attempt-a-0",
                    }
                ],
            },
        ),
    )

    assert isinstance(parsed, ClusterBackupResponse)
    assert parsed.status is ClusterBackupResponseStatus.AMBIGUOUS
    assert isinstance(parsed.tables[0], TableBackupStatus)
    assert parsed.tables[0].status is TableBackupStatusStatus.AMBIGUOUS
    assert parsed.tables[0].code is TableBackupStatusCode.BACKUP_OUTCOME_AMBIGUOUS
    assert parsed.tables[0].retryable is False
    assert parsed.tables[0].backup_id == "attempt-t-0"
    assert parsed.tables[0].artifact_backup_id == "attempt-a-0"
