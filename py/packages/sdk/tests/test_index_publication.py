"""Public recovery types preserve uncertain admission and exact identities."""

from antfly import (
    IndexPublicationPhase,
    IndexPublicationState,
    IndexPublicationStatus,
    IndexPublicationStatusIndexType,
)
from antfly.client_generated.models import SQLDDLReceipt


def test_publication_recovery_round_trip() -> None:
    body = {
        "index_type": "relational",
        "index_name": "by_email",
        "publication_token": "recovery-handle",
        "table_id": "18446744073709551615",
        "publication_id": "2" * 32,
        "plan_digest": "3" * 64,
        "metadata_incarnation": "1" * 32,
        "state": "admission_unknown",
        "replication_pending": True,
        "phase": "published",
        "owner_count": 2,
    }
    status = IndexPublicationStatus.from_dict(body)
    assert status.index_type is IndexPublicationStatusIndexType.RELATIONAL
    assert status.state is IndexPublicationState.ADMISSION_UNKNOWN
    assert status.phase is IndexPublicationPhase.PUBLISHED
    assert status.to_dict() == body
    receipt = SQLDDLReceipt.from_dict(
        {
            "database": "default",
            "namespace": "public",
            "table": "items",
            "table_id": body["table_id"],
            "schema_version": 2,
            "state": "admission_unknown",
            "index_publication_token": body["publication_token"],
            "fk_generation_publication_id": body["publication_id"],
        }
    )
    assert receipt.to_dict()["index_publication_token"] == body["publication_token"]
    assert receipt.to_dict()["fk_generation_publication_id"] == body["publication_id"]
