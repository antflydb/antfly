"""Public recovery types preserve uncertain admission and exact identities."""

from unittest.mock import MagicMock, patch

import httpx
import pytest

from antfly import (
    AntflyClient,
    AntflyException,
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


@patch("antfly.client.Client")
def test_recovery_read_paths_header_and_no_replay(mock_client_class: MagicMock) -> None:
    token = "A" * 171
    calls: list[httpx.Request] = []
    mode = "valid"

    def handle(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        assert request.method == "GET"
        assert request.headers["X-Antfly-Index-Publication-Token"] == token
        if mode == "oversize":
            return httpx.Response(200, content=b"x" * 16385)
        return httpx.Response(
            200,
            json={
                "index_type": "relational",
                "index_name": "by id/#",
                "publication_token": "B" * 171 if mode == "foreign" else token,
                "table_id": "18446744073709551615",
                "publication_id": "2" * 32,
                "plan_digest": "3" * 64,
                "metadata_incarnation": "1" * 32,
                "state": "published" if mode == "unconfirmed" else "admission_unknown",
                "replication_pending": True,
                "phase": "published",
            },
        )

    with httpx.Client(base_url="http://antfly.test", transport=httpx.MockTransport(handle)) as transport:
        mock_client_class.return_value.get_httpx_client.return_value = transport
        client = AntflyClient(base_url="http://antfly.test")
        status = client.indexes.publication("items/a", "by id/#", token)
        assert status.state is IndexPublicationState.ADMISSION_UNKNOWN
        client.indexes.publication("items/a", "by id/#", token, database="warehouse", namespace="sales")
        assert [r.url.raw_path for r in calls] == [
            b"/db/v1/tables/items%2Fa/indexes/by%20id%2F%23/publication",
            b"/db/v1/databases/warehouse/namespaces/sales/tables/items%2Fa/indexes/by%20id%2F%23/publication",
        ]
        with pytest.raises(ValueError):
            client.indexes.publication("items/a", "by id/#", "bad")
        with pytest.raises(ValueError):
            client.indexes.publication("items/a", "by id/#", token, namespace="sales")
        assert len(calls) == 2
        for mode in ("foreign", "unconfirmed", "oversize"):
            before = len(calls)
            with pytest.raises(AntflyException):
                client.indexes.publication("items/a", "by id/#", token)
            assert len(calls) == before + 1
