"""Exact, bounded, read-only schema recovery through the public SDK."""

from unittest.mock import MagicMock, patch

import httpx
import pytest

from antfly import AntflyClient, AntflyException, SchemaValidationState


@patch("antfly.client.Client")
def test_schema_publication_identity_scope_bounds_and_no_replay(mock_client_class: MagicMock) -> None:
    token = "A" * 182
    calls: list[httpx.Request] = []
    mode = "valid"

    def handle(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        assert request.method == "GET"
        assert request.headers["X-Antfly-Schema-Validation-Token"] == token
        if mode == "oversize":
            return httpx.Response(200, content=b"x" * 16385)
        body: dict = {
            "schema_validation_token": "B" * 182 if mode == "foreign" else token,
            "table_id": "18446744073709551615",
            "active_schema_version": 2,
            "state": "admission_unknown",
        }
        if mode in ("incomplete", "negative"):
            body.update(
                state="succeeded",
                phase="succeeded",
                owner_count=-1 if mode == "negative" else 1,
                ready=-1 if mode == "negative" else 1,
                installed=-1 if mode == "negative" else 1,
                released=-1 if mode == "negative" else 0,
            )
        return httpx.Response(200, json=body)

    with httpx.Client(base_url="http://antfly.test", transport=httpx.MockTransport(handle)) as transport:
        mock_client_class.return_value.get_httpx_client.return_value = transport
        client = AntflyClient(base_url="http://antfly.test")
        status = client.get_schema_validation_publication("items/a", token)
        assert status.state is SchemaValidationState.ADMISSION_UNKNOWN
        assert status.table_id == "18446744073709551615"
        client.get_schema_validation_publication("items/a", token, database="warehouse", namespace="sales")
        assert [r.url.raw_path for r in calls] == [
            b"/db/v1/tables/items%2Fa/schema/publication",
            b"/db/v1/databases/warehouse/namespaces/sales/tables/items%2Fa/schema/publication",
        ]
        for mode in ("foreign", "oversize", "incomplete", "negative"):
            before = len(calls)
            with pytest.raises(AntflyException):
                client.get_schema_validation_publication("items/a", token)
            assert len(calls) == before + 1
        before = len(calls)
        with pytest.raises(ValueError):
            client.get_schema_validation_publication("items/a", "bad")
        with pytest.raises(ValueError):
            client.get_schema_validation_publication("items/a", token, database="warehouse")
        assert len(calls) == before
