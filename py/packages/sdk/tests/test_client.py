"""Tests for Antfly client."""

import json
from unittest.mock import MagicMock, Mock, patch

import pytest
from httpx import Timeout

from antfly import AntflyClient, AntflyException  # noqa: E402
from antfly.client import normalize_base_url  # noqa: E402
from antfly.client_generated.models.sort_profile import SortProfile  # noqa: E402
from antfly.client_generated.models.transform_op_type import TransformOpType  # noqa: E402
from antfly.client_generated.types import Unset  # noqa: E402


class TestAntflyClient:
    """Test cases for AntflyClient."""

    def test_transform_operator_names_are_stable(self) -> None:
        assert {member.name: member.value for member in TransformOpType} == {
            "SET": "$set",
            "SET_ON_INSERT": "$setOnInsert",
            "UNSET": "$unset",
            "INC": "$inc",
            "PUSH": "$push",
            "ADD_TO_SET": "$addToSet",
            "MAX": "$max",
        }

    def test_sort_profile_uses_closed_public_diagnostic_shape(self) -> None:
        """SortProfile keeps stable fields typed and drops internal counters."""
        profile = SortProfile.from_dict(
            {
                "plan": "native_doc_values_top_n",
                "candidate_count": 7,
                "native_doc_value_load_us": 13,
                "collector_heap_peak": 5,
            }
        )

        assert profile.plan == "native_doc_values_top_n"
        assert profile.candidate_count == 7
        encoded = profile.to_dict()

        assert "native_doc_value_load_us" not in encoded
        assert "collector_heap_peak" not in encoded

    @patch("antfly.client.Client")
    def test_client_initialization(self, mock_client: MagicMock) -> None:
        """Test client initialization with and without auth."""
        # Without auth
        client = AntflyClient(base_url="http://localhost:8080")
        assert client.base_url == "http://localhost:8080"
        mock_client.assert_called_once_with(
            base_url="http://localhost:8080",
            timeout=Timeout(30.0),
            httpx_args={},
        )

        # With auth
        mock_client.reset_mock()
        client = AntflyClient(base_url="http://localhost:8080/", username="admin", password="password")
        assert client.base_url == "http://localhost:8080"
        mock_client.assert_called_once_with(
            base_url="http://localhost:8080",
            timeout=Timeout(30.0),
            httpx_args={"auth": ("admin", "password")},
        )

    def test_normalize_base_url(self) -> None:
        assert normalize_base_url("http://localhost:8080") == "http://localhost:8080"
        assert normalize_base_url("http://localhost:8080/") == "http://localhost:8080"
        assert normalize_base_url("http://localhost:8080/db/v1") == "http://localhost:8080"
        assert normalize_base_url("http://localhost:8080/auth/v1") == "http://localhost:8080"
        assert normalize_base_url("http://localhost:8080/ai/v1") == "http://localhost:8080"
        assert (
            normalize_base_url("https://platform.antfly.io/cloud/v1/instance")
            == "https://platform.antfly.io/cloud/v1/instance"
        )
        assert (
            normalize_base_url("https://platform.antfly.io/cloud/v1/instance/db/v1")
            == "https://platform.antfly.io/cloud/v1/instance"
        )

    @patch("antfly.client.AuthenticatedClient")
    def test_token_auth(self, mock_client: MagicMock) -> None:
        AntflyClient(base_url="https://platform.antfly.io/cloud/v1/instance", token="antflydb_test")
        mock_client.assert_called_once_with(
            base_url="https://platform.antfly.io/cloud/v1/instance",
            token="antflydb_test",
            prefix="Bearer",
            timeout=Timeout(30.0),
            httpx_args={},
        )

    @patch("antfly.client.Client")
    def test_list_tables(self, mock_client_class: MagicMock) -> None:
        """Test listing tables."""
        client = AntflyClient(base_url="http://localhost:8080")

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = []

        mock_httpx = MagicMock()
        mock_httpx.request.return_value = mock_response
        mock_client_class.return_value.get_httpx_client.return_value = mock_httpx

        # Re-create client so it picks up the mock
        client = AntflyClient(base_url="http://localhost:8080")
        tables = client.list_tables()

        assert tables == []
        mock_httpx.request.assert_called_once_with("GET", "/db/v1/tables")

    @patch("antfly.client.Client")
    def test_create_table(self, mock_client_class: MagicMock) -> None:
        """Test creating a table."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"name": "test_table", "shards": {}, "indexes": {}}

        mock_httpx = MagicMock()
        mock_httpx.request.return_value = mock_response
        mock_client_class.return_value.get_httpx_client.return_value = mock_httpx

        client = AntflyClient(base_url="http://localhost:8080")
        result = client.create_table(name="test_table", num_shards=2)

        assert result["name"] == "test_table"
        mock_httpx.request.assert_called_once_with("POST", "/db/v1/tables/test_table", json={"num_shards": 2})

    @patch("antfly.client.Client")
    def test_create_table_failure(self, mock_client_class: MagicMock) -> None:
        """Test handling of create table failure."""
        mock_response = Mock()
        mock_response.status_code = 400
        mock_response.text = "bad request"
        mock_response.json.return_value = {"error": "table already exists"}

        mock_httpx = MagicMock()
        mock_httpx.request.return_value = mock_response
        mock_client_class.return_value.get_httpx_client.return_value = mock_httpx

        client = AntflyClient(base_url="http://localhost:8080")

        with pytest.raises(AntflyException) as exc_info:
            client.create_table(name="test_table")

        assert "table already exists" in str(exc_info.value)

    @patch("antfly.client.Client")
    def test_query_preserves_sorted_cursor_contract(self, mock_client_class: MagicMock) -> None:
        """High-level query forwards order_by/search_after/profile and returns generated response model."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "responses": [
                {
                    "took": 3,
                    "status": 200,
                    "hits": {
                        "hits": [
                            {
                                "_id": "doc:2",
                                "_score": 1.0,
                                "_sort": ["2026-01-01T00:00:00Z", "doc:2"],
                                "_source": {"created_at": "2026-01-01T00:00:00Z"},
                            }
                        ]
                    },
                }
            ]
        }

        mock_httpx = MagicMock()
        mock_httpx.request.return_value = mock_response
        mock_client_class.return_value.get_httpx_client.return_value = mock_httpx

        client = AntflyClient(base_url="http://localhost:8080")
        result = client.query(
            table="docs",
            query={"match_all": {}},
            order_by=[{"field": "created_at", "desc": True}],
            search_after=["2025-12-31T00:00:00Z", "doc:1"],
            limit=10,
            profile=False,
        )

        mock_httpx.request.assert_called_once_with(
            "POST",
            "/db/v1/tables/docs/query",
            json={
                "query": {"match_all": {}},
                "limit": 10,
                "order_by": [{"field": "created_at", "desc": True}],
                "search_after": ["2025-12-31T00:00:00Z", "doc:1"],
                "profile": False,
            },
        )
        assert not isinstance(result.responses, Unset)
        query_result = result.responses[0]
        assert not isinstance(query_result.hits, Unset)
        assert not isinstance(query_result.hits.hits, Unset)
        hit = query_result.hits.hits[0]
        assert hit.field_id == "doc:2"
        assert hit.field_sort == ["2026-01-01T00:00:00Z", "doc:2"]

    def test_query_rejects_ambiguous_aggregation_aliases(self) -> None:
        client = AntflyClient(base_url="http://localhost:8080")

        with pytest.raises(AntflyException, match="either aggregations or facets"):
            client.query(table="docs", aggregations={"a": {}}, facets={"b": {}})

    @patch("antfly.client.Client")
    @patch("antfly.client.lookup_key")
    def test_get_record(self, mock_lookup_key: MagicMock, mock_client_class: MagicMock) -> None:
        """Test getting a record by key."""
        mock_response = Mock()
        mock_response.to_dict.return_value = {"name": "John Doe"}
        mock_lookup_key.sync.return_value = mock_response

        client = AntflyClient(base_url="http://localhost:8080")
        record = client.get(table="users", key="user:1")

        assert record == {"name": "John Doe"}
        mock_lookup_key.sync.assert_called_once_with(table_name="users", key="user:1", client=client._client)

    @patch("antfly.client.Client")
    @patch("antfly.client.lookup_key")
    def test_get_record_failure(self, mock_lookup_key: MagicMock, mock_client_class: MagicMock) -> None:
        """Test handling of get record failure."""
        mock_lookup_key.sync.return_value = None

        client = AntflyClient(base_url="http://localhost:8080")

        with pytest.raises(AntflyException) as exc_info:
            client.get(table="users", key="user:1")

        assert "Failed to get key 'user:1' from table 'users'" in str(exc_info.value)

    @patch("antfly.client.Client")
    def test_batch_rejects_oversized_request(self, mock_client_class: MagicMock) -> None:
        """Test client-side write request size enforcement."""
        mock_httpx = MagicMock()
        mock_client_class.return_value.get_httpx_client.return_value = mock_httpx
        client = AntflyClient(base_url="http://localhost:8080", max_write_request_bytes=32)

        with pytest.raises(AntflyException) as exc_info:
            client.batch(table="users", inserts={"user:1": {"bio": "x" * 128}})

        assert "exceeding max write request size 32" in str(exc_info.value)
        mock_httpx.request.assert_not_called()

    @patch("antfly.client.Client")
    def test_batch_sends_exact_checked_bytes(self, mock_client_class: MagicMock) -> None:
        """Test batch sends the same bytes used for request-size enforcement."""
        expected_body = {"inserts": {"user:1": {"name": "Zoë"}}, "deletes": []}
        expected_content = json.dumps(expected_body, separators=(",", ":"), ensure_ascii=False).encode("utf-8")

        mock_response = Mock()
        mock_response.status_code = 201
        mock_response.json.return_value = {"inserted": 1}

        mock_httpx = MagicMock()
        mock_httpx.request.return_value = mock_response
        mock_client_class.return_value.get_httpx_client.return_value = mock_httpx

        client = AntflyClient(base_url="http://localhost:8080", max_write_request_bytes=len(expected_content))
        client.batch(table="users", inserts={"user:1": {"name": "Zoë"}})

        mock_httpx.request.assert_called_once_with(
            "POST",
            "/db/v1/tables/users/batch",
            content=expected_content,
            headers={"Content-Type": "application/json"},
        )
