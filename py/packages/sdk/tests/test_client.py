"""Tests for Antfly client."""

import json
from collections.abc import Iterator
from unittest.mock import MagicMock, Mock, patch

import httpx
import pytest
from httpx import Timeout

from antfly import (  # noqa: E402
    AntflyClient,
    AntflyException,
    CreatedEmbeddingsIndex,
    CreateEmbeddingsIndexRequest,
    CreateEmbeddingsIndexRequestType,
    StorageResourceExhaustedError,
    antfly_embedder,
)
from antfly.client import normalize_base_url  # noqa: E402
from antfly.client_generated.models.batch_request import BatchRequest  # noqa: E402
from antfly.client_generated.models.graph_document_fuzzy_filter import (  # noqa: E402
    GraphDocumentFuzzyFilter,
)
from antfly.client_generated.models.graph_document_numeric_range_filter import (  # noqa: E402
    GraphDocumentNumericRangeFilter,
)
from antfly.client_generated.models.graph_document_term_filter import (  # noqa: E402
    GraphDocumentTermFilter,
)
from antfly.client_generated.models.graph_match_node import GraphMatchNode  # noqa: E402
from antfly.client_generated.models.graph_match_query import GraphMatchQuery  # noqa: E402
from antfly.client_generated.models.inference_chat_message import InferenceChatMessage  # noqa: E402
from antfly.client_generated.models.inference_generate_request import InferenceGenerateRequest  # noqa: E402
from antfly.client_generated.models.inference_role import InferenceRole  # noqa: E402
from antfly.client_generated.models.sort_profile import SortProfile  # noqa: E402
from antfly.client_generated.models.transform import Transform  # noqa: E402
from antfly.client_generated.models.transform_op import TransformOp  # noqa: E402
from antfly.client_generated.models.transform_op_type import TransformOpType  # noqa: E402
from antfly.client_generated.types import Unset  # noqa: E402


class ChunkStream(httpx.SyncByteStream):
    def __init__(self, chunks: list[bytes]) -> None:
        self.chunks = chunks
        self.closed = False

    def __iter__(self) -> Iterator[bytes]:
        yield from self.chunks

    def close(self) -> None:
        self.closed = True


def configure_response(mock_httpx: MagicMock, status_code: int, body: object) -> Mock:
    response = Mock(status_code=status_code, reason_phrase="Bad Request" if status_code >= 400 else "OK")
    response.iter_bytes.return_value = [json.dumps(body).encode()]
    mock_httpx.stream.return_value.__enter__.return_value = response
    return response


def install_transport(client: AntflyClient, transport: httpx.MockTransport) -> None:
    generated = client._client
    headers = generated.get_httpx_client().headers
    generated.set_httpx_client(httpx.Client(base_url="http://test", headers=headers, transport=transport))


class TestAntflyClient:
    """Test cases for AntflyClient."""

    def test_transform_operator_names_are_stable(self) -> None:
        assert {member.name: member.value for member in TransformOpType} == {
            "SET": "$set",
            "SET_ON_INSERT": "$setOnInsert",
            "UNSET": "$unset",
            "INC": "$inc",
            "PUSH": "$push",
            "PULL": "$pull",
            "ADD_TO_SET": "$addToSet",
            "MIN": "$min",
            "MAX": "$max",
        }

    def test_generated_create_index_request_is_discriminated_and_path_owned(self) -> None:
        request = CreateEmbeddingsIndexRequest(
            type_=CreateEmbeddingsIndexRequestType.EMBEDDINGS,
            dimension=512,
        ).to_dict()

        assert request["type"] == "embeddings"
        assert request["dimension"] == 512
        assert "name" not in request

    def test_graph_document_filters_parse_to_unambiguous_sdk_types(self) -> None:
        exact = GraphMatchNode.from_dict({"filter": {"term": "beta", "path": "/title"}})
        fuzzy = GraphMatchNode.from_dict({"filter": {"term": "beta", "path": "/title", "fuzziness": 1}})
        numeric = GraphMatchNode.from_dict({"filter": {"numeric_range": {"path": "/score", "min": 0.8}}})

        assert isinstance(exact.filter_, GraphDocumentTermFilter)
        assert isinstance(fuzzy.filter_, GraphDocumentFuzzyFilter)
        assert isinstance(numeric.filter_, GraphDocumentNumericRangeFilter)
        assert exact.to_dict()["filter"] == {"term": "beta", "path": "/title"}
        assert fuzzy.to_dict()["filter"] == {
            "term": "beta",
            "path": "/title",
            "fuzziness": 1,
        }

    def test_min_transform_serializes_from_generated_models(self) -> None:
        request = BatchRequest(
            transforms=[
                Transform(
                    key="doc-1",
                    operations=[TransformOp(op=TransformOpType.MIN, path="priority", value=4)],
                )
            ]
        )

        assert request.to_dict()["transforms"] == [
            {
                "key": "doc-1",
                "operations": [{"op": "$min", "path": "priority", "value": 4}],
                "upsert": False,
            }
        ]

    def test_generate_request_defaults_match_server_and_omit_speculative_k(self) -> None:
        request = InferenceGenerateRequest(
            model="target",
            messages=[InferenceChatMessage(role=InferenceRole.USER, content="hello")],
        ).to_dict()

        assert request["temperature"] == 0
        assert request["top_p"] == 0
        assert request["top_k"] == 0
        assert "speculative_k" not in request

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

    def test_response_limits_must_be_positive(self) -> None:
        with pytest.raises(ValueError, match="response byte limits must be positive"):
            AntflyClient("http://localhost:8080", max_json_response_bytes=0)
        with pytest.raises(ValueError, match="response byte limits must be positive"):
            AntflyClient("http://localhost:8080", max_error_response_bytes=-1)

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

        mock_httpx = MagicMock()
        configure_response(mock_httpx, 200, [])
        mock_client_class.return_value.get_httpx_client.return_value = mock_httpx

        # Re-create client so it picks up the mock
        client = AntflyClient(base_url="http://localhost:8080")
        tables = client.list_tables()

        assert tables == []
        mock_httpx.stream.assert_called_once_with("GET", "/db/v1/tables")

    @patch("antfly.client.Client")
    def test_create_table(self, mock_client_class: MagicMock) -> None:
        """Test creating a table."""
        mock_httpx = MagicMock()
        configure_response(mock_httpx, 200, {"name": "test_table", "shards": {}, "indexes": {}})
        mock_client_class.return_value.get_httpx_client.return_value = mock_httpx

        client = AntflyClient(base_url="http://localhost:8080")
        result = client.create_table(name="test_table", num_shards=2)

        assert result["name"] == "test_table"
        mock_httpx.stream.assert_called_once_with("POST", "/db/v1/tables/test_table", json={"num_shards": 2})

    @patch("antfly.client.Client")
    def test_create_table_failure(self, mock_client_class: MagicMock) -> None:
        """Test handling of create table failure."""
        mock_httpx = MagicMock()
        configure_response(mock_httpx, 400, {"error": "table already exists"})
        mock_client_class.return_value.get_httpx_client.return_value = mock_httpx

        client = AntflyClient(base_url="http://localhost:8080")

        with pytest.raises(AntflyException) as exc_info:
            client.create_table(name="test_table")

        assert "table already exists" in str(exc_info.value)

    @patch("antfly.client.Client")
    def test_create_index_uses_path_identity_and_returns_config(self, mock_client_class: MagicMock) -> None:
        mock_httpx = MagicMock()
        created = {"name": "thumbnail", "type": "embeddings", "dimension": 512}
        configure_response(mock_httpx, 201, created)
        mock_client_class.return_value.get_httpx_client.return_value = mock_httpx

        client = AntflyClient(base_url="http://localhost:8080")
        config = {"type": "embeddings", "dimension": 512}
        result = client.indexes.create("wiki/media", "thumbnail image", config)

        assert isinstance(result, CreatedEmbeddingsIndex)
        assert result.name == "thumbnail"
        assert result.type_.value == "embeddings"
        assert result.dimension == 512
        assert config == {"type": "embeddings", "dimension": 512}
        mock_httpx.stream.assert_called_once_with(
            "POST",
            "/db/v1/tables/wiki%2Fmedia/indexes/thumbnail%20image",
            json=config,
        )

    @patch("antfly.client.Client")
    def test_create_index_accepts_generated_request_model(self, mock_client_class: MagicMock) -> None:
        mock_httpx = MagicMock()
        configure_response(mock_httpx, 201, {"name": "vectors", "type": "embeddings", "dimension": 768})
        mock_client_class.return_value.get_httpx_client.return_value = mock_httpx

        client = AntflyClient(base_url="http://localhost:8080")
        request = CreateEmbeddingsIndexRequest(
            type_=CreateEmbeddingsIndexRequestType.EMBEDDINGS,
            dimension=768,
            embedder=antfly_embedder("antflydb/clipclap"),
        )
        created = client.indexes.create("docs", "vectors", request)

        assert isinstance(created, CreatedEmbeddingsIndex)
        assert created.name == "vectors"
        assert created.dimension == 768
        mock_httpx.stream.assert_called_once_with(
            "POST",
            "/db/v1/tables/docs/indexes/vectors",
            json={
                "type": "embeddings",
                "version": 0,
                "external": False,
                "sparse": False,
                "dimension": 768,
                "embedder": {"provider": "antfly", "model": "antflydb/clipclap"},
                "top_k": 10,
                "min_weight": 0.0,
                "chunk_size": 1024,
            },
        )

    def test_create_index_rejects_duplicate_body_identity(self) -> None:
        client = AntflyClient(base_url="http://localhost:8080")
        with pytest.raises(ValueError, match="owned by the path"):
            client.indexes.create("docs", "search", {"name": "other", "type": "full_text"})

    @patch("antfly.client.Client")
    def test_create_index_preserves_storage_admission_retry(self, mock_client_class: MagicMock) -> None:
        mock_httpx = MagicMock()
        response = configure_response(
            mock_httpx,
            429,
            {
                "code": "storage_resource_exhausted",
                "error": "storage_resource_exhausted",
                "message": "storage capacity is temporarily exhausted",
                "retryable": True,
                "retry_after_ms": 1250,
            },
        )
        response.headers = {"Retry-After": "2"}
        mock_client_class.return_value.get_httpx_client.return_value = mock_httpx

        client = AntflyClient(base_url="http://localhost:8080")
        with pytest.raises(StorageResourceExhaustedError) as exc_info:
            client.indexes.create("docs", "vectors", {"type": "embeddings", "dimension": 512})

        assert exc_info.value.status_code == 429
        assert exc_info.value.code == "storage_resource_exhausted"
        assert exc_info.value.retryable is True
        assert exc_info.value.retry_after_ms == 1250
        assert exc_info.value.retry_after_seconds == 2

    @patch("antfly.client.Client")
    def test_query_preserves_sorted_cursor_contract(self, mock_client_class: MagicMock) -> None:
        """High-level query forwards order_by/search_after/profile and returns generated response model."""
        response_body = {
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
        configure_response(mock_httpx, 200, response_body)
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

        mock_httpx.stream.assert_called_once_with(
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
    def test_query_preserves_legacy_graph_searches_bridge(self, mock_client_class: MagicMock) -> None:
        mock_httpx = MagicMock()
        configure_response(mock_httpx, 200, {"responses": []})
        mock_client_class.return_value.get_httpx_client.return_value = mock_httpx

        client = AntflyClient(base_url="http://localhost:8080")
        client.query(
            table="docs",
            graph_searches={"related": {"type": "neighbors", "index_name": "graph"}},
        )

        mock_httpx.stream.assert_called_once_with(
            "POST",
            "/db/v1/tables/docs/query",
            json={"graph_searches": {"related": {"type": "neighbors", "index_name": "graph"}}},
        )

    def test_query_rejects_mixed_graph_contracts(self) -> None:
        client = AntflyClient(base_url="http://localhost:8080")

        with pytest.raises(AntflyException, match="either graph_queries or graph_searches"):
            client.query(table="docs", graph_queries={}, graph_searches={})

    @patch("antfly.client.Client")
    def test_query_serializes_typed_graph_operations(self, mock_client_class: MagicMock) -> None:
        mock_httpx = MagicMock()
        configure_response(mock_httpx, 200, {"responses": []})
        mock_client_class.return_value.get_httpx_client.return_value = mock_httpx
        graph_query = GraphMatchQuery.from_dict(
            {
                "index": "social",
                "match": {"anchor": "a", "nodes": {"a": {}}, "edges": []},
                "return": {"aggregates": {"rows": {"count": "*"}}},
            }
        )

        client = AntflyClient(base_url="http://localhost:8080")
        client.query(table="docs", graph_queries={"count_rows": graph_query})

        mock_httpx.stream.assert_called_once_with(
            "POST",
            "/db/v1/tables/docs/query",
            json={
                "graph_queries": {
                    "count_rows": {
                        "index": "social",
                        "match": {"anchor": "a", "nodes": {"a": {}}, "edges": []},
                        "return": {"aggregates": {"rows": {"count": "*"}}},
                    }
                }
            },
        )

    def test_query_rejects_invalid_graph_operation_values(self) -> None:
        client = AntflyClient(base_url="http://localhost:8080")

        with pytest.raises(AntflyException, match="generated graph query model or mapping"):
            client.query(table="docs", graph_queries={"bad": 1})  # type: ignore[dict-item]

    @pytest.mark.parametrize("name", [" bad", "bad\u00a0name", "bad\u200bname", "bad\u202ename", "*"])
    def test_query_rejects_unsafe_graph_operation_names(self, name: str) -> None:
        client = AntflyClient(base_url="http://localhost:8080")

        with pytest.raises(AntflyException, match="GraphIdentifier policy"):
            client.query(table="docs", graph_queries={name: {"traverse": {}}})

    def test_query_rejects_unsafe_nested_graph_aliases(self) -> None:
        client = AntflyClient(base_url="http://localhost:8080")
        query = {
            "index": "social",
            "match": {
                "anchor": "person",
                "nodes": {"person": {}, "post\u200bauthor": {}},
                "edges": [{"from": "person", "to": "post\u200bauthor"}],
            },
            "return": {"bindings": ["person"]},
        }

        with pytest.raises(AntflyException, match="match.nodes key"):
            client.query(table="docs", graph_queries={"people": query})

    @pytest.mark.parametrize(
        ("start", "error"),
        [
            ({"result_ref": "$graph_results.bad\u200bname"}, "result_ref query name"),
            (
                {"result_ref": "$graph_results.people", "binding": "bad\u200bname"},
                "traverse.start.binding",
            ),
            (
                {"result_ref": "$query_results", "binding": "person"},
                "binding requires a \\$graph_results",
            ),
        ],
    )
    def test_query_rejects_unsafe_graph_result_selectors(self, start: dict[str, object], error: str) -> None:
        client = AntflyClient(base_url="http://localhost:8080")

        with pytest.raises(AntflyException, match=error):
            client.query(
                table="docs",
                graph_queries={"walk": {"index": "social", "traverse": {"start": start}}},
            )

    def test_query_identifier_preflight_matches_graph_predicate_depth_limit(self) -> None:
        client = AntflyClient(base_url="http://localhost:8080")
        where: dict[str, object] = {"not_equal": {"left": {"alias": "person"}, "right": {"alias": "author"}}}
        for _ in range(16):
            where = {"and": [where]}
        query = {
            "match": {
                "anchor": "person",
                "nodes": {"person": {}, "author": {}},
                "edges": [{"from": "person", "to": "author"}],
                "where": where,
            },
            "return": {"bindings": ["person"]},
        }

        with pytest.raises(AntflyException, match="maximum graph predicate depth"):
            client.query(table="docs", graph_queries={"people": query})

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
        mock_httpx.stream.assert_not_called()

    @patch("antfly.client.Client")
    def test_batch_sends_exact_checked_bytes(self, mock_client_class: MagicMock) -> None:
        """Test batch sends the same bytes used for request-size enforcement."""
        expected_body = {"inserts": {"user:1": {"name": "Zoë"}}, "deletes": []}
        expected_content = json.dumps(expected_body, separators=(",", ":"), ensure_ascii=False).encode("utf-8")

        mock_httpx = MagicMock()
        configure_response(mock_httpx, 201, {"inserted": 1})
        mock_client_class.return_value.get_httpx_client.return_value = mock_httpx

        client = AntflyClient(base_url="http://localhost:8080", max_write_request_bytes=len(expected_content))
        client.batch(table="users", inserts={"user:1": {"name": "Zoë"}})

        mock_httpx.stream.assert_called_once_with(
            "POST",
            "/db/v1/tables/users/batch",
            content=expected_content,
            headers={"Content-Type": "application/json"},
        )

    def test_request_reads_chunked_json_and_closes(self) -> None:
        stream = ChunkStream([b'{"ok":', b"true}"])
        client = AntflyClient("http://test", max_json_response_bytes=16)
        install_transport(client, httpx.MockTransport(lambda _: httpx.Response(200, stream=stream)))

        assert client._request("GET", "/db/v1/test") == {"ok": True}
        assert stream.closed

    @pytest.mark.parametrize("status_code,limit", [(200, 16), (500, 8)])
    def test_request_rejects_oversized_chunked_response_and_closes(self, status_code: int, limit: int) -> None:
        stream = ChunkStream([b'{"value":"', b"x" * 32, b'"}'])
        client = AntflyClient(
            "http://test",
            max_json_response_bytes=limit if status_code == 200 else 64,
            max_error_response_bytes=limit if status_code >= 400 else 64,
        )
        install_transport(client, httpx.MockTransport(lambda _: httpx.Response(status_code, stream=stream)))

        with pytest.raises(AntflyException, match=f"exceeded {limit} bytes"):
            client._request("GET", "/db/v1/test")
        assert stream.closed
