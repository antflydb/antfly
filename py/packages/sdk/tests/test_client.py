"""Tests for Antfly client."""

import json
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta, timezone
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
    IndexMutationTemporarilyUnavailableError,
    StorageResourceExhaustedError,
    antfly_embedder,
    count_graph_alias,
    count_graph_rows,
    graph_date_range_filter,
    graph_numeric_range_filter,
    graph_term_range_filter,
)
from antfly.client import normalize_base_url  # noqa: E402
from antfly.client_generated.models.batch_request import BatchRequest  # noqa: E402
from antfly.client_generated.models.graph_index_stats import GraphIndexStats  # noqa: E402
from antfly.client_generated.models.graph_index_stats_index_type import GraphIndexStatsIndexType  # noqa: E402
from antfly.client_generated.models.graph_metric_build_page_status import GraphMetricBuildPageStatus  # noqa: E402
from antfly.client_generated.models.graph_metric_build_page_status_range_kind import (
    GraphMetricBuildPageStatusRangeKind,  # noqa: E402
)
from antfly.client_generated.models.graph_metric_query import GraphMetricQuery  # noqa: E402
from antfly.client_generated.models.graph_metric_query_metric_freshness import (
    GraphMetricQueryMetricFreshness,  # noqa: E402
)
from antfly.client_generated.models.graph_metric_runtime_stats import GraphMetricRuntimeStats  # noqa: E402
from antfly.client_generated.models.graph_metric_runtime_stats_role import GraphMetricRuntimeStatsRole  # noqa: E402
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

    def test_graph_metric_summary_build_page_deserializes(self) -> None:
        page = GraphMetricBuildPageStatus.from_dict(
            {
                "phase": "reduce_ranks",
                "iteration": 2,
                "page_id": 0,
                "state": "complete",
                "range_kind": "summary",
            }
        )

        assert page.range_kind is GraphMetricBuildPageStatusRangeKind.SUMMARY

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

    def test_graph_index_stats_model_serializes_metric_runtime(self) -> None:
        stats = GraphIndexStats(
            index_type=GraphIndexStatsIndexType.GRAPH,
            total_edges=4,
            graph_metric_runtime=GraphMetricRuntimeStats(
                enabled=True,
                role=GraphMetricRuntimeStatsRole.WORKER_POOL,
                owner_id_hash=17,
                worker_count=3,
                takeover_count=2,
                lost_leases=1,
                total_pages_claimed=6,
                last_pages_completed=3,
                last_budget_exhausted=True,
            ),
        )

        stats_dict = stats.to_dict()
        assert stats_dict["graph_metric_runtime"]["role"] == "worker_pool"
        assert stats_dict["graph_metric_runtime"]["owner_id_hash"] == 17
        assert stats_dict["graph_metric_runtime"]["last_budget_exhausted"] is True

        round_tripped = GraphIndexStats.from_dict(stats_dict)
        assert isinstance(round_tripped.graph_metric_runtime, GraphMetricRuntimeStats)
        assert round_tripped.graph_metric_runtime.role == GraphMetricRuntimeStatsRole.WORKER_POOL
        assert round_tripped.graph_metric_runtime.worker_count == 3
        assert round_tripped.graph_metric_runtime.total_pages_claimed == 6

    def test_direct_graph_metric_query_round_trip(self) -> None:
        query = GraphMetricQuery(
            name="central",
            index="graph_idx",
            metric="pagerank",
            top_k=25,
            metric_freshness=GraphMetricQueryMetricFreshness.FRESH,
        )

        payload = query.to_dict()
        assert payload == {
            "name": "central",
            "index": "graph_idx",
            "metric": "pagerank",
            "top_k": 25,
            "metric_freshness": "fresh",
        }
        assert GraphMetricQuery.from_dict(payload) == query

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
    def test_create_table_rejects_invalid_inline_index_before_transport(self, mock_client_class: MagicMock) -> None:
        mock_httpx = MagicMock()
        mock_client_class.return_value.get_httpx_client.return_value = mock_httpx
        client = AntflyClient(base_url="http://localhost:8080")

        with pytest.raises(ValueError, match=r"invalid index 'semantic'.*embedding_name"):
            client.create_table(
                name="test_table",
                indexes={
                    "semantic": {
                        "type": "embeddings",
                        "source_artifact_name": "document_chunks_v1",
                    }
                },
            )

        mock_httpx.stream.assert_not_called()

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
    def test_create_index_rejects_invalid_relationships_before_transport(self, mock_client_class: MagicMock) -> None:
        mock_httpx = MagicMock()
        mock_client_class.return_value.get_httpx_client.return_value = mock_httpx
        client = AntflyClient(base_url="http://localhost:8080")

        with pytest.raises(ValueError, match="requires a non-empty embedding_name"):
            client.indexes.create(
                "docs",
                "vectors",
                {"type": "embeddings", "source_artifact_name": "chunks_v1"},
            )
        mock_httpx.stream.assert_not_called()

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
    def test_create_index_preserves_temporary_mutation_retry(self, mock_client_class: MagicMock) -> None:
        mock_httpx = MagicMock()
        response = configure_response(
            mock_httpx,
            503,
            {
                "error": "index_probe_unavailable",
                "message": "model probe is temporarily unavailable",
                "retryable": True,
            },
        )
        response.headers = {"Retry-After": "4"}
        mock_client_class.return_value.get_httpx_client.return_value = mock_httpx

        client = AntflyClient(base_url="http://localhost:8080")
        with pytest.raises(IndexMutationTemporarilyUnavailableError) as exc_info:
            client.indexes.create("docs", "vectors", {"type": "embeddings", "dimension": 512})

        assert exc_info.value.status_code == 503
        assert exc_info.value.code == "index_probe_unavailable"
        assert exc_info.value.retryable is True
        assert exc_info.value.retry_after_seconds == 4

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

    @patch("antfly.client.Client")
    def test_query_forwards_named_full_text_index(self, mock_client_class: MagicMock) -> None:
        mock_httpx = MagicMock()
        configure_response(mock_httpx, 200, {"responses": []})
        mock_client_class.return_value.get_httpx_client.return_value = mock_httpx

        client = AntflyClient(base_url="http://localhost:8080")
        client.query(
            table="docs",
            full_text_search={"match": "singularity", "field": "text"},
            full_text_index="document_text",
        )

        mock_httpx.stream.assert_called_once_with(
            "POST",
            "/db/v1/tables/docs/query",
            json={
                "full_text_search": {"match": "singularity", "field": "text"},
                "full_text_index": "document_text",
            },
        )

    def test_query_rejects_ambiguous_aggregation_aliases(self) -> None:
        client = AntflyClient(base_url="http://localhost:8080")

        with pytest.raises(AntflyException, match="either aggregations or facets"):
            client.query(table="docs", aggregations={"a": {}}, facets={"b": {}})

    @patch("antfly.client.Client")
    def test_query_serializes_typed_graph_operations(self, mock_client_class: MagicMock) -> None:
        mock_httpx = MagicMock()
        configure_response(
            mock_httpx,
            200,
            {
                "responses": [
                    {
                        "took": 0,
                        "status": 200,
                        "graph_results": {
                            "count_rows": {
                                "kind": "aggregates",
                                "aggregates": {"rows": {"value": "0", "exact": True}},
                                "stats": {"returned_items": 1},
                            }
                        },
                    }
                ]
            },
        )
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

    def test_graph_count_helpers_construct_valid_generated_models(self) -> None:
        assert count_graph_rows().to_dict() == {"count": "*"}
        assert count_graph_alias("person").to_dict() == {
            "count": "person",
            "distinct": False,
        }
        assert count_graph_alias("person", distinct=True).to_dict() == {
            "count": "person",
            "distinct": True,
        }
        with pytest.raises(AntflyException, match="graph count alias"):
            count_graph_alias("*")

    def test_graph_range_helpers_are_validated_and_operation_keyed(self) -> None:
        assert graph_numeric_range_filter("/score", min_value=0, inclusive_min=True).to_dict() == {
            "numeric_range": {"path": "/score", "min": 0, "inclusive_min": True}
        }
        assert graph_term_range_filter("/status", max_value="z").to_dict() == {
            "term_range": {"path": "/status", "max": "z"}
        }
        assert graph_date_range_filter("/created_at", start=datetime(2026, 1, 1, tzinfo=UTC)).to_dict() == {
            "date_range": {"path": "/created_at", "start": "2026-01-01T00:00:00+00:00"}
        }
        with pytest.raises(AntflyException, match="requires min_value or max_value"):
            graph_numeric_range_filter("/score")
        with pytest.raises(AntflyException, match="RFC 6901"):
            graph_term_range_filter("score", min_value="a")
        with pytest.raises(AntflyException, match="finite number"):
            graph_numeric_range_filter("/score", min_value=True)
        with pytest.raises(AntflyException, match="min_value must not exceed max_value"):
            graph_numeric_range_filter("/score", min_value=2, max_value=1)
        with pytest.raises(AntflyException, match="must be a boolean"):
            graph_numeric_range_filter("/score", min_value=1, inclusive_min="false")  # type: ignore[arg-type]
        with pytest.raises(AntflyException, match="must be a string"):
            graph_term_range_filter("/status", min_value=1)  # type: ignore[arg-type]
        with pytest.raises(AntflyException, match="timezone-aware"):
            graph_date_range_filter("/created_at", start=datetime(2026, 1, 1))
        with pytest.raises(AntflyException, match="start must not exceed end"):
            graph_date_range_filter(
                "/created_at",
                start=datetime(2026, 1, 2, tzinfo=UTC),
                end=datetime(2026, 1, 1, tzinfo=UTC),
            )
        with pytest.raises(AntflyException, match="supported Unix-nanosecond range"):
            graph_date_range_filter("/created_at", start=datetime(1969, 12, 31, tzinfo=UTC))
        with pytest.raises(AntflyException, match="supported Unix-nanosecond range"):
            graph_date_range_filter(
                "/created_at",
                start=datetime(1, 1, 1, tzinfo=timezone(timedelta(hours=14))),
            )
        assert (
            graph_date_range_filter(
                "/created_at",
                end=datetime(2554, 7, 21, 23, 34, 33, 709551, tzinfo=UTC),
            ).to_dict()["date_range"]["end"]
            == "2554-07-21T23:34:33.709551+00:00"
        )
        with pytest.raises(AntflyException, match="supported Unix-nanosecond range"):
            graph_date_range_filter("/created_at", end=datetime(2554, 7, 21, 23, 34, 34, tzinfo=UTC))
        normalized = graph_date_range_filter(
            "/created_at",
            start=datetime(2300, 1, 1, tzinfo=timezone(timedelta(seconds=30))),
        ).to_dict()
        assert normalized["date_range"]["start"] == "2299-12-31T23:59:30+00:00"

    @pytest.mark.parametrize("distinct", [False, True])
    def test_query_rejects_distinct_presence_on_row_count(self, distinct: bool) -> None:
        client = AntflyClient(base_url="http://localhost:8080")
        graph_query = {
            "index": "social",
            "match": {"anchor": "a", "nodes": {"a": {}}, "edges": []},
            "return": {"aggregates": {"rows": {"count": "*", "distinct": distinct}}},
        }

        with pytest.raises(AntflyException, match="distinct is only valid for alias counts"):
            client.query(table="docs", graph_queries={"count_rows": graph_query})

    def test_query_rejects_invalid_graph_operation_values(self) -> None:
        client = AntflyClient(base_url="http://localhost:8080")

        with pytest.raises(AntflyException, match="generated graph query model or mapping"):
            client.query(table="docs", graph_queries={"bad": 1})  # type: ignore[dict-item]

    def test_query_rejects_empty_graph_queries(self) -> None:
        client = AntflyClient(base_url="http://localhost:8080")

        with pytest.raises(AntflyException, match="at least one named operation"):
            client.query(table="docs", graph_queries={})

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
        ("edge_types", "error"),
        [
            (["bad\ud800"], "valid UTF-8"),
            (["links", "links"], "duplicate edge types"),
            (["文" * (65_536 // 3 + 1)], "at most 65536 UTF-8 bytes"),
        ],
    )
    def test_query_rejects_edge_types_outside_wire_policy(self, edge_types: list[str], error: str) -> None:
        client = AntflyClient(base_url="http://localhost:8080")
        query = {
            "match": {
                "anchor": "person",
                "nodes": {"person": {}, "author": {}},
                "edges": [{"from": "person", "to": "author", "types": edge_types}],
            },
            "return": {"bindings": ["person"]},
        }

        with pytest.raises(AntflyException, match=error):
            client.query(table="docs", graph_queries={"people": query})

    def test_query_rejects_invalid_graph_match_edge_direction(self) -> None:
        client = AntflyClient(base_url="http://localhost:8080")
        query = {
            "match": {
                "anchor": "person",
                "nodes": {"person": {}, "author": {}},
                "edges": [{"from": "person", "to": "author", "direction": "sideways"}],
            },
            "return": {"bindings": ["person"]},
        }

        with pytest.raises(AntflyException, match="direction must be out, in, or both"):
            client.query(table="docs", graph_queries={"people": query})

        anti_join_query = {
            "match": {
                "anchor": "person",
                "nodes": {"person": {}, "author": {}},
                "edges": [],
                "where": {"not_exists": {"edges": [{"from": "person", "to": "author", "direction": "sideways"}]}},
            },
            "return": {"bindings": ["person"]},
        }
        with pytest.raises(AntflyException, match=r"not_exists\.edges\[0\]\.direction must be out, in, or both"):
            client.query(table="docs", graph_queries={"people": anti_join_query})

    @pytest.mark.parametrize(
        "query",
        [
            {
                "match": {
                    "anchor": "person",
                    "nodes": {"person": {}, "author": {}},
                    "edges": [{"from": "person", "to": "author", "edge_weight": {"min": -0.1}}],
                },
                "return": {"bindings": ["person"]},
            },
            {"traverse": {"start": {"keys": ["doc:a"]}, "edge_weight": {"max": -0.1}}},
            {"shortest_path": {"from": {"key": "doc:a"}, "to": {"key": "doc:b"}, "edge_weight": {"min": -0.1}}},
            {
                "k_shortest_paths": {
                    "from": {"key": "doc:a"},
                    "to": {"key": "doc:b"},
                    "k": 2,
                    "edge_weight": {"max": -0.1},
                }
            },
        ],
    )
    def test_query_rejects_negative_canonical_graph_weight_bounds(self, query: dict[str, object]) -> None:
        client = AntflyClient(base_url="http://localhost:8080")
        with pytest.raises(AntflyException, match="finite non-negative number"):
            client.query(table="docs", graph_queries={"walk": {"index": "graph_idx", **query}})

    @pytest.mark.parametrize(
        "query",
        [
            {"traverse": {"start": {"keys": ["doc:a"]}, "edge_weight": None}},
            {"traverse": {"start": {"keys": ["doc:a"]}, "edge_weight": {}}},
            {
                "shortest_path": {
                    "from": {"key": "doc:a"},
                    "to": {"key": "doc:b"},
                    "objective": None,
                }
            },
        ],
    )
    def test_query_rejects_empty_or_null_canonical_path_options(self, query: dict[str, object]) -> None:
        client = AntflyClient(base_url="http://localhost:8080")
        with pytest.raises(AntflyException):
            client.query(table="docs", graph_queries={"walk": {"index": "graph_idx", **query}})

    @pytest.mark.parametrize("operation", ["traverse", "shortest_path", "k_shortest_paths"])
    def test_query_validates_graph_operation_direction(self, operation: str) -> None:
        if operation == "traverse":
            body: dict[str, object] = {"start": {"keys": ["doc:a"]}, "direction": "both"}
        else:
            body = {
                "from": {"key": "doc:a"},
                "to": {"key": "doc:b"},
                "direction": "both",
            }
            if operation == "k_shortest_paths":
                body["k"] = 2

        with patch("antfly.client.Client") as mock_client_class:
            mock_httpx = MagicMock()
            configure_response(
                mock_httpx,
                200,
                {
                    "responses": [
                        {
                            "took": 0,
                            "status": 200,
                            "graph_results": {
                                "walk": {
                                    "kind": "nodes" if operation == "traverse" else "paths",
                                    **({"nodes": []} if operation == "traverse" else {"paths": []}),
                                    "stats": {
                                        "returned_items": 0,
                                        **({"truncated": False} if operation == "traverse" else {}),
                                    },
                                }
                            },
                        }
                    ]
                },
            )
            mock_client_class.return_value.get_httpx_client.return_value = mock_httpx
            client = AntflyClient(base_url="http://localhost:8080")
            client.query(table="docs", graph_queries={"walk": {"index": "graph_idx", operation: body}})
            mock_httpx.stream.assert_called_once()

        body["direction"] = "sideways"
        client = AntflyClient(base_url="http://localhost:8080")
        with pytest.raises(AntflyException, match="direction must be out, in, or both"):
            client.query(table="docs", graph_queries={"walk": {"index": "graph_idx", operation: body}})

    def test_query_rejects_more_than_eight_match_operations(self) -> None:
        client = AntflyClient(base_url="http://localhost:8080")
        graph_queries = {
            f"match_{index}": {
                "index": "graph_idx",
                "match": {"anchor": "node", "nodes": {"node": {}}, "edges": []},
                "return": {"bindings": ["node"]},
            }
            for index in range(9)
        }
        with pytest.raises(AntflyException, match="at most 8 match operations"):
            client.query(table="docs", graph_queries=graph_queries)

    def test_query_graph_hydration_and_table_qualifiers_fail_before_io(self) -> None:
        client = AntflyClient(base_url="http://localhost:8080")
        with pytest.raises(AntflyException, match="table must contain a non-whitespace"):
            client.query(
                table="docs",
                graph_queries={
                    "match": {
                        "index": "graph",
                        "match": {"anchor": "a", "nodes": {"a": {"table": " \t"}}, "edges": []},
                        "return": {"bindings": ["a"]},
                    }
                },
            )
        with pytest.raises(AntflyException, match="fields requires include_documents=true"):
            client.query(
                table="docs",
                graph_queries={"walk": {"index": "graph", "traverse": {"start": {"keys": ["a"]}, "fields": ["title"]}}},
            )
        with pytest.raises(AntflyException, match="maximum is 10000"):
            client.query(
                table="docs",
                graph_queries={
                    "match": {
                        "index": "graph",
                        "match": {"anchor": "a", "nodes": {"a": {}, "b": {}}, "edges": []},
                        "return": {"bindings": ["a", "b"], "limit": 5001, "include_documents": True},
                    }
                },
            )

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
