from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.enrichment_context_cypher import EnrichmentContextCypher
    from ..models.enrichment_context_query_embeddings import EnrichmentContextQueryEmbeddings
    from ..models.enrichment_context_query_exclusion_query import EnrichmentContextQueryExclusionQuery
    from ..models.enrichment_context_query_filter_query import EnrichmentContextQueryFilterQuery
    from ..models.enrichment_context_query_full_text_search import EnrichmentContextQueryFullTextSearch
    from ..models.enrichment_context_query_graph_queries import EnrichmentContextQueryGraphQueries
    from ..models.enrichment_context_query_merge_config import EnrichmentContextQueryMergeConfig
    from ..models.enrichment_context_query_order_by import EnrichmentContextQueryOrderBy
    from ..models.enrichment_context_query_reranker import EnrichmentContextQueryReranker
    from ..models.enrichment_context_query_vector import EnrichmentContextQueryVector
    from ..models.enrichment_context_sql import EnrichmentContextSql


T = TypeVar("T", bound="EnrichmentContextQuery")


@_attrs_define
class EnrichmentContextQuery:
    """A filter (tracked live) or a ranked retrieval (a snapshot). Any ranking clause makes the whole query a public
    QueryRequest executed by the table's query engine, with `{"$source":"/pointer"}` bindings anywhere; `limit` (at most
    1000) is then the member count, and paging (`offset`, `search_after`, `search_before`), `count`, `aggregations`, and
    a top-N `order_by` are rejected. Distributed tables refuse `semantic_search` (data servers have no query embedder);
    pass precomputed vectors in `embeddings` instead.

        Attributes:
            table (str | Unset): Table queried; omitted means the enrichment's own table.
            filter_query (EnrichmentContextQueryFilterQuery | Unset): An Antfly `filter_query` (Query shape): `bool`
                (`must`/`filter`/`should`/`must_not`/`minimum_should_match`, filter-context semantics), `conjuncts`, `disjuncts`
                (`min`), `match_all`, `match_none`, and leaves. Leaves evaluated by context resolution itself: `term`/`terms`
                (equality; at the top of the conjunction they form indexed subscriptions, at most one bound `terms` set),
                `range` (`gt`/`gte`/`lt`/`lte`), `numeric_range`/`term_range` (`min`/`max`/`inclusive_*`), `prefix`, `match` and
                `match_phrase` (pinned `antfly.context.simple.v1` analyzer: lowercase ASCII words; `operator` and|or), and
                `similar` (`{vector, threshold, metric: cosine|dot, dims, model}` over a vector stored in a document field).
                Every other row-decidable leaf (wildcard, regexp, fuzzy, date and IP ranges, geo, bool fields, `exists`, doc
                ids, `equals`/`contains`/`matches`) is lowered at admission into the storage engine's per-row filter and
                evaluated per changed row. Values are literals or typed bindings `{"$source":"/json/pointer"}` from the source
                document (in storage-evaluated leaves, string-valued positions only). Corpus-relative text queries
                (`query_string`, `multi_match`, analyzer-specific `match`) are rejected in a live filter; express them as a
                ranking clause (`full_text_search`) instead.
            order_by (EnrichmentContextQueryOrderBy | Unset): Live filters only: top-N by a field (ties by key);
                `max_documents` is N. Invalidation tracks the current boundary.
            full_text_search (EnrichmentContextQueryFullTextSearch | Unset): Ranking clause: full-text relevance
                (QueryRequest shape).
            semantic_search (Any | Unset): Ranking clause: query text embedded for vector search (QueryRequest shape).
            embeddings (EnrichmentContextQueryEmbeddings | Unset): Ranking clause: precomputed query vectors by index
                (QueryRequest shape).
            indexes (list[str] | Unset):
            merge_config (EnrichmentContextQueryMergeConfig | Unset):
            reranker (EnrichmentContextQueryReranker | Unset):
            exclusion_query (EnrichmentContextQueryExclusionQuery | Unset):
            graph_queries (EnrichmentContextQueryGraphQueries | Unset): Graph operations (QueryRequest shape). Exactly one
                `traverse` from the dependent row (or `start: {keys: [{"$source": "/pointer"}]}`) with only `edge_types`,
                `direction`, `max_depth` (at most 3), and the context options `max_fanout` (edges per visited node, default 100)
                and `include_generated` (also follow field-derived and enrichment-generated edges) is a live traversal:
                dependents watch every visited node's edges and every reached row, members render with depth, edge type, and
                direction, and `max_documents` bounds the rows reached. A `filter_query` beside it narrows the reached rows and
                stays live. Anything else (`match`, shortest paths, `edge_filter`, `edge_weight`, `limit`, metrics, deeper
                traversals, several operations) runs in the query engine as a snapshot.
            cypher (EnrichmentContextCypher | Unset): Ranking clause: a read-only Cypher statement against a graph, run as
                the principal that declared the enrichment (its graph and table permissions and row policies apply; the server
                binds the declaration to that principal, and revoking the credential stops rebuilds). Members are the nodes of
                the source `table` the statement returns, anywhere in a row (bare nodes, paths, lists, maps), in result order,
                each once, up to `limit`, and render as those rows' `fields`. Nodes of other tables are context the statement
                used, not members. Only `limit` may sit beside it. A Cypher source is a snapshot: a statement's result depends
                on rows it does not return, so dependents rebuild on `refresh_interval` or manually. Statements that write
                (`CREATE`, `MERGE`, `SET`, `DELETE`, `REMOVE`) or `EXPLAIN` are rejected.
            sql (EnrichmentContextSql | Unset): Ranking clause: a read-only SQL `SELECT` (see `POST /sql`), run as the
                principal that declared the enrichment (its table permissions and row policies apply; the server binds the
                declaration to that principal, and revoking the credential stops rebuilds). Members are the rows of the source
                `table` named by the result's `_id` column (every table exposes its document key as `_id`; alias another key
                column `AS _id`), in result order, each once, up to `limit`, and render as those rows' `fields`. A result
                without an `_id` column fails. Results are admitted up to 4096 rows; a larger result fails rather than
                truncating, so bound it with `LIMIT`. Only `limit` may sit beside it. A SQL source is a snapshot: dependents
                rebuild on `refresh_interval` or manually. Any statement but a `SELECT` is rejected. Embedded Lite resolves
                names in its single catalog and rejects `database` and `namespace`.
            limit (int | Unset): Ranked queries: member count.
            text (Any | Unset): Shorthand ranking clause: query text literal or `{"$source":"/pointer"}`, with `text_field`.
            text_field (str | Unset):
            vector (EnrichmentContextQueryVector | Unset): Shorthand ranking clause: `{"$source":"/pointer"}` to a query
                vector in the dependent row, with `vector_index`.
            vector_index (str | Unset):
            k (int | Unset):  Default: 10.
    """

    table: str | Unset = UNSET
    filter_query: EnrichmentContextQueryFilterQuery | Unset = UNSET
    order_by: EnrichmentContextQueryOrderBy | Unset = UNSET
    full_text_search: EnrichmentContextQueryFullTextSearch | Unset = UNSET
    semantic_search: Any | Unset = UNSET
    embeddings: EnrichmentContextQueryEmbeddings | Unset = UNSET
    indexes: list[str] | Unset = UNSET
    merge_config: EnrichmentContextQueryMergeConfig | Unset = UNSET
    reranker: EnrichmentContextQueryReranker | Unset = UNSET
    exclusion_query: EnrichmentContextQueryExclusionQuery | Unset = UNSET
    graph_queries: EnrichmentContextQueryGraphQueries | Unset = UNSET
    cypher: EnrichmentContextCypher | Unset = UNSET
    sql: EnrichmentContextSql | Unset = UNSET
    limit: int | Unset = UNSET
    text: Any | Unset = UNSET
    text_field: str | Unset = UNSET
    vector: EnrichmentContextQueryVector | Unset = UNSET
    vector_index: str | Unset = UNSET
    k: int | Unset = 10

    def to_dict(self) -> dict[str, Any]:
        table = self.table

        filter_query: dict[str, Any] | Unset = UNSET
        if not isinstance(self.filter_query, Unset):
            filter_query = self.filter_query.to_dict()

        order_by: dict[str, Any] | Unset = UNSET
        if not isinstance(self.order_by, Unset):
            order_by = self.order_by.to_dict()

        full_text_search: dict[str, Any] | Unset = UNSET
        if not isinstance(self.full_text_search, Unset):
            full_text_search = self.full_text_search.to_dict()

        semantic_search = self.semantic_search

        embeddings: dict[str, Any] | Unset = UNSET
        if not isinstance(self.embeddings, Unset):
            embeddings = self.embeddings.to_dict()

        indexes: list[str] | Unset = UNSET
        if not isinstance(self.indexes, Unset):
            indexes = self.indexes

        merge_config: dict[str, Any] | Unset = UNSET
        if not isinstance(self.merge_config, Unset):
            merge_config = self.merge_config.to_dict()

        reranker: dict[str, Any] | Unset = UNSET
        if not isinstance(self.reranker, Unset):
            reranker = self.reranker.to_dict()

        exclusion_query: dict[str, Any] | Unset = UNSET
        if not isinstance(self.exclusion_query, Unset):
            exclusion_query = self.exclusion_query.to_dict()

        graph_queries: dict[str, Any] | Unset = UNSET
        if not isinstance(self.graph_queries, Unset):
            graph_queries = self.graph_queries.to_dict()

        cypher: dict[str, Any] | Unset = UNSET
        if not isinstance(self.cypher, Unset):
            cypher = self.cypher.to_dict()

        sql: dict[str, Any] | Unset = UNSET
        if not isinstance(self.sql, Unset):
            sql = self.sql.to_dict()

        limit = self.limit

        text = self.text

        text_field = self.text_field

        vector: dict[str, Any] | Unset = UNSET
        if not isinstance(self.vector, Unset):
            vector = self.vector.to_dict()

        vector_index = self.vector_index

        k = self.k

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if table is not UNSET:
            field_dict["table"] = table
        if filter_query is not UNSET:
            field_dict["filter_query"] = filter_query
        if order_by is not UNSET:
            field_dict["order_by"] = order_by
        if full_text_search is not UNSET:
            field_dict["full_text_search"] = full_text_search
        if semantic_search is not UNSET:
            field_dict["semantic_search"] = semantic_search
        if embeddings is not UNSET:
            field_dict["embeddings"] = embeddings
        if indexes is not UNSET:
            field_dict["indexes"] = indexes
        if merge_config is not UNSET:
            field_dict["merge_config"] = merge_config
        if reranker is not UNSET:
            field_dict["reranker"] = reranker
        if exclusion_query is not UNSET:
            field_dict["exclusion_query"] = exclusion_query
        if graph_queries is not UNSET:
            field_dict["graph_queries"] = graph_queries
        if cypher is not UNSET:
            field_dict["cypher"] = cypher
        if sql is not UNSET:
            field_dict["sql"] = sql
        if limit is not UNSET:
            field_dict["limit"] = limit
        if text is not UNSET:
            field_dict["text"] = text
        if text_field is not UNSET:
            field_dict["text_field"] = text_field
        if vector is not UNSET:
            field_dict["vector"] = vector
        if vector_index is not UNSET:
            field_dict["vector_index"] = vector_index
        if k is not UNSET:
            field_dict["k"] = k

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.enrichment_context_cypher import EnrichmentContextCypher
        from ..models.enrichment_context_query_embeddings import EnrichmentContextQueryEmbeddings
        from ..models.enrichment_context_query_exclusion_query import EnrichmentContextQueryExclusionQuery
        from ..models.enrichment_context_query_filter_query import EnrichmentContextQueryFilterQuery
        from ..models.enrichment_context_query_full_text_search import EnrichmentContextQueryFullTextSearch
        from ..models.enrichment_context_query_graph_queries import EnrichmentContextQueryGraphQueries
        from ..models.enrichment_context_query_merge_config import EnrichmentContextQueryMergeConfig
        from ..models.enrichment_context_query_order_by import EnrichmentContextQueryOrderBy
        from ..models.enrichment_context_query_reranker import EnrichmentContextQueryReranker
        from ..models.enrichment_context_query_vector import EnrichmentContextQueryVector
        from ..models.enrichment_context_sql import EnrichmentContextSql

        if set(src_dict) - {
            "table",
            "filter_query",
            "order_by",
            "full_text_search",
            "semantic_search",
            "embeddings",
            "indexes",
            "merge_config",
            "reranker",
            "exclusion_query",
            "graph_queries",
            "cypher",
            "sql",
            "limit",
            "text",
            "text_field",
            "vector",
            "vector_index",
            "k",
        }:
            raise ValueError("Unexpected properties for EnrichmentContextQuery")
        d = dict(src_dict)
        table = d.pop("table", UNSET)

        _filter_query = d.pop("filter_query", UNSET)
        filter_query: EnrichmentContextQueryFilterQuery | Unset
        if isinstance(_filter_query, Unset):
            filter_query = UNSET
        else:
            filter_query = EnrichmentContextQueryFilterQuery.from_dict(_filter_query)

        _order_by = d.pop("order_by", UNSET)
        order_by: EnrichmentContextQueryOrderBy | Unset
        if isinstance(_order_by, Unset):
            order_by = UNSET
        else:
            order_by = EnrichmentContextQueryOrderBy.from_dict(_order_by)

        _full_text_search = d.pop("full_text_search", UNSET)
        full_text_search: EnrichmentContextQueryFullTextSearch | Unset
        if isinstance(_full_text_search, Unset):
            full_text_search = UNSET
        else:
            full_text_search = EnrichmentContextQueryFullTextSearch.from_dict(_full_text_search)

        semantic_search = d.pop("semantic_search", UNSET)

        _embeddings = d.pop("embeddings", UNSET)
        embeddings: EnrichmentContextQueryEmbeddings | Unset
        if isinstance(_embeddings, Unset):
            embeddings = UNSET
        else:
            embeddings = EnrichmentContextQueryEmbeddings.from_dict(_embeddings)

        indexes = cast(list[str], d.pop("indexes", UNSET))

        _merge_config = d.pop("merge_config", UNSET)
        merge_config: EnrichmentContextQueryMergeConfig | Unset
        if isinstance(_merge_config, Unset):
            merge_config = UNSET
        else:
            merge_config = EnrichmentContextQueryMergeConfig.from_dict(_merge_config)

        _reranker = d.pop("reranker", UNSET)
        reranker: EnrichmentContextQueryReranker | Unset
        if isinstance(_reranker, Unset):
            reranker = UNSET
        else:
            reranker = EnrichmentContextQueryReranker.from_dict(_reranker)

        _exclusion_query = d.pop("exclusion_query", UNSET)
        exclusion_query: EnrichmentContextQueryExclusionQuery | Unset
        if isinstance(_exclusion_query, Unset):
            exclusion_query = UNSET
        else:
            exclusion_query = EnrichmentContextQueryExclusionQuery.from_dict(_exclusion_query)

        _graph_queries = d.pop("graph_queries", UNSET)
        graph_queries: EnrichmentContextQueryGraphQueries | Unset
        if isinstance(_graph_queries, Unset):
            graph_queries = UNSET
        else:
            graph_queries = EnrichmentContextQueryGraphQueries.from_dict(_graph_queries)

        _cypher = d.pop("cypher", UNSET)
        cypher: EnrichmentContextCypher | Unset
        if isinstance(_cypher, Unset):
            cypher = UNSET
        else:
            cypher = EnrichmentContextCypher.from_dict(_cypher)

        _sql = d.pop("sql", UNSET)
        sql: EnrichmentContextSql | Unset
        if isinstance(_sql, Unset):
            sql = UNSET
        else:
            sql = EnrichmentContextSql.from_dict(_sql)

        limit = d.pop("limit", UNSET)

        text = d.pop("text", UNSET)

        text_field = d.pop("text_field", UNSET)

        _vector = d.pop("vector", UNSET)
        vector: EnrichmentContextQueryVector | Unset
        if isinstance(_vector, Unset):
            vector = UNSET
        else:
            vector = EnrichmentContextQueryVector.from_dict(_vector)

        vector_index = d.pop("vector_index", UNSET)

        k = d.pop("k", UNSET)

        enrichment_context_query = cls(
            table=table,
            filter_query=filter_query,
            order_by=order_by,
            full_text_search=full_text_search,
            semantic_search=semantic_search,
            embeddings=embeddings,
            indexes=indexes,
            merge_config=merge_config,
            reranker=reranker,
            exclusion_query=exclusion_query,
            graph_queries=graph_queries,
            cypher=cypher,
            sql=sql,
            limit=limit,
            text=text,
            text_field=text_field,
            vector=vector,
            vector_index=vector_index,
            k=k,
        )

        return enrichment_context_query
