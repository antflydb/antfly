from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.query_request_expand_strategy import QueryRequestExpandStrategy
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.analyses import Analyses
    from ..models.bool_field_query import BoolFieldQuery
    from ..models.boolean_query import BooleanQuery
    from ..models.conjunction_query import ConjunctionQuery
    from ..models.date_range_string_query import DateRangeStringQuery
    from ..models.disjunction_query import DisjunctionQuery
    from ..models.doc_id_query import DocIdQuery
    from ..models.fuzzy_query import FuzzyQuery
    from ..models.geo_bounding_box_query import GeoBoundingBoxQuery
    from ..models.geo_bounding_polygon_query import GeoBoundingPolygonQuery
    from ..models.geo_distance_query import GeoDistanceQuery
    from ..models.geo_shape_query import GeoShapeQuery
    from ..models.ip_range_query import IPRangeQuery
    from ..models.join_clause import JoinClause
    from ..models.match_all_query import MatchAllQuery
    from ..models.match_none_query import MatchNoneQuery
    from ..models.match_phrase_query import MatchPhraseQuery
    from ..models.match_query import MatchQuery
    from ..models.merge_config import MergeConfig
    from ..models.multi_match_query import MultiMatchQuery
    from ..models.multi_phrase_query import MultiPhraseQuery
    from ..models.numeric_range_query import NumericRangeQuery
    from ..models.phrase_query import PhraseQuery
    from ..models.prefix_query import PrefixQuery
    from ..models.pruner import Pruner
    from ..models.query_hierarchy import QueryHierarchy
    from ..models.query_request_aggregations import QueryRequestAggregations
    from ..models.query_request_embeddings import QueryRequestEmbeddings
    from ..models.query_request_foreign_sources import QueryRequestForeignSources
    from ..models.query_request_graph_queries import QueryRequestGraphQueries
    from ..models.query_request_graph_searches import QueryRequestGraphSearches
    from ..models.query_request_query import QueryRequestQuery
    from ..models.query_string_query import QueryStringQuery
    from ..models.regexp_query import RegexpQuery
    from ..models.reranker_config import RerankerConfig
    from ..models.sort_field import SortField
    from ..models.term_query import TermQuery
    from ..models.term_range_query import TermRangeQuery
    from ..models.tree_search_config import TreeSearchConfig
    from ..models.wildcard_query import WildcardQuery


T = TypeVar("T", bound="RetrievalQueryRequest")


@_attrs_define
class RetrievalQueryRequest:
    r"""A query in the retrieval pipeline. Extends QueryRequest with an optional
    tree search configuration. Each query specifies its own table.

    When both search fields (semantic_search, full_text_search) and tree_search
    are provided, the search results are used as start nodes for tree navigation.

        Attributes:
            table (str | Unset): Name of the table to query. Optional for global queries. Example: wikipedia.
            query (QueryRequestQuery | Unset): Canonical public query AST. Prefer this field for new clients.

                Boolean clauses are normalized before planning:
                - `bool.must` is scoring query input.
                - `bool.filter` is non-scoring query input.
                - `bool.must_not` is non-scoring exclusion query input.

                Filter branches accept the same query variants as `filter_query` and
                `exclusion_query`. Structured clauses use the native document-value
                path; text clauses are resolved through the text index before scoring.
                 Example: {'bool': {'must': [{'match': {'field': 'body', 'text': 'computer'}}], 'filter': [{'term': {'path':
                '/tenant', 'value': 'acme'}}], 'must_not': [{'exists': {'path': '/deleted_at'}}]}}.
            full_text_search (BooleanQuery | BoolFieldQuery | ConjunctionQuery | DateRangeStringQuery | DisjunctionQuery |
                DocIdQuery | FuzzyQuery | GeoBoundingBoxQuery | GeoBoundingPolygonQuery | GeoDistanceQuery | GeoShapeQuery |
                IPRangeQuery | MatchAllQuery | MatchNoneQuery | MatchPhraseQuery | MatchQuery | MultiMatchQuery |
                MultiPhraseQuery | NumericRangeQuery | PhraseQuery | PrefixQuery | QueryStringQuery | RegexpQuery | TermQuery |
                TermRangeQuery | Unset | WildcardQuery):
            semantic_search (str | Unset): Natural language query for vector similarity search. Results are ranked by
                semantic similarity
                to the query and can be combined with full_text_search using Reciprocal Rank Fusion (RRF).

                The semantic_search string is automatically embedded using the configured embedding model
                for the specified indexes. UTF-8 input is limited to 1 MiB. Use `embedding_template` for
                multimodal queries.
                 Example: artificial intelligence and machine learning applications.
            embedding_template (str | Unset): Optional Handlebars template for multimodal embedding of the semantic_search
                query.
                The template has access to `this` which contains the semantic_search string value.

                UTF-8 template input is limited to 64 KiB.

                Use this when you want to embed template-time multimodal content instead of
                just text. The template is rendered using dotprompt with access to remote content helpers.

                **Available Helpers**:
                - `remoteMedia url=<url>` - Fetches and embeds remote images/media
                - `remotePDF url=<url>` - **Deprecated.** Fetches and extracts text from born-digital PDFs
                - `remoteText url=<url>` - Fetches and includes remote text content

                Use a `document_extraction` asset producer when PDF pages and chunks must be persisted
                and reprocessed. `remoteMedia` and the other helpers only prepare template-time inference input.

                **Examples**:
                - Legacy PDF search: `{{remotePDF url=this}}`
                - Image search: `{{remoteMedia url=this}}`
                - Mixed: `Search for: {{this}} {{#if this}}{{remoteMedia url=this}}{{/if}}`

                When not specified, the semantic_search string is embedded as plain text.
                 Example: {{remoteMedia url=this}}.
            indexes (list[str] | Unset): List of vector index names to use for semantic search. Required when using
                semantic_search.
                Multiple indexes can be specified, and their results will be merged using RRF.
                 Example: ['title_body_nomic', 'description_embedding'].
            filter_prefix (str | Unset): Filter results by key prefix. Only returns documents whose keys start with this
                string.
                Applied before scoring to improve performance.

                Common use cases:
                - Multi-tenant filtering: `"tenant:acme:"`
                - User-specific data: `"user:123:"`
                - Document type filtering: `"article:"`
            filter_query (BooleanQuery | BoolFieldQuery | ConjunctionQuery | DateRangeStringQuery | DisjunctionQuery |
                DocIdQuery | FuzzyQuery | GeoBoundingBoxQuery | GeoBoundingPolygonQuery | GeoDistanceQuery | GeoShapeQuery |
                IPRangeQuery | MatchAllQuery | MatchNoneQuery | MatchPhraseQuery | MatchQuery | MultiMatchQuery |
                MultiPhraseQuery | NumericRangeQuery | PhraseQuery | PrefixQuery | QueryStringQuery | RegexpQuery | TermQuery |
                TermRangeQuery | Unset | WildcardQuery):
            exclusion_query (BooleanQuery | BoolFieldQuery | ConjunctionQuery | DateRangeStringQuery | DisjunctionQuery |
                DocIdQuery | FuzzyQuery | GeoBoundingBoxQuery | GeoBoundingPolygonQuery | GeoDistanceQuery | GeoShapeQuery |
                IPRangeQuery | MatchAllQuery | MatchNoneQuery | MatchPhraseQuery | MatchQuery | MultiMatchQuery |
                MultiPhraseQuery | NumericRangeQuery | PhraseQuery | PrefixQuery | QueryStringQuery | RegexpQuery | TermQuery |
                TermRangeQuery | Unset | WildcardQuery):
            aggregations (QueryRequestAggregations | Unset): Aggregation requests for computing metrics and bucketing
                results.
                Each key is a user-defined name for the aggregation, and the value specifies the aggregation configuration.

                When `hierarchy.group_by` is present, aggregations operate on the complete
                set of top-level grouped source or unit records. Nested `group_by.matches` are
                bounded evidence projections and are not counted as aggregation rows.

                Supports metric aggregations (sum, avg, min, max, count, stats, cardinality),
                bucketing aggregations (terms, range, date_range, histogram, date_histogram),
                geo aggregations (geohash_grid, geo_distance), and analytics (significant_terms).

                Example:
                ```json
                {
                  "price_stats": {
                    "type": "stats",
                    "field": "price"
                  },
                  "categories": {
                    "type": "terms",
                    "field": "category",
                    "size": 10
                  }
                }
                ```
            embeddings (QueryRequestEmbeddings | Unset): Pre-computed embeddings to use for semantic searches instead of
                embedding the semantic_search string.
                The keys are the index names. Values can be either:
                - **Dense (array)**: an array of floats, e.g. `[0.1, 0.2, 0.3]`
                - **Dense (packed)**: a base64 string of little-endian float32 bytes (~4x more compact)
                - **Sparse**: an object with `indices` (array of ints) and `values` (array of floats),
                  e.g. `{"indices": [1, 5, 100], "values": [0.3, 0.7, 0.1]}`
                - **Sparse (packed)**: an object with `packed_indices` (base64 uint32 LE) and `packed_values` (base64 float32
                LE)

                Use when you've already generated embeddings on the client side to avoid redundant embedding calls.
            search_effort (float | Unset): Controls the vector search recall/latency tradeoff for semantic searches.

                - `0.0` = fastest, lowest recall
                - `0.5` = balanced default
                - `1.0` = highest recall

                When omitted, Antfly uses the balanced default effort (`0.5`) unless
                lower-level vector search overrides are provided internally.
                 Default: 0.5. Example: 0.5.
            fields (list[str] | Unset): List of fields to include in the results. If not specified, all fields are returned.
                Use to reduce response size and improve performance. This field is required when
                hierarchy.group_by is present so a grouped query cannot accidentally hydrate an
                entire grouped document. Use an empty array for identity-only groups.
                This projection is also required for hierarchy.children traversal.
                 Example: ['title', 'url', 'summary', 'created_at'].
            hierarchy (QueryHierarchy | Unset): Returns direct index matches with optional projected ancestor context, or
                groups
                those matches at a hierarchy level through `group_by`. A group's nested `matches`
                projection is independently bounded and defaults to three hits while the top-level
                `limit` continues to control the number of groups.

                `children` is a separate sequential-browsing operation. It enumerates every unit
                in the selected source revision, including units with no searchable chunk, and uses
                the top-level `_sort`/`search_after` cursor contract.

                Ancestor and nested-match field projections are always explicit to keep response
                size predictable. The presence of this object selects the canonical contract:
                without `group_by` or `children`, including when the object is empty, direct index
                matches are returned. `ancestors` only controls projected context and never changes result
                cardinality. Omit `hierarchy` entirely to retain the legacy default result shape.
            limit (int | Unset): Maximum number of top-level results to return. For semantic_search, this is the topk
                parameter.
                This does not limit nested matches attached through hierarchy.group_by.matches;
                use hierarchy.group_by.matches.limit for that. Default varies by query type (typically 10).
                Queries using hierarchy.group_by.matches are limited to 100 top-level groups
                and a groups-times-matches execution budget of 1,000.
                 Example: 20.
            offset (int | Unset): Number of results to skip for pagination. Supported for text-backed,
                match_all, and filter-only requests. Not supported for semantic_search
                due to vector index limitations.
            timeout_ms (int | Unset): Optional query execution deadline in milliseconds. The server applies this as a
                cooperative deadline across query planning, search execution, aggregation reruns,
                sorting, and response post-processing. If the deadline expires before the query
                completes, the HTTP API returns 504. When omitted, semantic query embedding planning
                and provider I/O use a 30-second default deadline.
                 Example: 5000.
            order_by (list[SortField] | Unset): Sort order for results. Array of sort fields with direction.
                Antfly appends `_id` ascending as a stable tie-breaker when it is omitted.
                Hierarchy child traversal requires `_hierarchy.position` ascending; its opaque,
                sortable value is bound to the complete source hierarchy revision.
                Supported for exact text-backed, match_all, and filter-only requests
                when each non-`_id` field is a mapped exact scalar field with sortable
                native doc-value coverage. Sortable mapping types are keyword,
                numeric/number/integer, boolean/bool, datetime/date/timestamp, and
                link. Declare the field with `x-antfly-field` and `sortable: true`;
                `x-antfly-types` shorthand declarations alone are not sortable.
                Analyzed `text` fields and `search_as_you_type`, geo, embedding,
                blob, html, object, and array fields are not directly sortable; sort
                on an exact scalar mapping such as `title.keyword` instead. Requests
                that cannot be executed through an exact native sort path return 422
                rather than falling back to stored JSON sorting. Semantic searches
                are always sorted by similarity score. Not supported when `count` is true.
                 Example: [{'field': 'created_at', 'desc': True}, {'field': 'score', 'desc': True}].
            search_after (list[Any] | Unset): Cursor for forward pagination. Pass the `_sort` values from the last hit
                of the previous page exactly, including the appended `_id` tie-breaker.
                Values preserve their JSON types; for example numbers remain numbers,
                booleans remain booleans, and strings remain strings. Cursor values
                must be replayable JSON scalars; nulls, arrays, objects, and non-finite
                numbers are rejected.
                Mutually exclusive with `offset`.
                When `order_by` is omitted, Antfly uses `_id` ascending as the effective
                order and the cursor tuple must contain exactly one `_id` string.
                Supported for exact text-backed, match_all, and filter-only requests;
                not supported for semantic_search or count-only requests.
                For hierarchy child traversal, a cursor whose source-artifact revision
                changed returns `409 hierarchy_cursor_stale`; restart the same traversal
                without `search_after` rather than retrying the stale tuple.
            search_before (list[Any] | Unset): Cursor for backward pagination. Pass the `_sort` values from the first hit
                of the current page exactly, including the appended `_id` tie-breaker.
                Values preserve their JSON types; for example numbers remain numbers,
                booleans remain booleans, and strings remain strings. Cursor values
                must be replayable JSON scalars; nulls, arrays, objects, and non-finite
                numbers are rejected.
                Mutually exclusive with `offset`.
                When `order_by` is omitted, Antfly uses `_id` ascending as the effective
                order and the cursor tuple must contain exactly one `_id` string.
                Supported for exact text-backed, match_all, and filter-only requests;
                not supported for semantic_search or count-only requests.
            distance_under (float | Unset): Maximum distance threshold for semantic similarity search. Results with distance
                greater than this value are excluded. Lower distances indicate higher similarity.

                Useful for filtering out low-confidence matches.
                 Example: 0.5.
            distance_over (float | Unset): Minimum distance threshold for semantic similarity search. Results with distance
                less than this value are excluded.

                Useful for excluding near-exact duplicates or finding dissimilar documents.
                 Example: 0.1.
            merge_config (MergeConfig | Unset): Configuration for result fusion when combining multiple search indexes.
            count (bool | Unset): If true, returns only the total count of matching documents without retrieving the actual
                documents.
                Useful for pagination and displaying result counts. Count-only requests
                do not return an ordered result page, so `order_by`, `search_after`,
                and `search_before` are not supported when this is true.
            profile (bool | Unset): If true, includes detailed execution profiling in the response.
                Adds a `profile` object with per-phase timing breakdowns, shard statistics,
                join metadata, reranker stats, and merge details.
                Has minor performance overhead — not recommended for production traffic.
            reranker (RerankerConfig | Unset): A unified configuration for a reranking provider. Example: {'provider':
                'ollama', 'model': 'dengcao/Qwen3-Reranker-0.6B:F16', 'field': 'content'}.
            analyses (Analyses | Unset):
            graph_queries (QueryRequestGraphQueries | Unset): Declarative graph matching, traversal, and path queries. A
                nested node
                `filter` uses the same canonical query DSL as document search. A request
                may contain at most 64 named graph operations; operation names must be
                1-128 characters.
            graph_searches (QueryRequestGraphSearches | Unset): Deprecated compatibility alias for the v0.2 graph query
                contract.
                Use `graph_queries`; requests containing both fields are rejected.
            expand_strategy (QueryRequestExpandStrategy | Unset): Strategy for merging graph results with search results:
                - union: Include nodes from both search and graph results
                - intersection: Only include nodes appearing in both
            document_renderer (str | Unset): Optional Handlebars template string for rendering document content in RAG
                queries.
                Template has access to document fields via `{{this.fields.fieldName}}`.

                **Default**: Uses TOON (Token-Oriented Object Notation) format for 30-60% token reduction:
                ```handlebars
                {{encodeToon this.fields}}
                ```

                **Available Helpers**:
                - `encodeToon` - Renders fields in compact TOON format with configurable options:
                  - `lengthMarker` (bool): Add # prefix to array counts (default: true)
                  - `indent` (int): Indentation spacing (default: 2)
                  - `delimiter` (string): Field separator for tabular arrays
                - `scrubHtml` - Removes HTML tags and extracts text
                - `media` - Wraps data URIs for GenKit multimodal support
                - `eq` - Equality comparison for conditionals

                **Examples**:
                - Basic TOON: `{{encodeToon this.fields}}`
                - Compact TOON: `{{encodeToon this.fields lengthMarker=false indent=0}}`
                - Tabular data: `{{encodeToon this.fields delimiter="\t"}}`
                - Custom template: `Title: {{this.fields.title}}\nBody: {{this.fields.body}}`
                - Traditional format: `{{#each this.fields}}{{@key}}: {{this}}\n{{/each}}`

                TOON format produces compact, LLM-optimized output like:
                ```
                title: Introduction to Vector Search
                author: Jane Doe
                tags[#3]: ai,search,ml
                ```

                **References**:
                - TOON Specification: https://github.com/toon-format/toon
                - Go Implementation: https://github.com/alpkeskin/gotoon
                 Example: {{encodeToon this.fields}}.
            pruner (Pruner | Unset): Configuration for pruning search results based on score quality.
                Helps filter out low-relevance results in RAG pipelines by detecting
                score gaps or deviations from top results.
            join (JoinClause | Unset): Configuration for joining data from another table.
                Supports inner, left, and right joins with automatic strategy selection.
            foreign_sources (QueryRequestForeignSources | Unset): Map of table name to foreign data source configuration for
                query-time federated access.
                When a table name referenced in this query (or in a join's `right_table`) appears as a key
                here, the query is routed to the external database instead of Antfly shards.

                This enables joining Antfly search results with structured relational data (customer records,
                product catalogs, etc.) without ingesting that data into Antfly.

                **Supported operations on foreign tables:** filter_query, field selection, limit/offset.
                **Not supported:** full_text_search, semantic_search, graph_queries, aggregations, reranker.

                **Example - Join Antfly products with Postgres customers:**
                ```json
                {
                  "table": "products",
                  "full_text_search": {"query": "category:electronics"},
                  "join": {
                    "right_table": "pg_customers",
                    "on": {"left_field": "customer_id", "right_field": "id"}
                  },
                  "foreign_sources": {
                    "pg_customers": {
                      "type": "postgres",
                      "dsn": "${secret:pg_dsn}",
                      "postgres_table": "customers"
                    }
                  }
                }
                ```
            tree_search (TreeSearchConfig | Unset): Configuration for tree search strategy. Tree search navigates
                hierarchical
                document structures by evaluating summaries at each level.
    """

    table: str | Unset = UNSET
    query: QueryRequestQuery | Unset = UNSET
    full_text_search: (
        BooleanQuery
        | BoolFieldQuery
        | ConjunctionQuery
        | DateRangeStringQuery
        | DisjunctionQuery
        | DocIdQuery
        | FuzzyQuery
        | GeoBoundingBoxQuery
        | GeoBoundingPolygonQuery
        | GeoDistanceQuery
        | GeoShapeQuery
        | IPRangeQuery
        | MatchAllQuery
        | MatchNoneQuery
        | MatchPhraseQuery
        | MatchQuery
        | MultiMatchQuery
        | MultiPhraseQuery
        | NumericRangeQuery
        | PhraseQuery
        | PrefixQuery
        | QueryStringQuery
        | RegexpQuery
        | TermQuery
        | TermRangeQuery
        | Unset
        | WildcardQuery
    ) = UNSET
    semantic_search: str | Unset = UNSET
    embedding_template: str | Unset = UNSET
    indexes: list[str] | Unset = UNSET
    filter_prefix: str | Unset = UNSET
    filter_query: (
        BooleanQuery
        | BoolFieldQuery
        | ConjunctionQuery
        | DateRangeStringQuery
        | DisjunctionQuery
        | DocIdQuery
        | FuzzyQuery
        | GeoBoundingBoxQuery
        | GeoBoundingPolygonQuery
        | GeoDistanceQuery
        | GeoShapeQuery
        | IPRangeQuery
        | MatchAllQuery
        | MatchNoneQuery
        | MatchPhraseQuery
        | MatchQuery
        | MultiMatchQuery
        | MultiPhraseQuery
        | NumericRangeQuery
        | PhraseQuery
        | PrefixQuery
        | QueryStringQuery
        | RegexpQuery
        | TermQuery
        | TermRangeQuery
        | Unset
        | WildcardQuery
    ) = UNSET
    exclusion_query: (
        BooleanQuery
        | BoolFieldQuery
        | ConjunctionQuery
        | DateRangeStringQuery
        | DisjunctionQuery
        | DocIdQuery
        | FuzzyQuery
        | GeoBoundingBoxQuery
        | GeoBoundingPolygonQuery
        | GeoDistanceQuery
        | GeoShapeQuery
        | IPRangeQuery
        | MatchAllQuery
        | MatchNoneQuery
        | MatchPhraseQuery
        | MatchQuery
        | MultiMatchQuery
        | MultiPhraseQuery
        | NumericRangeQuery
        | PhraseQuery
        | PrefixQuery
        | QueryStringQuery
        | RegexpQuery
        | TermQuery
        | TermRangeQuery
        | Unset
        | WildcardQuery
    ) = UNSET
    aggregations: QueryRequestAggregations | Unset = UNSET
    embeddings: QueryRequestEmbeddings | Unset = UNSET
    search_effort: float | Unset = 0.5
    fields: list[str] | Unset = UNSET
    hierarchy: QueryHierarchy | Unset = UNSET
    limit: int | Unset = UNSET
    offset: int | Unset = UNSET
    timeout_ms: int | Unset = UNSET
    order_by: list[SortField] | Unset = UNSET
    search_after: list[Any] | Unset = UNSET
    search_before: list[Any] | Unset = UNSET
    distance_under: float | Unset = UNSET
    distance_over: float | Unset = UNSET
    merge_config: MergeConfig | Unset = UNSET
    count: bool | Unset = UNSET
    profile: bool | Unset = UNSET
    reranker: RerankerConfig | Unset = UNSET
    analyses: Analyses | Unset = UNSET
    graph_queries: QueryRequestGraphQueries | Unset = UNSET
    graph_searches: QueryRequestGraphSearches | Unset = UNSET
    expand_strategy: QueryRequestExpandStrategy | Unset = UNSET
    document_renderer: str | Unset = UNSET
    pruner: Pruner | Unset = UNSET
    join: JoinClause | Unset = UNSET
    foreign_sources: QueryRequestForeignSources | Unset = UNSET
    tree_search: TreeSearchConfig | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.bool_field_query import BoolFieldQuery
        from ..models.boolean_query import BooleanQuery
        from ..models.conjunction_query import ConjunctionQuery
        from ..models.date_range_string_query import DateRangeStringQuery
        from ..models.disjunction_query import DisjunctionQuery
        from ..models.doc_id_query import DocIdQuery
        from ..models.fuzzy_query import FuzzyQuery
        from ..models.geo_bounding_box_query import GeoBoundingBoxQuery
        from ..models.geo_bounding_polygon_query import GeoBoundingPolygonQuery
        from ..models.geo_distance_query import GeoDistanceQuery
        from ..models.ip_range_query import IPRangeQuery
        from ..models.match_all_query import MatchAllQuery
        from ..models.match_none_query import MatchNoneQuery
        from ..models.match_phrase_query import MatchPhraseQuery
        from ..models.match_query import MatchQuery
        from ..models.multi_match_query import MultiMatchQuery
        from ..models.multi_phrase_query import MultiPhraseQuery
        from ..models.numeric_range_query import NumericRangeQuery
        from ..models.phrase_query import PhraseQuery
        from ..models.prefix_query import PrefixQuery
        from ..models.query_string_query import QueryStringQuery
        from ..models.regexp_query import RegexpQuery
        from ..models.term_query import TermQuery
        from ..models.term_range_query import TermRangeQuery
        from ..models.wildcard_query import WildcardQuery

        table = self.table

        query: dict[str, Any] | Unset = UNSET
        if not isinstance(self.query, Unset):
            query = self.query.to_dict()

        full_text_search: dict[str, Any] | Unset
        if isinstance(self.full_text_search, Unset):
            full_text_search = UNSET
        elif isinstance(self.full_text_search, TermQuery):
            full_text_search = self.full_text_search.to_dict()
        elif isinstance(self.full_text_search, MatchQuery):
            full_text_search = self.full_text_search.to_dict()
        elif isinstance(self.full_text_search, MultiMatchQuery):
            full_text_search = self.full_text_search.to_dict()
        elif isinstance(self.full_text_search, MatchPhraseQuery):
            full_text_search = self.full_text_search.to_dict()
        elif isinstance(self.full_text_search, PhraseQuery):
            full_text_search = self.full_text_search.to_dict()
        elif isinstance(self.full_text_search, MultiPhraseQuery):
            full_text_search = self.full_text_search.to_dict()
        elif isinstance(self.full_text_search, FuzzyQuery):
            full_text_search = self.full_text_search.to_dict()
        elif isinstance(self.full_text_search, PrefixQuery):
            full_text_search = self.full_text_search.to_dict()
        elif isinstance(self.full_text_search, RegexpQuery):
            full_text_search = self.full_text_search.to_dict()
        elif isinstance(self.full_text_search, WildcardQuery):
            full_text_search = self.full_text_search.to_dict()
        elif isinstance(self.full_text_search, QueryStringQuery):
            full_text_search = self.full_text_search.to_dict()
        elif isinstance(self.full_text_search, NumericRangeQuery):
            full_text_search = self.full_text_search.to_dict()
        elif isinstance(self.full_text_search, TermRangeQuery):
            full_text_search = self.full_text_search.to_dict()
        elif isinstance(self.full_text_search, DateRangeStringQuery):
            full_text_search = self.full_text_search.to_dict()
        elif isinstance(self.full_text_search, BooleanQuery):
            full_text_search = self.full_text_search.to_dict()
        elif isinstance(self.full_text_search, ConjunctionQuery):
            full_text_search = self.full_text_search.to_dict()
        elif isinstance(self.full_text_search, DisjunctionQuery):
            full_text_search = self.full_text_search.to_dict()
        elif isinstance(self.full_text_search, MatchAllQuery):
            full_text_search = self.full_text_search.to_dict()
        elif isinstance(self.full_text_search, MatchNoneQuery):
            full_text_search = self.full_text_search.to_dict()
        elif isinstance(self.full_text_search, DocIdQuery):
            full_text_search = self.full_text_search.to_dict()
        elif isinstance(self.full_text_search, BoolFieldQuery):
            full_text_search = self.full_text_search.to_dict()
        elif isinstance(self.full_text_search, IPRangeQuery):
            full_text_search = self.full_text_search.to_dict()
        elif isinstance(self.full_text_search, GeoBoundingBoxQuery):
            full_text_search = self.full_text_search.to_dict()
        elif isinstance(self.full_text_search, GeoDistanceQuery):
            full_text_search = self.full_text_search.to_dict()
        elif isinstance(self.full_text_search, GeoBoundingPolygonQuery):
            full_text_search = self.full_text_search.to_dict()
        else:
            full_text_search = self.full_text_search.to_dict()

        semantic_search = self.semantic_search

        embedding_template = self.embedding_template

        indexes: list[str] | Unset = UNSET
        if not isinstance(self.indexes, Unset):
            indexes = self.indexes

        filter_prefix = self.filter_prefix

        filter_query: dict[str, Any] | Unset
        if isinstance(self.filter_query, Unset):
            filter_query = UNSET
        elif isinstance(self.filter_query, TermQuery):
            filter_query = self.filter_query.to_dict()
        elif isinstance(self.filter_query, MatchQuery):
            filter_query = self.filter_query.to_dict()
        elif isinstance(self.filter_query, MultiMatchQuery):
            filter_query = self.filter_query.to_dict()
        elif isinstance(self.filter_query, MatchPhraseQuery):
            filter_query = self.filter_query.to_dict()
        elif isinstance(self.filter_query, PhraseQuery):
            filter_query = self.filter_query.to_dict()
        elif isinstance(self.filter_query, MultiPhraseQuery):
            filter_query = self.filter_query.to_dict()
        elif isinstance(self.filter_query, FuzzyQuery):
            filter_query = self.filter_query.to_dict()
        elif isinstance(self.filter_query, PrefixQuery):
            filter_query = self.filter_query.to_dict()
        elif isinstance(self.filter_query, RegexpQuery):
            filter_query = self.filter_query.to_dict()
        elif isinstance(self.filter_query, WildcardQuery):
            filter_query = self.filter_query.to_dict()
        elif isinstance(self.filter_query, QueryStringQuery):
            filter_query = self.filter_query.to_dict()
        elif isinstance(self.filter_query, NumericRangeQuery):
            filter_query = self.filter_query.to_dict()
        elif isinstance(self.filter_query, TermRangeQuery):
            filter_query = self.filter_query.to_dict()
        elif isinstance(self.filter_query, DateRangeStringQuery):
            filter_query = self.filter_query.to_dict()
        elif isinstance(self.filter_query, BooleanQuery):
            filter_query = self.filter_query.to_dict()
        elif isinstance(self.filter_query, ConjunctionQuery):
            filter_query = self.filter_query.to_dict()
        elif isinstance(self.filter_query, DisjunctionQuery):
            filter_query = self.filter_query.to_dict()
        elif isinstance(self.filter_query, MatchAllQuery):
            filter_query = self.filter_query.to_dict()
        elif isinstance(self.filter_query, MatchNoneQuery):
            filter_query = self.filter_query.to_dict()
        elif isinstance(self.filter_query, DocIdQuery):
            filter_query = self.filter_query.to_dict()
        elif isinstance(self.filter_query, BoolFieldQuery):
            filter_query = self.filter_query.to_dict()
        elif isinstance(self.filter_query, IPRangeQuery):
            filter_query = self.filter_query.to_dict()
        elif isinstance(self.filter_query, GeoBoundingBoxQuery):
            filter_query = self.filter_query.to_dict()
        elif isinstance(self.filter_query, GeoDistanceQuery):
            filter_query = self.filter_query.to_dict()
        elif isinstance(self.filter_query, GeoBoundingPolygonQuery):
            filter_query = self.filter_query.to_dict()
        else:
            filter_query = self.filter_query.to_dict()

        exclusion_query: dict[str, Any] | Unset
        if isinstance(self.exclusion_query, Unset):
            exclusion_query = UNSET
        elif isinstance(self.exclusion_query, TermQuery):
            exclusion_query = self.exclusion_query.to_dict()
        elif isinstance(self.exclusion_query, MatchQuery):
            exclusion_query = self.exclusion_query.to_dict()
        elif isinstance(self.exclusion_query, MultiMatchQuery):
            exclusion_query = self.exclusion_query.to_dict()
        elif isinstance(self.exclusion_query, MatchPhraseQuery):
            exclusion_query = self.exclusion_query.to_dict()
        elif isinstance(self.exclusion_query, PhraseQuery):
            exclusion_query = self.exclusion_query.to_dict()
        elif isinstance(self.exclusion_query, MultiPhraseQuery):
            exclusion_query = self.exclusion_query.to_dict()
        elif isinstance(self.exclusion_query, FuzzyQuery):
            exclusion_query = self.exclusion_query.to_dict()
        elif isinstance(self.exclusion_query, PrefixQuery):
            exclusion_query = self.exclusion_query.to_dict()
        elif isinstance(self.exclusion_query, RegexpQuery):
            exclusion_query = self.exclusion_query.to_dict()
        elif isinstance(self.exclusion_query, WildcardQuery):
            exclusion_query = self.exclusion_query.to_dict()
        elif isinstance(self.exclusion_query, QueryStringQuery):
            exclusion_query = self.exclusion_query.to_dict()
        elif isinstance(self.exclusion_query, NumericRangeQuery):
            exclusion_query = self.exclusion_query.to_dict()
        elif isinstance(self.exclusion_query, TermRangeQuery):
            exclusion_query = self.exclusion_query.to_dict()
        elif isinstance(self.exclusion_query, DateRangeStringQuery):
            exclusion_query = self.exclusion_query.to_dict()
        elif isinstance(self.exclusion_query, BooleanQuery):
            exclusion_query = self.exclusion_query.to_dict()
        elif isinstance(self.exclusion_query, ConjunctionQuery):
            exclusion_query = self.exclusion_query.to_dict()
        elif isinstance(self.exclusion_query, DisjunctionQuery):
            exclusion_query = self.exclusion_query.to_dict()
        elif isinstance(self.exclusion_query, MatchAllQuery):
            exclusion_query = self.exclusion_query.to_dict()
        elif isinstance(self.exclusion_query, MatchNoneQuery):
            exclusion_query = self.exclusion_query.to_dict()
        elif isinstance(self.exclusion_query, DocIdQuery):
            exclusion_query = self.exclusion_query.to_dict()
        elif isinstance(self.exclusion_query, BoolFieldQuery):
            exclusion_query = self.exclusion_query.to_dict()
        elif isinstance(self.exclusion_query, IPRangeQuery):
            exclusion_query = self.exclusion_query.to_dict()
        elif isinstance(self.exclusion_query, GeoBoundingBoxQuery):
            exclusion_query = self.exclusion_query.to_dict()
        elif isinstance(self.exclusion_query, GeoDistanceQuery):
            exclusion_query = self.exclusion_query.to_dict()
        elif isinstance(self.exclusion_query, GeoBoundingPolygonQuery):
            exclusion_query = self.exclusion_query.to_dict()
        else:
            exclusion_query = self.exclusion_query.to_dict()

        aggregations: dict[str, Any] | Unset = UNSET
        if not isinstance(self.aggregations, Unset):
            aggregations = self.aggregations.to_dict()

        embeddings: dict[str, Any] | Unset = UNSET
        if not isinstance(self.embeddings, Unset):
            embeddings = self.embeddings.to_dict()

        search_effort = self.search_effort

        fields: list[str] | Unset = UNSET
        if not isinstance(self.fields, Unset):
            fields = self.fields

        hierarchy: dict[str, Any] | Unset = UNSET
        if not isinstance(self.hierarchy, Unset):
            hierarchy = self.hierarchy.to_dict()

        limit = self.limit

        offset = self.offset

        timeout_ms = self.timeout_ms

        order_by: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.order_by, Unset):
            order_by = []
            for order_by_item_data in self.order_by:
                order_by_item = order_by_item_data.to_dict()
                order_by.append(order_by_item)

        search_after: list[Any] | Unset = UNSET
        if not isinstance(self.search_after, Unset):
            search_after = self.search_after

        search_before: list[Any] | Unset = UNSET
        if not isinstance(self.search_before, Unset):
            search_before = self.search_before

        distance_under = self.distance_under

        distance_over = self.distance_over

        merge_config: dict[str, Any] | Unset = UNSET
        if not isinstance(self.merge_config, Unset):
            merge_config = self.merge_config.to_dict()

        count = self.count

        profile = self.profile

        reranker: dict[str, Any] | Unset = UNSET
        if not isinstance(self.reranker, Unset):
            reranker = self.reranker.to_dict()

        analyses: dict[str, Any] | Unset = UNSET
        if not isinstance(self.analyses, Unset):
            analyses = self.analyses.to_dict()

        graph_queries: dict[str, Any] | Unset = UNSET
        if not isinstance(self.graph_queries, Unset):
            graph_queries = self.graph_queries.to_dict()

        graph_searches: dict[str, Any] | Unset = UNSET
        if not isinstance(self.graph_searches, Unset):
            graph_searches = self.graph_searches.to_dict()

        expand_strategy: str | Unset = UNSET
        if not isinstance(self.expand_strategy, Unset):
            expand_strategy = self.expand_strategy.value

        document_renderer = self.document_renderer

        pruner: dict[str, Any] | Unset = UNSET
        if not isinstance(self.pruner, Unset):
            pruner = self.pruner.to_dict()

        join: dict[str, Any] | Unset = UNSET
        if not isinstance(self.join, Unset):
            join = self.join.to_dict()

        foreign_sources: dict[str, Any] | Unset = UNSET
        if not isinstance(self.foreign_sources, Unset):
            foreign_sources = self.foreign_sources.to_dict()

        tree_search: dict[str, Any] | Unset = UNSET
        if not isinstance(self.tree_search, Unset):
            tree_search = self.tree_search.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if table is not UNSET:
            field_dict["table"] = table
        if query is not UNSET:
            field_dict["query"] = query
        if full_text_search is not UNSET:
            field_dict["full_text_search"] = full_text_search
        if semantic_search is not UNSET:
            field_dict["semantic_search"] = semantic_search
        if embedding_template is not UNSET:
            field_dict["embedding_template"] = embedding_template
        if indexes is not UNSET:
            field_dict["indexes"] = indexes
        if filter_prefix is not UNSET:
            field_dict["filter_prefix"] = filter_prefix
        if filter_query is not UNSET:
            field_dict["filter_query"] = filter_query
        if exclusion_query is not UNSET:
            field_dict["exclusion_query"] = exclusion_query
        if aggregations is not UNSET:
            field_dict["aggregations"] = aggregations
        if embeddings is not UNSET:
            field_dict["embeddings"] = embeddings
        if search_effort is not UNSET:
            field_dict["search_effort"] = search_effort
        if fields is not UNSET:
            field_dict["fields"] = fields
        if hierarchy is not UNSET:
            field_dict["hierarchy"] = hierarchy
        if limit is not UNSET:
            field_dict["limit"] = limit
        if offset is not UNSET:
            field_dict["offset"] = offset
        if timeout_ms is not UNSET:
            field_dict["timeout_ms"] = timeout_ms
        if order_by is not UNSET:
            field_dict["order_by"] = order_by
        if search_after is not UNSET:
            field_dict["search_after"] = search_after
        if search_before is not UNSET:
            field_dict["search_before"] = search_before
        if distance_under is not UNSET:
            field_dict["distance_under"] = distance_under
        if distance_over is not UNSET:
            field_dict["distance_over"] = distance_over
        if merge_config is not UNSET:
            field_dict["merge_config"] = merge_config
        if count is not UNSET:
            field_dict["count"] = count
        if profile is not UNSET:
            field_dict["profile"] = profile
        if reranker is not UNSET:
            field_dict["reranker"] = reranker
        if analyses is not UNSET:
            field_dict["analyses"] = analyses
        if graph_queries is not UNSET:
            field_dict["graph_queries"] = graph_queries
        if graph_searches is not UNSET:
            field_dict["graph_searches"] = graph_searches
        if expand_strategy is not UNSET:
            field_dict["expand_strategy"] = expand_strategy
        if document_renderer is not UNSET:
            field_dict["document_renderer"] = document_renderer
        if pruner is not UNSET:
            field_dict["pruner"] = pruner
        if join is not UNSET:
            field_dict["join"] = join
        if foreign_sources is not UNSET:
            field_dict["foreign_sources"] = foreign_sources
        if tree_search is not UNSET:
            field_dict["tree_search"] = tree_search

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.analyses import Analyses
        from ..models.bool_field_query import BoolFieldQuery
        from ..models.boolean_query import BooleanQuery
        from ..models.conjunction_query import ConjunctionQuery
        from ..models.date_range_string_query import DateRangeStringQuery
        from ..models.disjunction_query import DisjunctionQuery
        from ..models.doc_id_query import DocIdQuery
        from ..models.fuzzy_query import FuzzyQuery
        from ..models.geo_bounding_box_query import GeoBoundingBoxQuery
        from ..models.geo_bounding_polygon_query import GeoBoundingPolygonQuery
        from ..models.geo_distance_query import GeoDistanceQuery
        from ..models.geo_shape_query import GeoShapeQuery
        from ..models.ip_range_query import IPRangeQuery
        from ..models.join_clause import JoinClause
        from ..models.match_all_query import MatchAllQuery
        from ..models.match_none_query import MatchNoneQuery
        from ..models.match_phrase_query import MatchPhraseQuery
        from ..models.match_query import MatchQuery
        from ..models.merge_config import MergeConfig
        from ..models.multi_match_query import MultiMatchQuery
        from ..models.multi_phrase_query import MultiPhraseQuery
        from ..models.numeric_range_query import NumericRangeQuery
        from ..models.phrase_query import PhraseQuery
        from ..models.prefix_query import PrefixQuery
        from ..models.pruner import Pruner
        from ..models.query_hierarchy import QueryHierarchy
        from ..models.query_request_aggregations import QueryRequestAggregations
        from ..models.query_request_embeddings import QueryRequestEmbeddings
        from ..models.query_request_foreign_sources import QueryRequestForeignSources
        from ..models.query_request_graph_queries import QueryRequestGraphQueries
        from ..models.query_request_graph_searches import QueryRequestGraphSearches
        from ..models.query_request_query import QueryRequestQuery
        from ..models.query_string_query import QueryStringQuery
        from ..models.regexp_query import RegexpQuery
        from ..models.reranker_config import RerankerConfig
        from ..models.sort_field import SortField
        from ..models.term_query import TermQuery
        from ..models.term_range_query import TermRangeQuery
        from ..models.tree_search_config import TreeSearchConfig
        from ..models.wildcard_query import WildcardQuery

        d = dict(src_dict)
        table = d.pop("table", UNSET)

        _query = d.pop("query", UNSET)
        query: QueryRequestQuery | Unset
        if isinstance(_query, Unset):
            query = UNSET
        else:
            query = QueryRequestQuery.from_dict(_query)

        def _parse_full_text_search(
            data: object,
        ) -> (
            BooleanQuery
            | BoolFieldQuery
            | ConjunctionQuery
            | DateRangeStringQuery
            | DisjunctionQuery
            | DocIdQuery
            | FuzzyQuery
            | GeoBoundingBoxQuery
            | GeoBoundingPolygonQuery
            | GeoDistanceQuery
            | GeoShapeQuery
            | IPRangeQuery
            | MatchAllQuery
            | MatchNoneQuery
            | MatchPhraseQuery
            | MatchQuery
            | MultiMatchQuery
            | MultiPhraseQuery
            | NumericRangeQuery
            | PhraseQuery
            | PrefixQuery
            | QueryStringQuery
            | RegexpQuery
            | TermQuery
            | TermRangeQuery
            | Unset
            | WildcardQuery
        ):
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_0 = TermQuery.from_dict(data)

                return componentsschemas_query_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_1 = MatchQuery.from_dict(data)

                return componentsschemas_query_type_1
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_2 = MultiMatchQuery.from_dict(data)

                return componentsschemas_query_type_2
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_3 = MatchPhraseQuery.from_dict(data)

                return componentsschemas_query_type_3
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_4 = PhraseQuery.from_dict(data)

                return componentsschemas_query_type_4
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_5 = MultiPhraseQuery.from_dict(data)

                return componentsschemas_query_type_5
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_6 = FuzzyQuery.from_dict(data)

                return componentsschemas_query_type_6
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_7 = PrefixQuery.from_dict(data)

                return componentsschemas_query_type_7
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_8 = RegexpQuery.from_dict(data)

                return componentsschemas_query_type_8
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_9 = WildcardQuery.from_dict(data)

                return componentsschemas_query_type_9
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_10 = QueryStringQuery.from_dict(data)

                return componentsschemas_query_type_10
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_11 = NumericRangeQuery.from_dict(data)

                return componentsschemas_query_type_11
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_12 = TermRangeQuery.from_dict(data)

                return componentsschemas_query_type_12
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_13 = DateRangeStringQuery.from_dict(data)

                return componentsschemas_query_type_13
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_14 = BooleanQuery.from_dict(data)

                return componentsschemas_query_type_14
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_15 = ConjunctionQuery.from_dict(data)

                return componentsschemas_query_type_15
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_16 = DisjunctionQuery.from_dict(data)

                return componentsschemas_query_type_16
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_17 = MatchAllQuery.from_dict(data)

                return componentsschemas_query_type_17
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_18 = MatchNoneQuery.from_dict(data)

                return componentsschemas_query_type_18
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_19 = DocIdQuery.from_dict(data)

                return componentsschemas_query_type_19
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_20 = BoolFieldQuery.from_dict(data)

                return componentsschemas_query_type_20
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_21 = IPRangeQuery.from_dict(data)

                return componentsschemas_query_type_21
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_22 = GeoBoundingBoxQuery.from_dict(data)

                return componentsschemas_query_type_22
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_23 = GeoDistanceQuery.from_dict(data)

                return componentsschemas_query_type_23
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_24 = GeoBoundingPolygonQuery.from_dict(data)

                return componentsschemas_query_type_24
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            if not isinstance(data, dict):
                raise TypeError()
            componentsschemas_query_type_25 = GeoShapeQuery.from_dict(data)

            return componentsschemas_query_type_25

        full_text_search = _parse_full_text_search(d.pop("full_text_search", UNSET))

        semantic_search = d.pop("semantic_search", UNSET)

        embedding_template = d.pop("embedding_template", UNSET)

        indexes = cast(list[str], d.pop("indexes", UNSET))

        filter_prefix = d.pop("filter_prefix", UNSET)

        def _parse_filter_query(
            data: object,
        ) -> (
            BooleanQuery
            | BoolFieldQuery
            | ConjunctionQuery
            | DateRangeStringQuery
            | DisjunctionQuery
            | DocIdQuery
            | FuzzyQuery
            | GeoBoundingBoxQuery
            | GeoBoundingPolygonQuery
            | GeoDistanceQuery
            | GeoShapeQuery
            | IPRangeQuery
            | MatchAllQuery
            | MatchNoneQuery
            | MatchPhraseQuery
            | MatchQuery
            | MultiMatchQuery
            | MultiPhraseQuery
            | NumericRangeQuery
            | PhraseQuery
            | PrefixQuery
            | QueryStringQuery
            | RegexpQuery
            | TermQuery
            | TermRangeQuery
            | Unset
            | WildcardQuery
        ):
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_0 = TermQuery.from_dict(data)

                return componentsschemas_query_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_1 = MatchQuery.from_dict(data)

                return componentsschemas_query_type_1
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_2 = MultiMatchQuery.from_dict(data)

                return componentsschemas_query_type_2
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_3 = MatchPhraseQuery.from_dict(data)

                return componentsschemas_query_type_3
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_4 = PhraseQuery.from_dict(data)

                return componentsschemas_query_type_4
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_5 = MultiPhraseQuery.from_dict(data)

                return componentsschemas_query_type_5
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_6 = FuzzyQuery.from_dict(data)

                return componentsschemas_query_type_6
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_7 = PrefixQuery.from_dict(data)

                return componentsschemas_query_type_7
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_8 = RegexpQuery.from_dict(data)

                return componentsschemas_query_type_8
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_9 = WildcardQuery.from_dict(data)

                return componentsschemas_query_type_9
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_10 = QueryStringQuery.from_dict(data)

                return componentsschemas_query_type_10
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_11 = NumericRangeQuery.from_dict(data)

                return componentsschemas_query_type_11
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_12 = TermRangeQuery.from_dict(data)

                return componentsschemas_query_type_12
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_13 = DateRangeStringQuery.from_dict(data)

                return componentsschemas_query_type_13
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_14 = BooleanQuery.from_dict(data)

                return componentsschemas_query_type_14
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_15 = ConjunctionQuery.from_dict(data)

                return componentsschemas_query_type_15
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_16 = DisjunctionQuery.from_dict(data)

                return componentsschemas_query_type_16
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_17 = MatchAllQuery.from_dict(data)

                return componentsschemas_query_type_17
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_18 = MatchNoneQuery.from_dict(data)

                return componentsschemas_query_type_18
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_19 = DocIdQuery.from_dict(data)

                return componentsschemas_query_type_19
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_20 = BoolFieldQuery.from_dict(data)

                return componentsschemas_query_type_20
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_21 = IPRangeQuery.from_dict(data)

                return componentsschemas_query_type_21
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_22 = GeoBoundingBoxQuery.from_dict(data)

                return componentsschemas_query_type_22
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_23 = GeoDistanceQuery.from_dict(data)

                return componentsschemas_query_type_23
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_24 = GeoBoundingPolygonQuery.from_dict(data)

                return componentsschemas_query_type_24
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            if not isinstance(data, dict):
                raise TypeError()
            componentsschemas_query_type_25 = GeoShapeQuery.from_dict(data)

            return componentsschemas_query_type_25

        filter_query = _parse_filter_query(d.pop("filter_query", UNSET))

        def _parse_exclusion_query(
            data: object,
        ) -> (
            BooleanQuery
            | BoolFieldQuery
            | ConjunctionQuery
            | DateRangeStringQuery
            | DisjunctionQuery
            | DocIdQuery
            | FuzzyQuery
            | GeoBoundingBoxQuery
            | GeoBoundingPolygonQuery
            | GeoDistanceQuery
            | GeoShapeQuery
            | IPRangeQuery
            | MatchAllQuery
            | MatchNoneQuery
            | MatchPhraseQuery
            | MatchQuery
            | MultiMatchQuery
            | MultiPhraseQuery
            | NumericRangeQuery
            | PhraseQuery
            | PrefixQuery
            | QueryStringQuery
            | RegexpQuery
            | TermQuery
            | TermRangeQuery
            | Unset
            | WildcardQuery
        ):
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_0 = TermQuery.from_dict(data)

                return componentsschemas_query_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_1 = MatchQuery.from_dict(data)

                return componentsschemas_query_type_1
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_2 = MultiMatchQuery.from_dict(data)

                return componentsschemas_query_type_2
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_3 = MatchPhraseQuery.from_dict(data)

                return componentsschemas_query_type_3
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_4 = PhraseQuery.from_dict(data)

                return componentsschemas_query_type_4
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_5 = MultiPhraseQuery.from_dict(data)

                return componentsschemas_query_type_5
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_6 = FuzzyQuery.from_dict(data)

                return componentsschemas_query_type_6
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_7 = PrefixQuery.from_dict(data)

                return componentsschemas_query_type_7
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_8 = RegexpQuery.from_dict(data)

                return componentsschemas_query_type_8
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_9 = WildcardQuery.from_dict(data)

                return componentsschemas_query_type_9
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_10 = QueryStringQuery.from_dict(data)

                return componentsschemas_query_type_10
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_11 = NumericRangeQuery.from_dict(data)

                return componentsschemas_query_type_11
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_12 = TermRangeQuery.from_dict(data)

                return componentsschemas_query_type_12
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_13 = DateRangeStringQuery.from_dict(data)

                return componentsschemas_query_type_13
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_14 = BooleanQuery.from_dict(data)

                return componentsschemas_query_type_14
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_15 = ConjunctionQuery.from_dict(data)

                return componentsschemas_query_type_15
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_16 = DisjunctionQuery.from_dict(data)

                return componentsschemas_query_type_16
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_17 = MatchAllQuery.from_dict(data)

                return componentsschemas_query_type_17
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_18 = MatchNoneQuery.from_dict(data)

                return componentsschemas_query_type_18
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_19 = DocIdQuery.from_dict(data)

                return componentsschemas_query_type_19
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_20 = BoolFieldQuery.from_dict(data)

                return componentsschemas_query_type_20
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_21 = IPRangeQuery.from_dict(data)

                return componentsschemas_query_type_21
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_22 = GeoBoundingBoxQuery.from_dict(data)

                return componentsschemas_query_type_22
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_23 = GeoDistanceQuery.from_dict(data)

                return componentsschemas_query_type_23
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_type_24 = GeoBoundingPolygonQuery.from_dict(data)

                return componentsschemas_query_type_24
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            if not isinstance(data, dict):
                raise TypeError()
            componentsschemas_query_type_25 = GeoShapeQuery.from_dict(data)

            return componentsschemas_query_type_25

        exclusion_query = _parse_exclusion_query(d.pop("exclusion_query", UNSET))

        _aggregations = d.pop("aggregations", UNSET)
        aggregations: QueryRequestAggregations | Unset
        if isinstance(_aggregations, Unset):
            aggregations = UNSET
        else:
            aggregations = QueryRequestAggregations.from_dict(_aggregations)

        _embeddings = d.pop("embeddings", UNSET)
        embeddings: QueryRequestEmbeddings | Unset
        if isinstance(_embeddings, Unset):
            embeddings = UNSET
        else:
            embeddings = QueryRequestEmbeddings.from_dict(_embeddings)

        search_effort = d.pop("search_effort", UNSET)

        fields = cast(list[str], d.pop("fields", UNSET))

        _hierarchy = d.pop("hierarchy", UNSET)
        hierarchy: QueryHierarchy | Unset
        if isinstance(_hierarchy, Unset):
            hierarchy = UNSET
        else:
            hierarchy = QueryHierarchy.from_dict(_hierarchy)

        limit = d.pop("limit", UNSET)

        offset = d.pop("offset", UNSET)

        timeout_ms = d.pop("timeout_ms", UNSET)

        _order_by = d.pop("order_by", UNSET)
        order_by: list[SortField] | Unset = UNSET
        if _order_by is not UNSET:
            order_by = []
            for order_by_item_data in _order_by:
                order_by_item = SortField.from_dict(order_by_item_data)

                order_by.append(order_by_item)

        search_after = cast(list[Any], d.pop("search_after", UNSET))

        search_before = cast(list[Any], d.pop("search_before", UNSET))

        distance_under = d.pop("distance_under", UNSET)

        distance_over = d.pop("distance_over", UNSET)

        _merge_config = d.pop("merge_config", UNSET)
        merge_config: MergeConfig | Unset
        if isinstance(_merge_config, Unset):
            merge_config = UNSET
        else:
            merge_config = MergeConfig.from_dict(_merge_config)

        count = d.pop("count", UNSET)

        profile = d.pop("profile", UNSET)

        _reranker = d.pop("reranker", UNSET)
        reranker: RerankerConfig | Unset
        if isinstance(_reranker, Unset):
            reranker = UNSET
        else:
            reranker = RerankerConfig.from_dict(_reranker)

        _analyses = d.pop("analyses", UNSET)
        analyses: Analyses | Unset
        if isinstance(_analyses, Unset):
            analyses = UNSET
        else:
            analyses = Analyses.from_dict(_analyses)

        _graph_queries = d.pop("graph_queries", UNSET)
        graph_queries: QueryRequestGraphQueries | Unset
        if isinstance(_graph_queries, Unset):
            graph_queries = UNSET
        else:
            graph_queries = QueryRequestGraphQueries.from_dict(_graph_queries)

        _graph_searches = d.pop("graph_searches", UNSET)
        graph_searches: QueryRequestGraphSearches | Unset
        if isinstance(_graph_searches, Unset):
            graph_searches = UNSET
        else:
            graph_searches = QueryRequestGraphSearches.from_dict(_graph_searches)

        _expand_strategy = d.pop("expand_strategy", UNSET)
        expand_strategy: QueryRequestExpandStrategy | Unset
        if isinstance(_expand_strategy, Unset):
            expand_strategy = UNSET
        else:
            expand_strategy = QueryRequestExpandStrategy(_expand_strategy)

        document_renderer = d.pop("document_renderer", UNSET)

        _pruner = d.pop("pruner", UNSET)
        pruner: Pruner | Unset
        if isinstance(_pruner, Unset):
            pruner = UNSET
        else:
            pruner = Pruner.from_dict(_pruner)

        _join = d.pop("join", UNSET)
        join: JoinClause | Unset
        if isinstance(_join, Unset):
            join = UNSET
        else:
            join = JoinClause.from_dict(_join)

        _foreign_sources = d.pop("foreign_sources", UNSET)
        foreign_sources: QueryRequestForeignSources | Unset
        if isinstance(_foreign_sources, Unset):
            foreign_sources = UNSET
        else:
            foreign_sources = QueryRequestForeignSources.from_dict(_foreign_sources)

        _tree_search = d.pop("tree_search", UNSET)
        tree_search: TreeSearchConfig | Unset
        if isinstance(_tree_search, Unset):
            tree_search = UNSET
        else:
            tree_search = TreeSearchConfig.from_dict(_tree_search)

        retrieval_query_request = cls(
            table=table,
            query=query,
            full_text_search=full_text_search,
            semantic_search=semantic_search,
            embedding_template=embedding_template,
            indexes=indexes,
            filter_prefix=filter_prefix,
            filter_query=filter_query,
            exclusion_query=exclusion_query,
            aggregations=aggregations,
            embeddings=embeddings,
            search_effort=search_effort,
            fields=fields,
            hierarchy=hierarchy,
            limit=limit,
            offset=offset,
            timeout_ms=timeout_ms,
            order_by=order_by,
            search_after=search_after,
            search_before=search_before,
            distance_under=distance_under,
            distance_over=distance_over,
            merge_config=merge_config,
            count=count,
            profile=profile,
            reranker=reranker,
            analyses=analyses,
            graph_queries=graph_queries,
            graph_searches=graph_searches,
            expand_strategy=expand_strategy,
            document_renderer=document_renderer,
            pruner=pruner,
            join=join,
            foreign_sources=foreign_sources,
            tree_search=tree_search,
        )

        retrieval_query_request.additional_properties = d
        return retrieval_query_request

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> Any:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
