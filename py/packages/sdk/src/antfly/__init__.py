"""
Antfly SDK - Python client for Antfly distributed key-value store and search engine.
"""

from .client import (
    AntflyClient,
    CreatedIndex,
    CreateIndexRequest,
    GraphQueriesInput,
    GraphQueryInput,
    IndexOperations,
    antfly_embedder,
)
from .client_generated.models import (
    CreateAlgebraicIndexRequest,
    CreateAlgebraicIndexRequestType,
    CreatedAlgebraicIndex,
    CreatedEmbeddingsIndex,
    CreatedFullTextIndex,
    CreatedGraphIndex,
    CreateEmbeddingsIndexRequest,
    CreateEmbeddingsIndexRequestType,
    CreateFullTextIndexRequest,
    CreateFullTextIndexRequestType,
    CreateGraphIndexRequest,
    CreateGraphIndexRequestType,
    DerivedCoveragePolicy,
    DistanceMetric,
    EmbedderConfig,
    EmbedderProvider,
)
from .client_generated.models.embedding_type_1 import EmbeddingType1 as SparseEmbedding
from .client_generated.models.embedding_type_3 import EmbeddingType3 as PackedSparseEmbedding
from .exceptions import (
    AntflyAuthError,
    AntflyConnectionError,
    AntflyException,
    InferenceAPIError,
    InferenceCapacityError,
    StorageResourceExhaustedError,
)
from .graph_identifier_policy_generated import (
    GRAPH_IDENTIFIER_POLICY_VERSION,
    GRAPH_IDENTIFIER_UNICODE_VERSION,
    is_valid_graph_identifier,
)
from .graph_queries import (
    GraphCountAggregate,
    count_graph_alias,
    count_graph_rows,
    graph_date_range_filter,
    graph_numeric_range_filter,
    graph_term_range_filter,
)

__version__ = "0.1.0"

__all__ = [
    "AntflyClient",
    "IndexOperations",
    "CreateIndexRequest",
    "CreateFullTextIndexRequest",
    "CreateFullTextIndexRequestType",
    "CreateEmbeddingsIndexRequest",
    "CreateEmbeddingsIndexRequestType",
    "CreateGraphIndexRequest",
    "CreateGraphIndexRequestType",
    "CreateAlgebraicIndexRequest",
    "CreateAlgebraicIndexRequestType",
    "CreatedIndex",
    "GraphQueryInput",
    "GraphQueriesInput",
    "CreatedFullTextIndex",
    "CreatedEmbeddingsIndex",
    "CreatedGraphIndex",
    "CreatedAlgebraicIndex",
    "DerivedCoveragePolicy",
    "DistanceMetric",
    "EmbedderConfig",
    "EmbedderProvider",
    "antfly_embedder",
    "AntflyException",
    "AntflyConnectionError",
    "AntflyAuthError",
    "InferenceAPIError",
    "InferenceCapacityError",
    "StorageResourceExhaustedError",
    "GRAPH_IDENTIFIER_POLICY_VERSION",
    "GRAPH_IDENTIFIER_UNICODE_VERSION",
    "is_valid_graph_identifier",
    "GraphCountAggregate",
    "count_graph_alias",
    "count_graph_rows",
    "graph_date_range_filter",
    "graph_numeric_range_filter",
    "graph_term_range_filter",
    "SparseEmbedding",
    "PackedSparseEmbedding",
    "__version__",
]
