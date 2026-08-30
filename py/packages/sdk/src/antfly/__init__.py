"""
Antfly SDK - Python client for Antfly distributed key-value store and search engine.
"""

from .client import AntflyClient, CreatedIndex, CreateIndexRequest, IndexOperations, antfly_embedder
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
    IndexMutationTemporarilyUnavailableError,
    InferenceAPIError,
    InferenceCapacityError,
    StorageResourceExhaustedError,
)
from .index_config import (
    ArtifactEmbeddingSource,
    FullTextArtifactSource,
    GraphArtifactSource,
    GraphContextMapping,
    GraphEdgeMapping,
    GraphNodeMapping,
    artifact_embedding_index_config,
    artifact_full_text_index_config,
    artifact_index_sources,
    graph_index_sources,
    validate_create_index_request_relationships,
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
    "IndexMutationTemporarilyUnavailableError",
    "AntflyConnectionError",
    "AntflyAuthError",
    "InferenceAPIError",
    "InferenceCapacityError",
    "StorageResourceExhaustedError",
    "SparseEmbedding",
    "PackedSparseEmbedding",
    "ArtifactEmbeddingSource",
    "FullTextArtifactSource",
    "GraphArtifactSource",
    "GraphContextMapping",
    "GraphEdgeMapping",
    "GraphNodeMapping",
    "artifact_embedding_index_config",
    "artifact_full_text_index_config",
    "artifact_index_sources",
    "graph_index_sources",
    "validate_create_index_request_relationships",
    "__version__",
]
