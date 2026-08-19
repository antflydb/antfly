"""
Antfly SDK - Python client for Antfly distributed key-value store and search engine.
"""

from .client import AntflyClient, CreateIndexRequest, IndexOperations
from .client_generated.models import (
    CreateAlgebraicIndexRequest,
    CreateAlgebraicIndexRequestType,
    CreatedIndex,
    CreateEmbeddingsIndexRequest,
    CreateEmbeddingsIndexRequestType,
    CreateFullTextIndexRequest,
    CreateFullTextIndexRequestType,
    CreateGraphIndexRequest,
    CreateGraphIndexRequestType,
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
    "AntflyException",
    "AntflyConnectionError",
    "AntflyAuthError",
    "InferenceAPIError",
    "InferenceCapacityError",
    "StorageResourceExhaustedError",
    "SparseEmbedding",
    "PackedSparseEmbedding",
    "__version__",
]
