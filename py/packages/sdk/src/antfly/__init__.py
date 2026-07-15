"""
Antfly SDK - Python client for Antfly distributed key-value store and search engine.
"""

from .client import AntflyClient
from .client_generated.models.embedding_type_1 import EmbeddingType1 as SparseEmbedding
from .client_generated.models.embedding_type_3 import EmbeddingType3 as PackedSparseEmbedding
from .exceptions import AntflyAuthError, AntflyConnectionError, AntflyException
from .index_config import (
    ArtifactEmbeddingSource,
    artifact_embedding_index_config,
    artifact_index_sources,
    graph_index_sources,
)

__version__ = "0.1.0"

__all__ = [
    "AntflyClient",
    "AntflyException",
    "AntflyConnectionError",
    "AntflyAuthError",
    "SparseEmbedding",
    "PackedSparseEmbedding",
    "ArtifactEmbeddingSource",
    "artifact_embedding_index_config",
    "artifact_index_sources",
    "graph_index_sources",
    "__version__",
]
