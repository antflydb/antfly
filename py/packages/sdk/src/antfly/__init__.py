"""
Antfly SDK - Python client for Antfly distributed key-value store and search engine.
"""

from .client import AntflyClient, IndexOperations
from .client_generated.models.embedding_type_1 import EmbeddingType1 as SparseEmbedding
from .client_generated.models.embedding_type_3 import EmbeddingType3 as PackedSparseEmbedding
from .exceptions import (
    AntflyAuthError,
    AntflyConnectionError,
    AntflyException,
    InferenceAPIError,
    InferenceCapacityError,
)

__version__ = "0.1.0"

__all__ = [
    "AntflyClient",
    "IndexOperations",
    "AntflyException",
    "AntflyConnectionError",
    "AntflyAuthError",
    "InferenceAPIError",
    "InferenceCapacityError",
    "SparseEmbedding",
    "PackedSparseEmbedding",
    "__version__",
]
