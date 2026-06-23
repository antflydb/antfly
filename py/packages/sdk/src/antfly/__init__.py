"""
Antfly SDK - Python client for Antfly distributed key-value store and search engine.
"""

from .client import AntflyClient
from .client_generated.models.embedding_type_1 import EmbeddingType1 as SparseEmbedding
from .client_generated.models.embedding_type_3 import EmbeddingType3 as PackedSparseEmbedding
from .client_generated.models.sql_statement_request import SqlStatementRequest
from .client_generated.models.sql_statement_response import SqlStatementResponse
from .client_generated.models.sql_statement_response_kind import SqlStatementResponseKind
from .exceptions import AntflyAuthError, AntflyConnectionError, AntflyException

__version__ = "0.1.0"

__all__ = [
    "AntflyClient",
    "AntflyException",
    "AntflyConnectionError",
    "AntflyAuthError",
    "SparseEmbedding",
    "PackedSparseEmbedding",
    "SqlStatementRequest",
    "SqlStatementResponse",
    "SqlStatementResponseKind",
    "__version__",
]
