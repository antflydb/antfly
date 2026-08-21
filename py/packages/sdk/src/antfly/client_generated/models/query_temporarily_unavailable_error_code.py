from enum import Enum


class QueryTemporarilyUnavailableErrorCode(str, Enum):
    DOC_IDENTITY_UNAVAILABLE = "doc_identity_unavailable"
    INDEX_REBUILDING = "index_rebuilding"
    QUERY_EMBEDDING_TEMPORARILY_UNAVAILABLE = "query_embedding_temporarily_unavailable"
    READ_REQUIRES_PRIMARY = "read_requires_primary"
    STANDBY_READ_UNAVAILABLE = "standby_read_unavailable"
    STORAGE_READ_TEMPORARILY_UNAVAILABLE = "storage_read_temporarily_unavailable"

    def __str__(self) -> str:
        return str(self.value)
