from enum import Enum


class QueryDependencyErrorCode(str, Enum):
    EMBEDDING_INDEX_NOT_FOUND = "embedding_index_not_found"
    QUERY_EMBEDDING_INPUT_TOO_LARGE = "query_embedding_input_too_large"
    QUERY_EMBEDDING_OVERLOADED = "query_embedding_overloaded"
    QUERY_EMBEDDING_RATE_LIMITED = "query_embedding_rate_limited"
    QUERY_EMBEDDING_UPSTREAM_FAILURE = "query_embedding_upstream_failure"
    QUERY_TIMEOUT = "query_timeout"
    RERANKER_RATE_LIMITED = "reranker_rate_limited"
    RERANKER_UPSTREAM_FAILURE = "reranker_upstream_failure"

    def __str__(self) -> str:
        return str(self.value)
