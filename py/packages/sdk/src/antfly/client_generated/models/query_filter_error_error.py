from enum import StrEnum


class QueryFilterErrorError(StrEnum):
    INVALID_QUERY_REQUEST = "invalid_query_request"
    UNSUPPORTED_QUERY_REQUEST = "unsupported_query_request"

    def __str__(self) -> str:
        return str(self.value)
