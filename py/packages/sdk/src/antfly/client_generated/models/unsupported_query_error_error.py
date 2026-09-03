from enum import Enum


class UnsupportedQueryErrorError(str, Enum):
    UNSUPPORTED_QUERY_REQUEST = "unsupported_query_request"

    def __str__(self) -> str:
        return str(self.value)
