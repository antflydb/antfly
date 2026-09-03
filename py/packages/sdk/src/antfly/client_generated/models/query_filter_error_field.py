from enum import Enum


class QueryFilterErrorField(str, Enum):
    EXCLUSION_QUERY = "exclusion_query"
    FILTER_QUERY = "filter_query"

    def __str__(self) -> str:
        return str(self.value)
