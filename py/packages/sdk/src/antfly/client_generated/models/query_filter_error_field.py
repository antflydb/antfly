from enum import StrEnum


class QueryFilterErrorField(StrEnum):
    EXCLUSION_QUERY = "exclusion_query"
    FILTER_QUERY = "filter_query"

    def __str__(self) -> str:
        return str(self.value)
