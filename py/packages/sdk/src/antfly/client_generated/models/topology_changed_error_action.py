from enum import StrEnum


class TopologyChangedErrorAction(StrEnum):
    RETRY_QUERY = "retry_query"

    def __str__(self) -> str:
        return str(self.value)
