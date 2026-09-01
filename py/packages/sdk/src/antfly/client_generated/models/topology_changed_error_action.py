from enum import Enum


class TopologyChangedErrorAction(str, Enum):
    RETRY_QUERY = "retry_query"

    def __str__(self) -> str:
        return str(self.value)
