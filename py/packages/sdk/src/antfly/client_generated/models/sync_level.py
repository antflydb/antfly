from enum import Enum


class SyncLevel(str, Enum):
    ENRICHMENTS = "enrichments"
    FULL_INDEX = "full_index"
    PROPOSE = "propose"
    QUERY = "query"
    WRITE = "write"

    def __str__(self) -> str:
        return str(self.value)
