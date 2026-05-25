from enum import Enum


class SyncLevel(str, Enum):
    AKNN = "aknn"
    ENRICHMENTS = "enrichments"
    FULL_INDEX = "full_index"
    FULL_TEXT = "full_text"
    PROPOSE = "propose"
    WRITE = "write"

    def __str__(self) -> str:
        return str(self.value)
