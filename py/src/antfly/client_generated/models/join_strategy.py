from enum import Enum


class JoinStrategy(str, Enum):
    BROADCAST = "broadcast"
    INDEX_LOOKUP = "index_lookup"
    SHUFFLE = "shuffle"

    def __str__(self) -> str:
        return str(self.value)
