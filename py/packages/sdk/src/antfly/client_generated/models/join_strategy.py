from enum import StrEnum


class JoinStrategy(StrEnum):
    BROADCAST = "broadcast"
    INDEX_LOOKUP = "index_lookup"
    SHUFFLE = "shuffle"

    def __str__(self) -> str:
        return str(self.value)
