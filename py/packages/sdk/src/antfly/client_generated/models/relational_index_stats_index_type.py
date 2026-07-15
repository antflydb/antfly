from enum import Enum


class RelationalIndexStatsIndexType(str, Enum):
    RELATIONAL = "relational"

    def __str__(self) -> str:
        return str(self.value)
