from enum import StrEnum


class RelationalIndexStatsIndexType(StrEnum):
    RELATIONAL = "relational"

    def __str__(self) -> str:
        return str(self.value)
