from enum import StrEnum


class MaintainLakeTableBodyAction(StrEnum):
    COMPACT = "compact"
    VACUUM = "vacuum"
    WAL_GC = "wal_gc"

    def __str__(self) -> str:
        return str(self.value)
