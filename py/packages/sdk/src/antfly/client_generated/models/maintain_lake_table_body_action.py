from enum import StrEnum


class MaintainLakeTableBodyAction(StrEnum):
    COMPACT = "compact"
    STATUS = "status"
    VACUUM = "vacuum"
    WAL_GC = "wal_gc"

    def __str__(self) -> str:
        return str(self.value)
