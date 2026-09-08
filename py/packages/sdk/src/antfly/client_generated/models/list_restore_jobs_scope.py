from enum import StrEnum


class ListRestoreJobsScope(StrEnum):
    CLUSTER = "cluster"
    TABLE = "table"

    def __str__(self) -> str:
        return str(self.value)
