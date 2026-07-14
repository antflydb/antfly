from enum import Enum


class ListRestoreJobsScope(str, Enum):
    CLUSTER = "cluster"
    TABLE = "table"

    def __str__(self) -> str:
        return str(self.value)
