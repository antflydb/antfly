from enum import Enum


class RestoreJobScope(str, Enum):
    CLUSTER = "cluster"
    TABLE = "table"

    def __str__(self) -> str:
        return str(self.value)
