from enum import StrEnum


class RestoreJobScope(StrEnum):
    CLUSTER = "cluster"
    TABLE = "table"

    def __str__(self) -> str:
        return str(self.value)
