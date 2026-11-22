from enum import StrEnum


class TableSchemaReplicaIdentity(StrEnum):
    DEFAULT = "default"
    FULL = "full"

    def __str__(self) -> str:
        return str(self.value)
