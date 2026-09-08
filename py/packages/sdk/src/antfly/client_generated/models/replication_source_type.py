from enum import StrEnum


class ReplicationSourceType(StrEnum):
    POSTGRES = "postgres"

    def __str__(self) -> str:
        return str(self.value)
