from enum import Enum


class ReplicationSourceType(str, Enum):
    POSTGRES = "postgres"

    def __str__(self) -> str:
        return str(self.value)
