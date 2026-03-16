from enum import Enum


class TableMigrationState(str, Enum):
    REBUILDING = "rebuilding"

    def __str__(self) -> str:
        return str(self.value)
