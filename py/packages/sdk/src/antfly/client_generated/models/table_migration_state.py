from enum import StrEnum


class TableMigrationState(StrEnum):
    REBUILDING = "rebuilding"

    def __str__(self) -> str:
        return str(self.value)
