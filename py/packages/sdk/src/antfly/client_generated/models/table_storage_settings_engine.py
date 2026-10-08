from enum import StrEnum


class TableStorageSettingsEngine(StrEnum):
    LOCAL = "local"
    OBJECT = "object"

    def __str__(self) -> str:
        return str(self.value)
