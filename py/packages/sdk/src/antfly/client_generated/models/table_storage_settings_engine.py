from enum import StrEnum


class TableStorageSettingsEngine(StrEnum):
    NATIVE = "native"
    OBJECT = "object"

    def __str__(self) -> str:
        return str(self.value)
