from enum import StrEnum


class StorageRuntimeStatusEngine(StrEnum):
    LITE = "lite"
    LOCAL = "local"
    OBJECT = "object"

    def __str__(self) -> str:
        return str(self.value)
