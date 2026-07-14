from enum import Enum


class StorageRuntimeStatusEngine(str, Enum):
    LITE = "lite"
    LOCAL = "local"
    OBJECT = "object"

    def __str__(self) -> str:
        return str(self.value)
