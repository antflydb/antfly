from enum import Enum


class StorageResourceExhaustedErrorError(str, Enum):
    STORAGE_RESOURCE_EXHAUSTED = "storage_resource_exhausted"

    def __str__(self) -> str:
        return str(self.value)
