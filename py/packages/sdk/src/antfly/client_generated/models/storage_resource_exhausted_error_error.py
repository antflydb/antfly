from enum import StrEnum


class StorageResourceExhaustedErrorError(StrEnum):
    STORAGE_RESOURCE_EXHAUSTED = "storage_resource_exhausted"

    def __str__(self) -> str:
        return str(self.value)
