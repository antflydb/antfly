from enum import StrEnum


class UnsupportedIndexCapabilityErrorError(StrEnum):
    UNSUPPORTED_INDEX_CAPABILITY = "unsupported_index_capability"

    def __str__(self) -> str:
        return str(self.value)
