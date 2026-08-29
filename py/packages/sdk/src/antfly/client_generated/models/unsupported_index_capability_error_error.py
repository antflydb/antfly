from enum import Enum


class UnsupportedIndexCapabilityErrorError(str, Enum):
    UNSUPPORTED_INDEX_CAPABILITY = "unsupported_index_capability"

    def __str__(self) -> str:
        return str(self.value)
