from enum import Enum


class BatchWriteResponse409(str, Enum):
    PARTIAL_WRITE_OUTCOME = "partial write outcome"
    WRITE_OUTCOME_UNKNOWN = "write outcome unknown"

    def __str__(self) -> str:
        return str(self.value)
