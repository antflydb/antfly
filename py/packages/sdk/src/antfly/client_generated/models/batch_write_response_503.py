from enum import Enum


class BatchWriteResponse503(str, Enum):
    WRITE_UNAVAILABLE = "write unavailable"

    def __str__(self) -> str:
        return str(self.value)
