from enum import Enum


class LinearMergePageStatus(str, Enum):
    ERROR = "error"
    PARTIAL = "partial"
    SUCCESS = "success"

    def __str__(self) -> str:
        return str(self.value)
