from enum import StrEnum


class LinearMergePageStatus(StrEnum):
    ERROR = "error"
    PARTIAL = "partial"
    SUCCESS = "success"

    def __str__(self) -> str:
        return str(self.value)
