from enum import StrEnum


class LinearMergePageStatus(StrEnum):
    SUCCESS = "success"

    def __str__(self) -> str:
        return str(self.value)
