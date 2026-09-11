from enum import StrEnum


class InferenceGenerateBatchMode(StrEnum):
    SYNC = "sync"

    def __str__(self) -> str:
        return str(self.value)
