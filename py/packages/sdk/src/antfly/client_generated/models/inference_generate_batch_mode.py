from enum import Enum


class InferenceGenerateBatchMode(str, Enum):
    SYNC = "sync"

    def __str__(self) -> str:
        return str(self.value)
