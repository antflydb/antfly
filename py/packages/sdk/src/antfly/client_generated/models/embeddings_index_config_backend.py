from enum import Enum


class EmbeddingsIndexConfigBackend(str, Enum):
    LSM = "lsm"
    SEGMENTS = "segments"

    def __str__(self) -> str:
        return str(self.value)
