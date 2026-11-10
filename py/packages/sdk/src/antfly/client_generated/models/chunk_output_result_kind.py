from enum import StrEnum


class ChunkOutputResultKind(StrEnum):
    CLASSIFY = "classify"
    DIARIZE = "diarize"
    EMBEDDING = "embedding"
    TRANSCRIBE = "transcribe"

    def __str__(self) -> str:
        return str(self.value)
