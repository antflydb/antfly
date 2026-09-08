from enum import StrEnum


class InferenceTranscribeObjectObject(StrEnum):
    TRANSCRIPTION = "transcription"

    def __str__(self) -> str:
        return str(self.value)
