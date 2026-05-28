from enum import Enum


class InferenceTranscribeObjectObject(str, Enum):
    TRANSCRIPTION = "transcription"

    def __str__(self) -> str:
        return str(self.value)
