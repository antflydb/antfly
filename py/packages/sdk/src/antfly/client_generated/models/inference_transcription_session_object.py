from enum import StrEnum


class InferenceTranscriptionSessionObject(StrEnum):
    TRANSCRIPTION_SESSION = "transcription.session"

    def __str__(self) -> str:
        return str(self.value)
