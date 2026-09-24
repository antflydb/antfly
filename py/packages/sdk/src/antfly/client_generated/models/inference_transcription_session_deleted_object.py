from enum import StrEnum


class InferenceTranscriptionSessionDeletedObject(StrEnum):
    TRANSCRIPTION_SESSION_DELETED = "transcription.session.deleted"

    def __str__(self) -> str:
        return str(self.value)
