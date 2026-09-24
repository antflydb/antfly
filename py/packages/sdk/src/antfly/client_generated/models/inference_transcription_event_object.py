from enum import StrEnum


class InferenceTranscriptionEventObject(StrEnum):
    TRANSCRIPTION_EVENT = "transcription.event"

    def __str__(self) -> str:
        return str(self.value)
