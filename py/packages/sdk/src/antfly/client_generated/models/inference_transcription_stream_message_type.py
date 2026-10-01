from enum import StrEnum


class InferenceTranscriptionStreamMessageType(StrEnum):
    ERROR = "error"
    PING = "ping"
    SESSION_CLOSED = "session.closed"
    SESSION_OPEN = "session.open"
    TRANSCRIPTION_EVENT = "transcription.event"

    def __str__(self) -> str:
        return str(self.value)
