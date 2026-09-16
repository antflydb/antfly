from enum import StrEnum


class InferenceDictationEventType(StrEnum):
    DICTATION_COMPLETED = "dictation.completed"
    DICTATION_DELTA = "dictation.delta"
    DICTATION_TRANSCRIPT = "dictation.transcript"
    ERROR = "error"

    def __str__(self) -> str:
        return str(self.value)
