from enum import Enum


class TermiteTranscribeObjectObject(str, Enum):
    TRANSCRIPTION = "transcription"

    def __str__(self) -> str:
        return str(self.value)
