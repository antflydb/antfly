from enum import StrEnum


class InferenceTextRegionOrigin(StrEnum):
    DERIVED = "derived"
    ESTIMATED = "estimated"
    INTERPRETATION = "interpretation"
    NATIVE = "native"
    TRANSCRIPTION = "transcription"

    def __str__(self) -> str:
        return str(self.value)
