from enum import StrEnum


class InferenceAudioReadSegmentGranularity(StrEnum):
    PHRASE = "phrase"
    WINDOW = "window"

    def __str__(self) -> str:
        return str(self.value)
