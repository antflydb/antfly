from enum import StrEnum


class InferenceAudioReadMetadataLanguageSource(StrEnum):
    DETECTED = "detected"
    FORCED = "forced"
    UNKNOWN = "unknown"

    def __str__(self) -> str:
        return str(self.value)
