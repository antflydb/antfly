from enum import StrEnum


class InferenceGenerateRequestSpeculationCalibration(StrEnum):
    NONE = "none"
    POSITIVE = "positive"
    PROBE = "probe"

    def __str__(self) -> str:
        return str(self.value)
