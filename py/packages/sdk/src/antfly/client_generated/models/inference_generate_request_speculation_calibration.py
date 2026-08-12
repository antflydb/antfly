from enum import Enum


class InferenceGenerateRequestSpeculationCalibration(str, Enum):
    NONE = "none"
    POSITIVE = "positive"
    PROBE = "probe"

    def __str__(self) -> str:
        return str(self.value)
