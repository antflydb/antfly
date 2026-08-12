from enum import Enum


class InferenceGenerateRequestSpeculationPolicy(str, Enum):
    AUTO = "auto"
    FORCE = "force"
    OFF = "off"

    def __str__(self) -> str:
        return str(self.value)
