from enum import StrEnum


class InferenceGenerateRequestSpeculationPolicy(StrEnum):
    AUTO = "auto"
    FORCE = "force"
    OFF = "off"

    def __str__(self) -> str:
        return str(self.value)
