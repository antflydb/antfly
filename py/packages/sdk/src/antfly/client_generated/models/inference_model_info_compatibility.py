from enum import StrEnum


class InferenceModelInfoCompatibility(StrEnum):
    COMPATIBLE = "compatible"
    INCOMPATIBLE = "incompatible"
    UNKNOWN = "unknown"

    def __str__(self) -> str:
        return str(self.value)
