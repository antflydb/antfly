from enum import Enum


class InferenceA4BResidencyMode(str, Enum):
    AUTO = "auto"
    RESIDENT = "resident"
    STREAMED = "streamed"

    def __str__(self) -> str:
        return str(self.value)
