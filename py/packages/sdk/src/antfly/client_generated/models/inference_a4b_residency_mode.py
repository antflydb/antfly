from enum import StrEnum


class InferenceA4BResidencyMode(StrEnum):
    AUTO = "auto"
    RESIDENT = "resident"
    STREAMED = "streamed"

    def __str__(self) -> str:
        return str(self.value)
