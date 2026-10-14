from enum import StrEnum


class InferenceModelRuntimeKind(StrEnum):
    COMPOSITE = "composite"
    PRIMARY = "primary"

    def __str__(self) -> str:
        return str(self.value)
