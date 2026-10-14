from enum import StrEnum


class InferenceModelRuntimeLoadResponseOutcome(StrEnum):
    LOADED = "loaded"

    def __str__(self) -> str:
        return str(self.value)
