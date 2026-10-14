from enum import StrEnum


class InferenceModelRuntimeUnloadResponseOutcome(StrEnum):
    ALREADY_UNLOADED = "already_unloaded"
    UNLOADED = "unloaded"

    def __str__(self) -> str:
        return str(self.value)
