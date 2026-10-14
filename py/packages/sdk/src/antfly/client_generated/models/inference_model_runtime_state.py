from enum import StrEnum


class InferenceModelRuntimeState(StrEnum):
    IN_USE = "in_use"
    LOADED = "loaded"
    LOADING = "loading"

    def __str__(self) -> str:
        return str(self.value)
