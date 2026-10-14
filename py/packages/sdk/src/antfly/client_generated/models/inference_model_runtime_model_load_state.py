from enum import StrEnum


class InferenceModelRuntimeModelLoadState(StrEnum):
    IDLE = "idle"
    LOADING = "loading"

    def __str__(self) -> str:
        return str(self.value)
