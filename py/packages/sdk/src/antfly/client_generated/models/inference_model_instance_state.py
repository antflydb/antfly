from enum import StrEnum


class InferenceModelInstanceState(StrEnum):
    LOADED = "loaded"
    LOADING = "loading"

    def __str__(self) -> str:
        return str(self.value)
