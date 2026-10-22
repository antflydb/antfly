from enum import StrEnum


class InferenceModelInstanceUnloadBlockedReason(StrEnum):
    IN_USE = "in_use"
    LOADING = "loading"
    STARTUP_MANAGED = "startup_managed"

    def __str__(self) -> str:
        return str(self.value)
