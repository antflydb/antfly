from enum import StrEnum


class EnrichmentRuntimeStatusActivePhase(StrEnum):
    EXECUTING = "executing"
    IDLE = "idle"
    LOADING_MODEL = "loading_model"
    PUBLISHING = "publishing"
    SERIALIZING = "serializing"
    TOKENIZING = "tokenizing"

    def __str__(self) -> str:
        return str(self.value)
