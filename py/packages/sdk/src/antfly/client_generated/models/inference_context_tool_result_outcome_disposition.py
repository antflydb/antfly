from enum import StrEnum


class InferenceContextToolResultOutcomeDisposition(StrEnum):
    DROPPED = "dropped"
    RETAINED = "retained"
    SUMMARIZED = "summarized"

    def __str__(self) -> str:
        return str(self.value)
