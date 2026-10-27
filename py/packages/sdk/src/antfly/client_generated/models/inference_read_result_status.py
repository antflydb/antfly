from enum import StrEnum


class InferenceReadResultStatus(StrEnum):
    COMPLETE = "complete"
    TRUNCATED = "truncated"
    UNRESOLVED = "unresolved"

    def __str__(self) -> str:
        return str(self.value)
