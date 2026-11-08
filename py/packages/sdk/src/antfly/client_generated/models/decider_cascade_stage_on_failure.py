from enum import StrEnum


class DeciderCascadeStageOnFailure(StrEnum):
    FAIL = "fail"
    KEEP_PREVIOUS = "keep_previous"

    def __str__(self) -> str:
        return str(self.value)
