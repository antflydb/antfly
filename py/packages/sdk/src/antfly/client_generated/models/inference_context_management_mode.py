from enum import StrEnum


class InferenceContextManagementMode(StrEnum):
    REJECT = "reject"
    WINDOW = "window"

    def __str__(self) -> str:
        return str(self.value)
