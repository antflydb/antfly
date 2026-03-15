from enum import Enum


class RetrievalReasoningStepStatus(str, Enum):
    ERROR = "error"
    SKIPPED = "skipped"
    SUCCESS = "success"

    def __str__(self) -> str:
        return str(self.value)
