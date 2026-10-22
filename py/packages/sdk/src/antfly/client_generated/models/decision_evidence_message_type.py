from enum import StrEnum


class DecisionEvidenceMessageType(StrEnum):
    MESSAGE = "message"

    def __str__(self) -> str:
        return str(self.value)
