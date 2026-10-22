from enum import StrEnum


class DecisionEvidenceMessageRole(StrEnum):
    USER = "user"

    def __str__(self) -> str:
        return str(self.value)
