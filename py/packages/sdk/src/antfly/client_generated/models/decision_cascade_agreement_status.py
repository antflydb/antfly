from enum import StrEnum


class DecisionCascadeAgreementStatus(StrEnum):
    AGREED = "agreed"
    DISAGREED = "disagreed"
    UNVERIFIED = "unverified"

    def __str__(self) -> str:
        return str(self.value)
