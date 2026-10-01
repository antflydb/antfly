from enum import StrEnum


class ExtractionJointConstraintEntityOverlapPolicyPolicy(StrEnum):
    ALLOW = "allow"
    DISALLOW = "disallow"
    NESTED = "nested"

    def __str__(self) -> str:
        return str(self.value)
