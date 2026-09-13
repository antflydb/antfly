from enum import StrEnum


class ExtractionJointConstraintEntityOverlapPolicyType(StrEnum):
    ENTITYOVERLAPPOLICY = "EntityOverlapPolicy"

    def __str__(self) -> str:
        return str(self.value)
