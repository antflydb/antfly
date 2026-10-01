from enum import StrEnum


class RelationalConstraintValidationState(StrEnum):
    ENFORCED = "enforced"
    INVALID = "invalid"
    UNVALIDATED = "unvalidated"
    VALIDATING = "validating"

    def __str__(self) -> str:
        return str(self.value)
