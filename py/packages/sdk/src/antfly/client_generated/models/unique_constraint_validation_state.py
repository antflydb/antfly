from enum import Enum


class UniqueConstraintValidationState(str, Enum):
    ENFORCED = "enforced"
    UNVALIDATED = "unvalidated"

    def __str__(self) -> str:
        return str(self.value)
