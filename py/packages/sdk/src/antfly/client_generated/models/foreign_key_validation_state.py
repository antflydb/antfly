from enum import Enum


class ForeignKeyValidationState(str, Enum):
    ENFORCED = "enforced"
    UNVALIDATED = "unvalidated"

    def __str__(self) -> str:
        return str(self.value)
