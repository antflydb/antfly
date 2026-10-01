from enum import StrEnum


class RelationalConstraintRetryResponseStatus(StrEnum):
    ACCEPTED = "accepted"

    def __str__(self) -> str:
        return str(self.value)
