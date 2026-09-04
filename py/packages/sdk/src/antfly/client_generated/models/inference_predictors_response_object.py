from enum import StrEnum


class InferencePredictorsResponseObject(StrEnum):
    LIST = "list"

    def __str__(self) -> str:
        return str(self.value)
