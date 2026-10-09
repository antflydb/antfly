from enum import StrEnum


class ExternalLakeTableSourceObjectMutability(StrEnum):
    IMMUTABLE = "immutable"
    MUTABLE = "mutable"

    def __str__(self) -> str:
        return str(self.value)
