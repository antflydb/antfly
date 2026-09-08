from enum import StrEnum


class CreatedAlgebraicIndexType(StrEnum):
    ALGEBRAIC = "algebraic"

    def __str__(self) -> str:
        return str(self.value)
