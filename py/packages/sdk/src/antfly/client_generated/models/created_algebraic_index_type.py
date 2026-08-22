from enum import Enum


class CreatedAlgebraicIndexType(str, Enum):
    ALGEBRAIC = "algebraic"

    def __str__(self) -> str:
        return str(self.value)
