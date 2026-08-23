from enum import Enum


class CreateAlgebraicIndexRequestType(str, Enum):
    ALGEBRAIC = "algebraic"

    def __str__(self) -> str:
        return str(self.value)
