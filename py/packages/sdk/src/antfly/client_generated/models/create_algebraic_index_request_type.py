from enum import StrEnum


class CreateAlgebraicIndexRequestType(StrEnum):
    ALGEBRAIC = "algebraic"

    def __str__(self) -> str:
        return str(self.value)
