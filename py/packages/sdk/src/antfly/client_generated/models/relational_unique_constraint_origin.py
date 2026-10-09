from enum import StrEnum


class RelationalUniqueConstraintOrigin(StrEnum):
    CONSTRAINT = "constraint"
    INDEX = "index"

    def __str__(self) -> str:
        return str(self.value)
