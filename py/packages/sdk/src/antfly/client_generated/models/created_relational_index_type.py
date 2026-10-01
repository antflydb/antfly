from enum import StrEnum


class CreatedRelationalIndexType(StrEnum):
    RELATIONAL = "relational"

    def __str__(self) -> str:
        return str(self.value)
