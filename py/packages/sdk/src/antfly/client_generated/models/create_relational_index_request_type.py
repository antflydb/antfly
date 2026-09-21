from enum import StrEnum


class CreateRelationalIndexRequestType(StrEnum):
    RELATIONAL = "relational"

    def __str__(self) -> str:
        return str(self.value)
