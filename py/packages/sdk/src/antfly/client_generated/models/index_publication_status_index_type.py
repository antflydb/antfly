from enum import StrEnum


class IndexPublicationStatusIndexType(StrEnum):
    RELATIONAL = "relational"

    def __str__(self) -> str:
        return str(self.value)
