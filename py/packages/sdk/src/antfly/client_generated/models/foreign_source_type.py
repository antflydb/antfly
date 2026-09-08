from enum import StrEnum


class ForeignSourceType(StrEnum):
    POSTGRES = "postgres"

    def __str__(self) -> str:
        return str(self.value)
