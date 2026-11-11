from enum import StrEnum


class LeannIndexConfigMediaVectors(StrEnum):
    FULL = "full"
    INT8 = "int8"

    def __str__(self) -> str:
        return str(self.value)
