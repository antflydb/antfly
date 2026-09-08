from enum import StrEnum


class FuzzinessType1(StrEnum):
    AUTO = "auto"

    def __str__(self) -> str:
        return str(self.value)
