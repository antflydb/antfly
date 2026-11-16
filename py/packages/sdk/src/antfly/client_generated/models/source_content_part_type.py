from enum import StrEnum


class SourceContentPartType(StrEnum):
    SOURCE = "source"

    def __str__(self) -> str:
        return str(self.value)
