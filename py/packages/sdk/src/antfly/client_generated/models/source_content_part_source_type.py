from enum import StrEnum


class SourceContentPartSourceType(StrEnum):
    SOURCE = "source"

    def __str__(self) -> str:
        return str(self.value)
