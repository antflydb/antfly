from enum import StrEnum


class ExaSearchConfigProvider(StrEnum):
    EXA = "exa"

    def __str__(self) -> str:
        return str(self.value)
