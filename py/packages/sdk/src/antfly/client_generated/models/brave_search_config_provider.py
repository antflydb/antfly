from enum import StrEnum


class BraveSearchConfigProvider(StrEnum):
    BRAVE = "brave"

    def __str__(self) -> str:
        return str(self.value)
