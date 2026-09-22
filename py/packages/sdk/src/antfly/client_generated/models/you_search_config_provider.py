from enum import StrEnum


class YouSearchConfigProvider(StrEnum):
    YOU = "you"

    def __str__(self) -> str:
        return str(self.value)
