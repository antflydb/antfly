from enum import StrEnum


class SerperSearchConfigProvider(StrEnum):
    SERPER = "serper"

    def __str__(self) -> str:
        return str(self.value)
