from enum import StrEnum


class TavilySearchConfigProvider(StrEnum):
    TAVILY = "tavily"

    def __str__(self) -> str:
        return str(self.value)
