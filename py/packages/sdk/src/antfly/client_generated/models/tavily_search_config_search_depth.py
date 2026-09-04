from enum import StrEnum


class TavilySearchConfigSearchDepth(StrEnum):
    ADVANCED = "advanced"
    BASIC = "basic"

    def __str__(self) -> str:
        return str(self.value)
