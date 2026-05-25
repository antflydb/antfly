from enum import Enum


class TavilySearchConfigSearchDepth(str, Enum):
    ADVANCED = "advanced"
    BASIC = "basic"

    def __str__(self) -> str:
        return str(self.value)
