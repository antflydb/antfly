from enum import Enum


class WebSearchProvider(str, Enum):
    BRAVE = "brave"
    EXA = "exa"
    LINKUP = "linkup"
    SERPER = "serper"
    TAVILY = "tavily"
    VERTEX = "vertex"
    YOU = "you"

    def __str__(self) -> str:
        return str(self.value)
