from enum import StrEnum


class WebSearchProvider(StrEnum):
    BRAVE = "brave"
    EXA = "exa"
    LINKUP = "linkup"
    SERPER = "serper"
    TAVILY = "tavily"
    VERTEX = "vertex"
    YOU = "you"

    def __str__(self) -> str:
        return str(self.value)
