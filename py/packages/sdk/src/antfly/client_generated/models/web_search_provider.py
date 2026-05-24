from enum import Enum


class WebSearchProvider(str, Enum):
    BING = "bing"
    BRAVE = "brave"
    DUCKDUCKGO = "duckduckgo"
    GOOGLE = "google"
    SERPER = "serper"
    TAVILY = "tavily"

    def __str__(self) -> str:
        return str(self.value)
