from enum import Enum


class SerperSearchConfigSearchType(str, Enum):
    IMAGES = "images"
    NEWS = "news"
    PLACES = "places"
    SEARCH = "search"
    SHOPPING = "shopping"

    def __str__(self) -> str:
        return str(self.value)
