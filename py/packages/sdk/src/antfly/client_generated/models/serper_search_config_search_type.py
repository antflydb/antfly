from enum import StrEnum


class SerperSearchConfigSearchType(StrEnum):
    IMAGES = "images"
    NEWS = "news"
    PLACES = "places"
    SEARCH = "search"
    SHOPPING = "shopping"

    def __str__(self) -> str:
        return str(self.value)
