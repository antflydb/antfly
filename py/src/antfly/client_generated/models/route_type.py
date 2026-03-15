from enum import Enum


class RouteType(str, Enum):
    QUESTION = "question"
    SEARCH = "search"

    def __str__(self) -> str:
        return str(self.value)
