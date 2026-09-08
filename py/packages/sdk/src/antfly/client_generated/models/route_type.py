from enum import StrEnum


class RouteType(StrEnum):
    QUESTION = "question"
    SEARCH = "search"

    def __str__(self) -> str:
        return str(self.value)
