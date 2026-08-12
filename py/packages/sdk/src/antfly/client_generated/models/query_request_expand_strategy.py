from enum import Enum


class QueryRequestExpandStrategy(str, Enum):
    INTERSECTION = "intersection"
    UNION = "union"

    def __str__(self) -> str:
        return str(self.value)
