from enum import Enum


class StatefulQueryRequestExpandStrategy(str, Enum):
    INTERSECTION = "intersection"
    UNION = "union"

    def __str__(self) -> str:
        return str(self.value)
