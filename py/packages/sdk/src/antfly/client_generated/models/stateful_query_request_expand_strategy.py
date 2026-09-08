from enum import StrEnum


class StatefulQueryRequestExpandStrategy(StrEnum):
    INTERSECTION = "intersection"
    UNION = "union"

    def __str__(self) -> str:
        return str(self.value)
