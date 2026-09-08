from enum import StrEnum


class CreatedGraphIndexType(StrEnum):
    GRAPH = "graph"

    def __str__(self) -> str:
        return str(self.value)
