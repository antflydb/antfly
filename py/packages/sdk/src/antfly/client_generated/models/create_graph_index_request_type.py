from enum import StrEnum


class CreateGraphIndexRequestType(StrEnum):
    GRAPH = "graph"

    def __str__(self) -> str:
        return str(self.value)
