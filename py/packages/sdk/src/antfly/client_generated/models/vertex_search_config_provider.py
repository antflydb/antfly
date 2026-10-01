from enum import StrEnum


class VertexSearchConfigProvider(StrEnum):
    VERTEX = "vertex"

    def __str__(self) -> str:
        return str(self.value)
