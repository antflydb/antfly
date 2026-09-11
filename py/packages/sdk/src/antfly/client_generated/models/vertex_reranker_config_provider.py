from enum import StrEnum


class VertexRerankerConfigProvider(StrEnum):
    VERTEX = "vertex"

    def __str__(self) -> str:
        return str(self.value)
