from enum import Enum


class VertexRerankerConfigProvider(str, Enum):
    VERTEX = "vertex"

    def __str__(self) -> str:
        return str(self.value)
