from enum import Enum


class VertexEmbedderConfigProvider(str, Enum):
    VERTEX = "vertex"

    def __str__(self) -> str:
        return str(self.value)
