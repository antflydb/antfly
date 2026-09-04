from enum import StrEnum


class VertexEmbedderConfigProvider(StrEnum):
    VERTEX = "vertex"

    def __str__(self) -> str:
        return str(self.value)
