from enum import Enum


class VertexGeneratorConfigProvider(str, Enum):
    VERTEX = "vertex"

    def __str__(self) -> str:
        return str(self.value)
