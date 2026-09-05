from enum import StrEnum


class VertexGeneratorConfigProvider(StrEnum):
    VERTEX = "vertex"

    def __str__(self) -> str:
        return str(self.value)
