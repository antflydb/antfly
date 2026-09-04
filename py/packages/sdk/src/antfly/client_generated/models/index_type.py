from enum import StrEnum


class IndexType(StrEnum):
    ALGEBRAIC = "algebraic"
    EMBEDDINGS = "embeddings"
    FULL_TEXT = "full_text"
    GRAPH = "graph"

    def __str__(self) -> str:
        return str(self.value)
