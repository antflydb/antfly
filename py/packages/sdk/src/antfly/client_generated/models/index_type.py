from enum import Enum


class IndexType(str, Enum):
    ALGEBRAIC = "algebraic"
    EMBEDDINGS = "embeddings"
    FULL_TEXT = "full_text"
    GRAPH = "graph"

    def __str__(self) -> str:
        return str(self.value)
