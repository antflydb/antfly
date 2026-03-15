from enum import Enum


class IndexType(str, Enum):
    EMBEDDINGS = "embeddings"
    FULL_TEXT = "full_text"
    GRAPH = "graph"

    def __str__(self) -> str:
        return str(self.value)
