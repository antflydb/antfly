from enum import Enum


class RetrievalStrategy(str, Enum):
    BM25 = "bm25"
    GRAPH = "graph"
    HYBRID = "hybrid"
    METADATA = "metadata"
    SEMANTIC = "semantic"
    TREE = "tree"

    def __str__(self) -> str:
        return str(self.value)
