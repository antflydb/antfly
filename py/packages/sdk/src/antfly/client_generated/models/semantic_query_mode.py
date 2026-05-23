from enum import Enum


class SemanticQueryMode(str, Enum):
    HYPOTHETICAL = "hypothetical"
    REWRITE = "rewrite"

    def __str__(self) -> str:
        return str(self.value)
