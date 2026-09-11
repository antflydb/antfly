from enum import StrEnum


class SemanticQueryMode(StrEnum):
    HYPOTHETICAL = "hypothetical"
    REWRITE = "rewrite"

    def __str__(self) -> str:
        return str(self.value)
