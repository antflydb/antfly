from enum import StrEnum


class DeciderConfigDecisionMethod(StrEnum):
    EMBEDDING_SIMILARITY = "embedding_similarity"
    GENERATED = "generated"
    TYPED = "typed"

    def __str__(self) -> str:
        return str(self.value)
