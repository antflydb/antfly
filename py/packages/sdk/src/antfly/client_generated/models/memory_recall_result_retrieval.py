from enum import StrEnum


class MemoryRecallResultRetrieval(StrEnum):
    LEXICAL = "lexical"
    LEXICAL_VECTOR = "lexical_vector"

    def __str__(self) -> str:
        return str(self.value)
