from enum import StrEnum


class InferenceDictationStyle(StrEnum):
    CASUAL = "casual"
    CLEAN = "clean"
    FORMAL = "formal"
    VERBATIM = "verbatim"

    def __str__(self) -> str:
        return str(self.value)
