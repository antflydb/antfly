from enum import StrEnum


class ResolverResolvedEntitySuppressionReason(StrEnum):
    CURATOR = "curator"
    KEY = "key"
    MIN_TEXT_CHARS = "min_text_chars"
    TEXT = "text"

    def __str__(self) -> str:
        return str(self.value)
