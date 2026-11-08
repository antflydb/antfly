from enum import StrEnum


class InferenceMemorillaGenerateResponseAnswerPath(StrEnum):
    ABSTAINED = "abstained"
    MEMORY_VERIFIED = "memory_verified"
    SOURCE_FALLBACK_VERIFIED = "source_fallback_verified"

    def __str__(self) -> str:
        return str(self.value)
