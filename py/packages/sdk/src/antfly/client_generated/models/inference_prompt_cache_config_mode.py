from enum import StrEnum


class InferencePromptCacheConfigMode(StrEnum):
    BLOCK_HASH = "block_hash"
    RADIX = "radix"
    SIMPLE = "simple"

    def __str__(self) -> str:
        return str(self.value)
