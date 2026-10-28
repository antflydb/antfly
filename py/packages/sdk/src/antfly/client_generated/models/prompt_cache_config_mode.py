from enum import StrEnum


class PromptCacheConfigMode(StrEnum):
    BLOCK_HASH = "block_hash"
    RADIX = "radix"
    SIMPLE = "simple"

    def __str__(self) -> str:
        return str(self.value)
