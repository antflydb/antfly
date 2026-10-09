from enum import IntEnum


class EmbeddingDecisionOptionsDimensions(IntEnum):
    VALUE_768 = 768
    VALUE_512 = 512
    VALUE_256 = 256
    VALUE_128 = 128

    def __str__(self) -> str:
        return str(self.value)
