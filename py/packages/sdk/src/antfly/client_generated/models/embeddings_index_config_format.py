from enum import Enum


class EmbeddingsIndexConfigFormat(str, Enum):
    BASE_DELTA = "base_delta"
    PACKED_HBC = "packed_hbc"

    def __str__(self) -> str:
        return str(self.value)
