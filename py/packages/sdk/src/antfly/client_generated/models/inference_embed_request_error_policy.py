from enum import Enum


class InferenceEmbedRequestErrorPolicy(str, Enum):
    FAIL_FAST = "fail_fast"
    PER_ITEM = "per_item"

    def __str__(self) -> str:
        return str(self.value)
