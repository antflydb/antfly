from enum import StrEnum


class InferenceEmbedRequestErrorPolicy(StrEnum):
    FAIL_FAST = "fail_fast"
    PER_ITEM = "per_item"

    def __str__(self) -> str:
        return str(self.value)
