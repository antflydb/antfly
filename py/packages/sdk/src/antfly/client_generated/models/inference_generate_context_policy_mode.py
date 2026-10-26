from enum import StrEnum


class InferenceGenerateContextPolicyMode(StrEnum):
    ATTENTION_MATCHING = "attention_matching"
    EXACT = "exact"
    ROLLING = "rolling"

    def __str__(self) -> str:
        return str(self.value)
