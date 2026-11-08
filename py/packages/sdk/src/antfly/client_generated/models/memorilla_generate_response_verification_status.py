from enum import StrEnum


class MemorillaGenerateResponseVerificationStatus(StrEnum):
    ABSTAINED = "abstained"
    MODEL_SUPPORTED = "model_supported"

    def __str__(self) -> str:
        return str(self.value)
