from enum import StrEnum


class MemorillaGenerateResponseFinishReason(StrEnum):
    ABSTAINED = "abstained"
    EOS = "eos"
    LENGTH = "length"

    def __str__(self) -> str:
        return str(self.value)
