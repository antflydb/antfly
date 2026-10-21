from enum import StrEnum


class ConstrainedProbabilities(StrEnum):
    TOKEN_LOGPROBS = "token_logprobs"
    VERDICT_ONLY = "verdict_only"

    def __str__(self) -> str:
        return str(self.value)
