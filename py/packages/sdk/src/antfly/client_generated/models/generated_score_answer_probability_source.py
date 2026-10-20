from enum import StrEnum


class GeneratedScoreAnswerProbabilitySource(StrEnum):
    TOKEN_LOGPROBS = "token_logprobs"
    VERDICT = "verdict"

    def __str__(self) -> str:
        return str(self.value)
