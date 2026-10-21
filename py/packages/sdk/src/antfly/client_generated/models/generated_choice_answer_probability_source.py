from enum import StrEnum


class GeneratedChoiceAnswerProbabilitySource(StrEnum):
    TOKEN_LOGPROBS = "token_logprobs"
    VERDICT = "verdict"

    def __str__(self) -> str:
        return str(self.value)
