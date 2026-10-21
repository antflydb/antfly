from enum import StrEnum


class ExtractionObjectProbabilitySource(StrEnum):
    TOKEN_LOGPROBS = "token_logprobs"
    VERDICT = "verdict"

    def __str__(self) -> str:
        return str(self.value)
