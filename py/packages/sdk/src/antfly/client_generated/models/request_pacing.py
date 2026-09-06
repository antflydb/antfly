from enum import StrEnum


class RequestPacing(StrEnum):
    COMPLETION = "completion"
    TOKEN_BUCKET = "token_bucket"

    def __str__(self) -> str:
        return str(self.value)
