from enum import StrEnum


class QueryProfileValueSearchPlansItemVerification(StrEnum):
    LOSSLESS_KEYWORD = "lossless_keyword"
    ORIGINAL_VALUE = "original_value"

    def __str__(self) -> str:
        return str(self.value)
