from enum import StrEnum


class QueryProfileValueSearchPlansItemVerification(StrEnum):
    ORIGINAL_VALUE = "original_value"

    def __str__(self) -> str:
        return str(self.value)
