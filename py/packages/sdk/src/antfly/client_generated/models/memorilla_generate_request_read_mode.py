from enum import StrEnum


class MemorillaGenerateRequestReadMode(StrEnum):
    AUTO = "auto"
    NARROW = "narrow"
    NONE = "none"
    WIDE = "wide"

    def __str__(self) -> str:
        return str(self.value)
