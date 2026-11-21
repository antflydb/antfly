from enum import StrEnum


class InferenceMemorillaGenerateRequestReadMode(StrEnum):
    AUTO = "auto"
    NARROW = "narrow"
    NONE = "none"
    WIDE = "wide"

    def __str__(self) -> str:
        return str(self.value)
