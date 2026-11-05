from enum import StrEnum


class SourceContentPartSourceVersionKind(StrEnum):
    STRONG_ETAG = "strong_etag"

    def __str__(self) -> str:
        return str(self.value)
