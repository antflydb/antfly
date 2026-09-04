from enum import StrEnum


class CohereEmbedderConfigTruncate(StrEnum):
    END = "END"
    NONE = "NONE"
    START = "START"

    def __str__(self) -> str:
        return str(self.value)
