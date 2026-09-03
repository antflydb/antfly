from enum import Enum


class CohereEmbedderConfigTruncate(str, Enum):
    END = "END"
    NONE = "NONE"
    START = "START"

    def __str__(self) -> str:
        return str(self.value)
