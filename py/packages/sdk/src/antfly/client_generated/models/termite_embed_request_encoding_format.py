from enum import Enum


class TermiteEmbedRequestEncodingFormat(str, Enum):
    FLOAT = "float"

    def __str__(self) -> str:
        return str(self.value)
