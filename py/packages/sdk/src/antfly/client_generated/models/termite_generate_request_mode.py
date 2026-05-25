from enum import Enum


class TermiteGenerateRequestMode(str, Enum):
    COMPILED = "compiled"
    EAGER = "eager"

    def __str__(self) -> str:
        return str(self.value)
