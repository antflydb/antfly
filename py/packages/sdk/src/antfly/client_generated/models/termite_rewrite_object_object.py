from enum import Enum


class TermiteRewriteObjectObject(str, Enum):
    REWRITE = "rewrite"

    def __str__(self) -> str:
        return str(self.value)
