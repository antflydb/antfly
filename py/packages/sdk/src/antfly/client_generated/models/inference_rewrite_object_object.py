from enum import Enum


class InferenceRewriteObjectObject(str, Enum):
    REWRITE = "rewrite"

    def __str__(self) -> str:
        return str(self.value)
