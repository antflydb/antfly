from enum import StrEnum


class InferenceRewriteObjectObject(StrEnum):
    REWRITE = "rewrite"

    def __str__(self) -> str:
        return str(self.value)
