from enum import StrEnum


class InferenceGenerateRequestMode(StrEnum):
    COMPILED = "compiled"
    EAGER = "eager"

    def __str__(self) -> str:
        return str(self.value)
