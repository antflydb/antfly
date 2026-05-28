from enum import Enum


class InferenceExtractObjectObject(str, Enum):
    EXTRACTION = "extraction"

    def __str__(self) -> str:
        return str(self.value)
