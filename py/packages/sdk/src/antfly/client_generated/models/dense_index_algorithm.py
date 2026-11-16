from enum import StrEnum


class DenseIndexAlgorithm(StrEnum):
    HBC = "hbc"
    LEANN = "leann"

    def __str__(self) -> str:
        return str(self.value)
