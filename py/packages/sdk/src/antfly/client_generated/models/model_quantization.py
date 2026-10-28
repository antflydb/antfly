from enum import StrEnum


class ModelQuantization(StrEnum):
    FP16 = "fp16"
    Q4_K = "q4_k"
    Q8 = "q8"

    def __str__(self) -> str:
        return str(self.value)
