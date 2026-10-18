from enum import StrEnum


class InferenceModelRefCudaPrecision(StrEnum):
    AUTO = "auto"
    BF16 = "bf16"
    FP16 = "fp16"
    FP32 = "fp32"

    def __str__(self) -> str:
        return str(self.value)
