from enum import Enum


class InferenceGenerateRequestCacheDtype(str, Enum):
    F16 = "f16"
    F32 = "f32"
    FP8 = "fp8"
    INT4 = "int4"
    INT8 = "int8"

    def __str__(self) -> str:
        return str(self.value)
