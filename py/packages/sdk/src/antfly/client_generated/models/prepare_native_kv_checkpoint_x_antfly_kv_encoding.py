from enum import StrEnum


class PrepareNativeKvCheckpointXAntflyKVEncoding(StrEnum):
    F16 = "f16"
    F32 = "f32"
    FP8 = "fp8"
    ROTATED4 = "rotated4"

    def __str__(self) -> str:
        return str(self.value)
