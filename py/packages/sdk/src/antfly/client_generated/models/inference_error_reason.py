from enum import StrEnum


class InferenceErrorReason(StrEnum):
    INFERENCE_ADMISSION = "inference_admission"
    INFERENCE_CAPACITY = "inference_capacity"

    def __str__(self) -> str:
        return str(self.value)
