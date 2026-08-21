from enum import Enum


class InferenceErrorReason(str, Enum):
    INFERENCE_ADMISSION = "inference_admission"
    INFERENCE_CAPACITY = "inference_capacity"

    def __str__(self) -> str:
        return str(self.value)
