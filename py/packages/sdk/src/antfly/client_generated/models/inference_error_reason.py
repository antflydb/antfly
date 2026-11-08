from enum import StrEnum


class InferenceErrorReason(StrEnum):
    INFERENCE_ADMISSION = "inference_admission"
    INFERENCE_CAPACITY = "inference_capacity"
    MODEL_PUBLICATION = "model_publication"

    def __str__(self) -> str:
        return str(self.value)
