from enum import Enum


class InferenceEmbeddingItemErrorStage(str, Enum):
    AUDIO_DECODE = "audio_decode"
    AUDIO_INFERENCE = "audio_inference"
    FETCH = "fetch"
    IMAGE_DECODE = "image_decode"
    IMAGE_INFERENCE = "image_inference"
    INFERENCE = "inference"
    MODEL_ADMISSION = "model_admission"
    PARSE = "parse"
    TEXT_INFERENCE = "text_inference"

    def __str__(self) -> str:
        return str(self.value)
