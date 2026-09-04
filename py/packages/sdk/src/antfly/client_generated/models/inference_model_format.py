from enum import StrEnum


class InferenceModelFormat(StrEnum):
    GGUF = "gguf"
    HYBRID = "hybrid"
    ONNX = "onnx"
    SAFETENSORS = "safetensors"

    def __str__(self) -> str:
        return str(self.value)
