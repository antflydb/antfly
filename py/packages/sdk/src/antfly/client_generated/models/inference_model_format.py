from enum import Enum


class InferenceModelFormat(str, Enum):
    GGUF = "gguf"
    HYBRID = "hybrid"
    ONNX = "onnx"
    SAFETENSORS = "safetensors"

    def __str__(self) -> str:
        return str(self.value)
