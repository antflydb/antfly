from enum import Enum


class InferenceModelBackend(str, Enum):
    AUTO = "auto"
    CUDA = "cuda"
    METAL = "metal"
    NATIVE = "native"
    ONNX = "onnx"
    PJRT = "pjrt"
    WEBGPU = "webgpu"

    def __str__(self) -> str:
        return str(self.value)
