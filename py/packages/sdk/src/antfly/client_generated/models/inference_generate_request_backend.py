from enum import Enum


class InferenceGenerateRequestBackend(str, Enum):
    AUTO = "auto"
    METAL = "metal"
    NATIVE = "native"
    ONNX = "onnx"
    WEBGPU = "webgpu"
    XLA = "xla"

    def __str__(self) -> str:
        return str(self.value)
