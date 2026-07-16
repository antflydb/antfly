from enum import Enum


class InferenceBackendPriorityEntry(str, Enum):
    CUDA = "cuda"
    METAL = "metal"
    NATIVE = "native"
    ONNX = "onnx"
    PJRT = "pjrt"
    WEBGPU = "webgpu"

    def __str__(self) -> str:
        return str(self.value)
