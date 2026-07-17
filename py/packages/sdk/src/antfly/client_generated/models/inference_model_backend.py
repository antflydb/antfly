from enum import Enum


class InferenceModelBackend(str, Enum):
    AUTO = "auto"
    CUDA = "cuda"
    METAL = "metal"
    NATIVE = "native"
    ONNX = "onnx"
    WASM = "wasm"
    WEBGPU = "webgpu"
    XLA = "xla"

    def __str__(self) -> str:
        return str(self.value)
