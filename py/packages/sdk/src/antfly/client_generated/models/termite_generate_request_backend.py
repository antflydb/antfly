from enum import Enum


class TermiteGenerateRequestBackend(str, Enum):
    AUTO = "auto"
    METAL = "metal"
    MLX = "mlx"
    NATIVE = "native"
    ONNX = "onnx"
    WEBGPU = "webgpu"
    XLA = "xla"

    def __str__(self) -> str:
        return str(self.value)
