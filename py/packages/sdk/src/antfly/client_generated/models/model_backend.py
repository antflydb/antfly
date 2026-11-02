from enum import StrEnum


class ModelBackend(StrEnum):
    AUTO = "auto"
    CUDA = "cuda"
    METAL = "metal"
    NATIVE = "native"
    WASM = "wasm"
    WEBGPU = "webgpu"
    XLA = "xla"

    def __str__(self) -> str:
        return str(self.value)
