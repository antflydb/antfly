from enum import Enum


class RuntimeDeclMode(str, Enum):
    ANTFLY_API_TEMPLATE = "antfly_api_template"
    MANIFEST_ONLY = "manifest_only"
    NATIVE = "native"
    SIDECAR = "sidecar"
    WASM = "wasm"
    WORKFLOW = "workflow"

    def __str__(self) -> str:
        return str(self.value)
