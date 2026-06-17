from enum import Enum


class PackageArtifactKind(str, Enum):
    ASSET = "asset"
    MANIFEST = "manifest"
    NATIVE_LIBRARY = "native_library"
    WASM = "wasm"

    def __str__(self) -> str:
        return str(self.value)
