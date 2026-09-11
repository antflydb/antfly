from enum import StrEnum


class PackageArtifactKind(StrEnum):
    ASSET = "asset"
    MANIFEST = "manifest"
    NATIVE_LIBRARY = "native_library"
    WASM = "wasm"

    def __str__(self) -> str:
        return str(self.value)
