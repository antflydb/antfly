from enum import StrEnum


class DenseNativeStoragePhase(StrEnum):
    LEGACY = "legacy"
    NATIVE_AUTHORITATIVE = "native_authoritative"
    NATIVE_BUILDING = "native_building"
    NATIVE_VALIDATING = "native_validating"

    def __str__(self) -> str:
        return str(self.value)
