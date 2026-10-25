from enum import StrEnum


class InferenceContextDiagnosticsKvCacheVersion(StrEnum):
    V1 = "v1"

    def __str__(self) -> str:
        return str(self.value)
