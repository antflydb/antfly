from enum import StrEnum


class InferenceContextDiagnosticsKvCacheMode(StrEnum):
    EXACT_HYBRID = "exact_hybrid"
    SELECT_V1 = "select_v1"

    def __str__(self) -> str:
        return str(self.value)
