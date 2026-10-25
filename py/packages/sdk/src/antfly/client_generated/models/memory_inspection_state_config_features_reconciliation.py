from enum import StrEnum


class MemoryInspectionStateConfigFeaturesReconciliation(StrEnum):
    DETERMINISTIC = "deterministic"
    EXPLICIT = "explicit"
    MODEL = "model"

    def __str__(self) -> str:
        return str(self.value)
