from enum import StrEnum


class MemoryInspectionStateConfigProcessing(StrEnum):
    EXTERNAL = "external"
    HYBRID = "hybrid"
    MANAGED = "managed"

    def __str__(self) -> str:
        return str(self.value)
