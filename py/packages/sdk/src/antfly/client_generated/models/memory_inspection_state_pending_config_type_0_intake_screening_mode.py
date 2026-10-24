from enum import StrEnum


class MemoryInspectionStatePendingConfigType0IntakeScreeningMode(StrEnum):
    REDACT = "redact"
    REJECT = "reject"

    def __str__(self) -> str:
        return str(self.value)
