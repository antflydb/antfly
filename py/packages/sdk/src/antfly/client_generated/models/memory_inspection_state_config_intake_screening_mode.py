from enum import StrEnum


class MemoryInspectionStateConfigIntakeScreeningMode(StrEnum):
    REDACT = "redact"
    REJECT = "reject"

    def __str__(self) -> str:
        return str(self.value)
