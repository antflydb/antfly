from enum import StrEnum


class MemoryIntakeScreeningMode(StrEnum):
    REDACT = "redact"
    REJECT = "reject"

    def __str__(self) -> str:
        return str(self.value)
