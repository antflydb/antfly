from enum import StrEnum


class InferenceMemorillaGenerateResponseSourceAlignment(StrEnum):
    CALLER_SUPPLIED = "caller_supplied"

    def __str__(self) -> str:
        return str(self.value)
