from enum import StrEnum


class ManagedSourceConfiguredState(StrEnum):
    CONFIGURED = "configured"

    def __str__(self) -> str:
        return str(self.value)
