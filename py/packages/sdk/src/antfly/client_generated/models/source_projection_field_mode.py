from enum import StrEnum


class SourceProjectionFieldMode(StrEnum):
    IDENTITY = "identity"
    VALUE = "value"

    def __str__(self) -> str:
        return str(self.value)
