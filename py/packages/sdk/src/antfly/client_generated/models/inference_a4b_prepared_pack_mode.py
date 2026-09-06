from enum import StrEnum


class InferenceA4BPreparedPackMode(StrEnum):
    AUTO = "auto"
    OFF = "off"
    REQUIRED = "required"

    def __str__(self) -> str:
        return str(self.value)
