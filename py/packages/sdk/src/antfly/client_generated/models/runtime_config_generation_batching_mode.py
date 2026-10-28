from enum import StrEnum


class RuntimeConfigGenerationBatchingMode(StrEnum):
    AUTO = "auto"
    OFF = "off"
    ON = "on"

    def __str__(self) -> str:
        return str(self.value)
