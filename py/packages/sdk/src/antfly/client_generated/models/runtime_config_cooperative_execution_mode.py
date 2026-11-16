from enum import StrEnum


class RuntimeConfigCooperativeExecutionMode(StrEnum):
    OFF = "off"
    ON = "on"

    def __str__(self) -> str:
        return str(self.value)
